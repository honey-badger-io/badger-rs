//! Ports of Go's `txn_test.go` and of the `db_test.go` tests that need only
//! the write path (no compaction, GC or streaming).

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use super::*;
use crate::iterator::IteratorOptions;
use crate::structs::BIT_DELETE;
use crate::util::keys::key_with_ts;
use crate::{Entry, Iterator};

fn test_options(dir: &std::path::Path) -> Options {
    // Go's getTestOptions: small memtables and value log files.
    Options::new(dir)
        .with_mem_table_size(1 << 15)
        .with_base_table_size(1 << 15)
        .with_base_level_size(4 << 15)
        .with_value_log_file_size(1 << 20)
        .with_value_threshold(32)
        .with_compression(crate::options::CompressionType::None)
}

/// Go's runBadgerTest: a fresh DB in a temp dir.
fn run_test(opt: Option<Options>, f: impl FnOnce(&DB)) {
    let dir = tempfile::tempdir().unwrap();
    let opt = opt.map_or_else(
        || test_options(dir.path()),
        |o| o.with_dir(dir.path()).with_value_dir(dir.path()),
    );
    let db = DB::open(opt).unwrap();
    f(&db);
    db.close().unwrap();
}

fn read_ts(db: &DB) -> u64 {
    let ts = db.inner.orc.read_ts();
    db.inner.orc.done_read(ts);
    ts
}

fn value(it: &Iterator<'_>) -> String {
    String::from_utf8(it.item().value_copy().unwrap()).unwrap()
}

fn collect(it: &mut Iterator<'_>) -> Vec<String> {
    let mut out = Vec::new();
    it.rewind();
    while it.valid() {
        out.push(value(it));
        it.next();
    }
    out
}

fn rev() -> IteratorOptions {
    IteratorOptions {
        reverse: true,
        ..Default::default()
    }
}

fn all_versions(reverse: bool) -> IteratorOptions {
    IteratorOptions {
        all_versions: true,
        reverse,
        ..Default::default()
    }
}

#[test]
fn txn_simple() {
    run_test(None, |db| {
        let mut txn = db.new_transaction(true);
        for i in 0..10 {
            txn.set(format!("key={i}"), format!("val={i}")).unwrap();
        }
        let item = txn.get(b"key=8").unwrap();
        assert_eq!(item.value_copy().unwrap(), b"val=8");
        txn.commit().unwrap();
    });
}

#[test]
fn txn_read_after_write() {
    run_test(None, |db| {
        std::thread::scope(|s| {
            for i in 0..100 {
                s.spawn(move || {
                    let key = format!("key{i}");
                    db.update(|txn| txn.set(key.clone(), key.clone())).unwrap();
                    db.view(|txn| {
                        assert_eq!(txn.get(key.as_bytes())?.value_copy()?, key.as_bytes());
                        Ok(())
                    })
                    .unwrap();
                });
            }
        });
    });
}

#[test]
fn txn_versions() {
    run_test(None, |db| {
        let k = b"key";
        for i in 1..10u64 {
            let mut txn = db.new_transaction(true);
            txn.set(k.to_vec(), format!("valversion={i}")).unwrap();
            txn.commit().unwrap();
            assert_eq!(read_ts(db), i);
        }
        for i in 1..10u64 {
            let mut txn = db.new_transaction(true);
            txn.set_read_ts(i);
            let want = format!("valversion={i}");
            assert_eq!(txn.get(k).unwrap().value_copy().unwrap(), want.as_bytes());
            // Latest version, forward and reverse.
            for opt in [IteratorOptions::default(), rev()] {
                let mut it = txn.iter(opt);
                assert_eq!(collect(&mut it), std::slice::from_ref(&want), "i={i}");
            }
            // All versions, forward (newest first) and reverse.
            for reverse in [false, true] {
                let mut it = txn.iter(all_versions(reverse));
                let mut versions: Vec<u64> = (1..=i).collect();
                if !reverse {
                    versions.reverse();
                }
                let want: Vec<String> =
                    versions.iter().map(|v| format!("valversion={v}")).collect();
                assert_eq!(collect(&mut it), want, "i={i} reverse={reverse}");
            }
        }
        let txn = db.new_transaction(true);
        assert_eq!(txn.get(k).unwrap().value_copy().unwrap(), b"valversion=9");
    });
}

#[test]
fn txn_write_skew() {
    run_test(None, |db| {
        let (ax, ay) = (b"x", b"y");
        let mut txn = db.new_transaction(true);
        txn.set(ax.to_vec(), "100").unwrap();
        txn.set(ay.to_vec(), "100").unwrap();
        txn.commit().unwrap();
        assert_eq!(read_ts(db), 1);

        let bal = |txn: &Txn<'_>, k: &[u8]| -> i64 {
            String::from_utf8(txn.get(k).unwrap().value_copy().unwrap())
                .unwrap()
                .parse()
                .unwrap()
        };
        // Two transactions read both accounts and deduct from one each.
        let mut txn1 = db.new_transaction(true);
        assert_eq!(bal(&txn1, ax) + bal(&txn1, ay), 200);
        txn1.set(ax.to_vec(), "0").unwrap();
        assert_eq!(bal(&txn1, ax) + bal(&txn1, ay), 100);

        let mut txn2 = db.new_transaction(true);
        assert_eq!(bal(&txn2, ax) + bal(&txn2, ay), 200);
        txn2.set(ay.to_vec(), "0").unwrap();
        assert_eq!(bal(&txn2, ax) + bal(&txn2, ay), 100);

        txn1.commit().unwrap();
        assert!(matches!(txn2.commit(), Err(Error::Conflict)));
        assert_eq!(read_ts(db), 2);
    });
}

/// a3, a2, b4 (del), b3, c2, c1. Read at 4: a3, c2; uncommitted at 4: a3,
/// b4; at 3: a3, b3, c2; at 2: a2, c2; at 1: c1.
#[test]
fn txn_iteration_edge_case() {
    run_test(None, |db| {
        let commit = |sets: &[(&str, &str)], dels: &[&str]| {
            let mut txn = db.new_transaction(true);
            for (k, v) in sets {
                txn.set(k.as_bytes(), v.as_bytes()).unwrap();
            }
            for k in dels {
                txn.delete(k.as_bytes()).unwrap();
            }
            txn.commit().unwrap();
        };
        commit(&[("c", "c1")], &[]);
        assert_eq!(read_ts(db), 1);
        commit(&[("a", "a2"), ("c", "c2")], &[]);
        assert_eq!(read_ts(db), 2);
        commit(&[("a", "a3"), ("b", "b3")], &[]);
        assert_eq!(read_ts(db), 3);
        // b4, c4 (del), uncommitted.
        let mut txn4 = db.new_transaction(true);
        txn4.set("b", "b4").unwrap();
        txn4.delete("c").unwrap();
        assert_eq!(read_ts(db), 3);
        commit(&[], &["b"]);
        assert_eq!(read_ts(db), 4);

        let mut txn = db.new_transaction(true);
        assert_eq!(collect(&mut txn.iter(Default::default())), ["a3", "c2"]);
        assert_eq!(collect(&mut txn4.iter(Default::default())), ["a3", "b4"]);
        assert_eq!(collect(&mut txn.iter(rev())), ["c2", "a3"]);
        assert_eq!(collect(&mut txn4.iter(rev())), ["b4", "a3"]);

        txn.set_read_ts(3);
        assert_eq!(
            collect(&mut txn.iter(Default::default())),
            ["a3", "b3", "c2"]
        );
        assert_eq!(collect(&mut txn.iter(rev())), ["c2", "b3", "a3"]);
        txn.set_read_ts(2);
        assert_eq!(collect(&mut txn.iter(Default::default())), ["a2", "c2"]);
        assert_eq!(collect(&mut txn.iter(rev())), ["c2", "a2"]);
        txn.set_read_ts(1);
        assert_eq!(collect(&mut txn.iter(Default::default())), ["c1"]);
        assert_eq!(collect(&mut txn.iter(rev())), ["c1"]);
    });
}

/// Seeks in both directions across versions (Go's TestTxnIterationEdgeCase2).
#[test]
fn txn_iteration_edge_case2() {
    run_test(None, |db| {
        let commit = |sets: &[(&str, &str)], dels: &[&str]| {
            let mut txn = db.new_transaction(true);
            for (k, v) in sets {
                txn.set(k.as_bytes(), v.as_bytes()).unwrap();
            }
            for k in dels {
                txn.delete(k.as_bytes()).unwrap();
            }
            txn.commit().unwrap();
        };
        commit(&[("c", "c1")], &[]);
        commit(&[("a", "a2"), ("c", "c2")], &[]);
        commit(&[("a", "a3"), ("b", "b3")], &[]);
        commit(&[], &["b"]);
        assert_eq!(read_ts(db), 4);

        let mut txn = db.new_transaction(true);
        let mut it = txn.iter(Default::default());
        it.seek(b"b");
        assert_eq!(value(&it), "c2");
        it.seek(b"a");
        assert_eq!(value(&it), "a3");
        drop(it);
        let mut it = txn.iter(rev());
        it.seek(b"b");
        assert_eq!(value(&it), "a3");
        it.seek(b"c");
        assert_eq!(value(&it), "c2");
        drop(it);

        txn.set_read_ts(3);
        let mut it = txn.iter(Default::default());
        it.seek(b"b");
        assert_eq!(value(&it), "b3");
        drop(it);
        let mut it = txn.iter(rev());
        it.seek(b"b");
        assert_eq!(value(&it), "b3");
        drop(it);

        txn.set_read_ts(1);
        let mut it = txn.iter(Default::default());
        it.seek(b"b");
        assert_eq!(value(&it), "c1");
        drop(it);
        let mut it = txn.iter(rev());
        it.seek(b"b");
        assert!(!it.valid());
    });
}

#[test]
fn iterator_all_versions_with_deleted() {
    let test = |db: &DB| {
        db.update(|txn| {
            txn.set("answer1", "42")?;
            txn.set("answer2", "43")
        })
        .unwrap();
        // Delete one version directly, below the transaction layer.
        db.view(|txn| {
            let item = txn.get(b"answer1")?;
            db.inner.batch_set(vec![Entry {
                key: key_with_ts(item.key(), item.version()),
                meta: BIT_DELETE,
                ..Default::default()
            }])
        })
        .unwrap();
        db.view(|txn| {
            let mut it = txn.iter(IteratorOptions {
                all_versions: true,
                prefetch_values: false,
                ..Default::default()
            });
            it.rewind();
            assert_eq!(it.item().key(), b"answer1");
            assert!(it.item().is_deleted_or_expired());
            it.next();
            assert_eq!(it.item().key(), b"answer2");
            it.next();
            assert!(!it.valid());
            Ok(())
        })
        .unwrap();
    };
    run_test(None, test);
    let db = DB::open(Options::new("").with_in_memory(true)).unwrap();
    test(&db);
    db.close().unwrap();
}

#[test]
fn iterator_all_versions_with_deleted2() {
    run_test(None, |db| {
        for i in 0..4 {
            db.update(|txn| {
                if i % 2 == 0 {
                    txn.set("key", "value")
                } else {
                    txn.delete("key")
                }
            })
            .unwrap();
        }
        db.view(|txn| {
            let mut it = txn.iter(all_versions(false));
            let mut count = 0;
            it.rewind();
            while it.valid() {
                let item = it.item();
                assert_eq!(item.key(), b"key");
                if count % 2 != 0 {
                    assert_eq!(item.value_copy()?, b"value");
                } else {
                    assert!(item.is_deleted_or_expired());
                }
                count += 1;
                it.next();
            }
            assert_eq!(count, 4);
            Ok(())
        })
        .unwrap();
    });
}

#[test]
fn managed_db() {
    let test = |db: &DB| {
        let key = |i: usize| format!("key-{i:02}").into_bytes();
        let val = |i: usize| format!("val-{i}").into_bytes();
        db.view(|_| Ok(())).unwrap();

        // Committing without a timestamp fails in managed mode.
        let mut txn = db.new_transaction_at(3, true);
        txn.set(key(0), val(0)).unwrap();
        assert!(txn.commit().is_err());

        let mut txn = db.new_transaction_at(3, true);
        for i in 0..=3 {
            txn.set(key(i), val(i)).unwrap();
        }
        txn.commit_at(3).unwrap();

        let txn = db.new_transaction_at(2, false);
        for i in 0..=3 {
            assert!(matches!(txn.get(&key(i)), Err(Error::KeyNotFound)));
        }
        drop(txn);
        let txn = db.new_transaction_at(3, false);
        for i in 0..=3 {
            let item = txn.get(&key(i)).unwrap();
            assert_eq!(item.version(), 3);
            assert_eq!(item.value_copy().unwrap(), val(i));
        }
        drop(txn);

        let mut txn = db.new_transaction_at(6, true);
        for i in 0..=7 {
            if txn.get(&key(i)).is_ok() {
                continue;
            }
            txn.set(key(i), val(i)).unwrap();
        }
        txn.commit_at(7).unwrap();

        let txn = db.new_transaction_at(9, false);
        for i in 0..=9 {
            match txn.get(&key(i)) {
                Ok(item) => {
                    assert!(i <= 7);
                    assert_eq!(item.version(), if i <= 3 { 3 } else { 7 });
                    assert_eq!(item.value_copy().unwrap(), val(i));
                }
                Err(Error::KeyNotFound) => assert!(i > 7),
                Err(e) => panic!("{e}"),
            }
        }
        drop(txn);

        // Two transactions write the same key; the one that read it conflicts.
        let mut txn = db.new_transaction_at(10, true);
        let mut txnb = db.new_transaction_at(10, true);
        txnb.get(&key(0)).unwrap();
        txn.set(key(0), val(0)).unwrap();
        txnb.set(key(0), val(1)).unwrap();
        txn.commit_at(11).unwrap();
        assert!(matches!(txnb.commit_at(11), Err(Error::Conflict)));
    };
    let dir = tempfile::tempdir().unwrap();
    let db = DB::open_managed(test_options(dir.path())).unwrap();
    test(&db);
    db.close().unwrap();
    let db = DB::open_managed(Options::new("").with_in_memory(true)).unwrap();
    test(&db);
    db.close().unwrap();
}

#[test]
#[should_panic(expected = "managedDB=false")]
fn managed_db_update_panics() {
    let db = DB::open_managed(Options::new("").with_in_memory(true)).unwrap();
    let _ = db.update(|_| Ok(()));
}

/// Concurrent get-and-set: exactly one transaction creates the key
/// (Go's TestConflict).
#[test]
fn conflict() {
    for _ in 0..5 {
        run_test(None, |db| {
            let set_count = AtomicUsize::new(0);
            std::thread::scope(|s| {
                for _ in 0..10 {
                    s.spawn(|| {
                        let mut txn = db.new_transaction(true);
                        if matches!(txn.get(b"foo"), Err(Error::KeyNotFound)) {
                            txn.set("foo", "AA").unwrap();
                            match txn.commit() {
                                Ok(()) => {
                                    set_count.fetch_add(1, Ordering::SeqCst);
                                }
                                Err(e) => assert!(matches!(e, Error::Conflict), "{e}"),
                            }
                        }
                    });
                }
            });
            assert_eq!(set_count.load(Ordering::SeqCst), 1);
        });
    }
}

// ----- db_test.go -----

#[test]
fn write() {
    run_test(None, |db| {
        for i in 0..100 {
            db.update(|txn| txn.set(format!("key{i}"), format!("val{i}")))
                .unwrap();
        }
    });
}

#[test]
fn update_and_view() {
    run_test(None, |db| {
        db.update(|txn| {
            for i in 0..10 {
                txn.set_entry(Entry::new(format!("key{i}"), format!("val{i}")))?;
            }
            Ok(())
        })
        .unwrap();
        db.view(|txn| {
            for i in 0..10 {
                let item = txn.get(format!("key{i}").as_bytes())?;
                assert_eq!(item.value_copy()?, format!("val{i}").as_bytes());
            }
            Ok(())
        })
        .unwrap();
    });
}

#[test]
fn concurrent_write() {
    run_test(None, |db| {
        let (n, m) = (20, 500);
        std::thread::scope(|s| {
            for i in 0..n {
                s.spawn(move || {
                    for j in 0..m {
                        db.update(|txn| {
                            txn.set(format!("k{i:05}_{j:08}"), format!("v{i:05}_{j:08}"))
                        })
                        .unwrap();
                    }
                });
            }
        });
        db.view(|txn| {
            let mut it = txn.iter(IteratorOptions {
                prefetch_values: false,
                ..Default::default()
            });
            let (mut i, mut j) = (0, 0);
            it.rewind();
            while it.valid() {
                let item = it.item();
                assert_eq!(item.key(), format!("k{i:05}_{j:08}").as_bytes());
                assert_eq!(item.value_copy()?, format!("v{i:05}_{j:08}").as_bytes());
                j += 1;
                if j == m {
                    (i, j) = (i + 1, 0);
                }
                it.next();
            }
            assert_eq!((i, j), (n, 0));
            Ok(())
        })
        .unwrap();
    });
}

#[test]
fn get_and_delete() {
    run_test(None, |db| {
        db.update(|txn| txn.set("key1", "val1")).unwrap();
        db.update(|txn| txn.set("key1", "val2")).unwrap();
        // Go's TestGet: an empty value is a value.
        db.update(|txn| txn.set("key2", "")).unwrap();
        db.view(|txn| {
            assert_eq!(txn.get(b"key1")?.value_copy()?, b"val2");
            assert_eq!(txn.get(b"key2")?.value_copy()?, b"");
            Ok(())
        })
        .unwrap();
        // Go's TestGetAfterDelete.
        db.update(|txn| txn.delete("key1")).unwrap();
        db.view(|txn| {
            assert!(matches!(txn.get(b"key1"), Err(Error::KeyNotFound)));
            Ok(())
        })
        .unwrap();
        // A big value lives in the value log.
        let big = vec![b'x'; 1000];
        db.update(|txn| txn.set("big", big.clone())).unwrap();
        db.view(|txn| {
            let item = txn.get(b"big")?;
            assert_eq!(item.value_copy()?, big);
            assert!(item.value_size() >= 1000);
            Ok(())
        })
        .unwrap();
    });
}

#[test]
fn txn_too_big() {
    run_test(None, |db| {
        let data = |i: usize| format!("{i:b}").into_bytes();
        let n = 1000;
        let mut txn = db.new_transaction(true);
        let mut commits = 0;
        let mut i = 0;
        while i < n {
            match txn.set_entry(Entry::new(data(i), data(i))) {
                Ok(()) => i += 1,
                Err(e) => {
                    assert!(matches!(e, Error::TxnTooBig), "{e}");
                    txn.commit().unwrap();
                    commits += 1;
                    txn = db.new_transaction(true);
                }
            }
        }
        txn.commit().unwrap();
        assert!(commits > 0, "the limit was reached");
        db.view(|txn| {
            for i in 0..n {
                assert_eq!(txn.get(&data(i))?.value_copy()?, data(i));
            }
            Ok(())
        })
        .unwrap();

        let mut txn = db.new_transaction(true);
        let mut i = 0;
        while i < n {
            match txn.delete(data(i)) {
                Ok(()) => i += 1,
                Err(_) => {
                    txn.commit().unwrap();
                    txn = db.new_transaction(true);
                }
            }
        }
        txn.commit().unwrap();
        db.view(|txn| {
            assert!(matches!(txn.get(&data(7)), Err(Error::KeyNotFound)));
            Ok(())
        })
        .unwrap();

        // A value bigger than a value log file is refused.
        let huge = vec![0u8; (db.opts().value_log_file_size + 1) as usize];
        let mut txn = db.new_transaction(true);
        let err = txn.set(b"a".to_vec(), huge).unwrap_err();
        assert!(err.to_string().contains("exceeded"), "{err}");
    });
}

/// Many writes and overwrites spanning memtable flushes and value log files
/// (Go's TestGetMore, smaller).
#[test]
fn get_more() {
    run_test(None, |db| {
        let n = 10_000;
        let key = |i: usize| format!("{i:09}").into_bytes();
        // Batches must stay under max_batch_size (15% of the 32 KiB memtable).
        const CHUNK: usize = 20;
        for chunk in (0..n).collect::<Vec<_>>().chunks(CHUNK) {
            db.update(|txn| {
                for &i in chunk {
                    txn.set(key(i), key(i))?;
                }
                Ok(())
            })
            .unwrap();
        }
        // Overwrite with bigger values that go to the value log.
        for chunk in (0..n).collect::<Vec<_>>().chunks(CHUNK) {
            db.update(|txn| {
                for &i in chunk {
                    let mut v = key(i);
                    v.extend_from_slice(&[b'z'; 64]);
                    txn.set(key(i), v)?;
                }
                Ok(())
            })
            .unwrap();
        }
        // Delete every other key.
        for chunk in (0..n).step_by(2).collect::<Vec<_>>().chunks(CHUNK) {
            db.update(|txn| {
                for &i in chunk {
                    txn.delete(key(i))?;
                }
                Ok(())
            })
            .unwrap();
        }
        assert!(!db.tables().is_empty(), "memtables were flushed to L0");
        db.view(|txn| {
            for i in 0..n {
                match txn.get(&key(i)) {
                    Err(Error::KeyNotFound) => assert!(i % 2 == 0),
                    Ok(item) => {
                        assert!(i % 2 == 1);
                        let v = item.value_copy()?;
                        assert_eq!(&v[..9], key(i).as_slice());
                        assert_eq!(v.len(), 9 + 64);
                    }
                    Err(e) => return Err(e),
                }
            }
            Ok(())
        })
        .unwrap();
    });
}

/// Data survives close and reopen, including the value log and memtables
/// flushed on close.
#[test]
fn reopen() {
    let dir = tempfile::tempdir().unwrap();
    let mut model = BTreeMap::new();
    for round in 0..3 {
        let db = DB::open(test_options(dir.path())).unwrap();
        for i in 0..2000 {
            let k = format!("key{:05}", (i * 7 + round * 13) % 3000);
            let v = format!("r{round}-{i}-{}", "v".repeat(i % 80));
            db.update(|txn| txn.set(k.clone(), v.clone())).unwrap();
            model.insert(k, v);
        }
        db.close().unwrap();

        let db = DB::open(test_options(dir.path()).with_read_only(true)).unwrap();
        db.view(|txn| {
            let mut it = txn.iter(Default::default());
            let mut got = BTreeMap::new();
            it.rewind();
            while it.valid() {
                let item = it.item();
                got.insert(
                    String::from_utf8(item.key().to_vec()).unwrap(),
                    String::from_utf8(item.value_copy()?).unwrap(),
                );
                it.next();
            }
            assert_eq!(got, model, "round {round}");
            Ok(())
        })
        .unwrap();
    }
    // No WAL is left behind after a clean close.
    let mems: Vec<_> = fs::read_dir(dir.path())
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_string_lossy().ends_with(".mem"))
        .collect();
    assert!(mems.is_empty(), "{mems:?}");
}

#[test]
fn iterate_deleted() {
    run_test(None, |db| {
        db.update(|txn| {
            txn.set("Key1", "Value1")?;
            txn.set("Key2", "Value2")
        })
        .unwrap();
        db.update(|txn| txn.delete("Key1")).unwrap();
        let mut txn = db.new_transaction(true);
        txn.delete("Key2").unwrap();
        // The pending delete hides Key2 inside the transaction.
        let mut it = txn.iter(IteratorOptions {
            prefix: b"Key".to_vec(),
            ..Default::default()
        });
        it.rewind();
        assert!(!it.valid());
        drop(it);
        let mut it = txn.iter(IteratorOptions {
            prefix: b"Key".to_vec(),
            all_versions: true,
            ..Default::default()
        });
        let mut keys = Vec::new();
        it.rewind();
        while it.valid() {
            keys.push((it.item().key().to_vec(), it.item().is_deleted_or_expired()));
            it.next();
        }
        assert_eq!(
            keys,
            [
                (b"Key1".to_vec(), true),
                (b"Key1".to_vec(), false),
                (b"Key2".to_vec(), true),
                (b"Key2".to_vec(), false)
            ]
        );
    });
}

#[test]
fn invalid_key() {
    run_test(None, |db| {
        let err = db.update(|txn| txn.set("!badger!head", "")).unwrap_err();
        assert!(matches!(err, Error::InvalidKey));
        let err = db.update(|txn| txn.set("", "x")).unwrap_err();
        assert!(matches!(err, Error::EmptyKey));
        let err = db
            .update(|txn| txn.set(vec![b'k'; 65001], "x"))
            .unwrap_err();
        assert!(err.to_string().contains("exceeded"), "{err}");
    });
}

#[test]
fn expiry() {
    run_test(None, |db| {
        db.update(|txn| {
            txn.set_entry(Entry::new("live", "1").with_ttl(Duration::from_secs(3600)))?;
            let mut dead = Entry::new("dead", "2");
            dead.expires_at = 1;
            txn.set_entry(dead)?;
            txn.set_entry(Entry::new("meta", "3").with_meta(7))
        })
        .unwrap();
        db.view(|txn| {
            let live = txn.get(b"live")?;
            assert!(live.expires_at() > 0);
            assert!(matches!(txn.get(b"dead"), Err(Error::KeyNotFound)));
            assert_eq!(txn.get(b"meta")?.user_meta(), 7);
            Ok(())
        })
        .unwrap();
    });
}

#[test]
fn read_only_txn_cannot_write() {
    run_test(None, |db| {
        let mut txn = db.new_transaction(false);
        assert!(matches!(txn.set("a", "b"), Err(Error::ReadOnlyTxn)));
    });
}

#[test]
fn write_in_memory() {
    let db = DB::open(Options::new("").with_in_memory(true)).unwrap();
    for i in 0..100 {
        db.update(|txn| txn.set(format!("key{i}"), format!("val{i}")))
            .unwrap();
    }
    db.view(|txn| {
        for i in 0..100 {
            assert_eq!(
                txn.get(format!("key{i}").as_bytes())?.value_copy()?,
                format!("val{i}").as_bytes()
            );
        }
        Ok(())
    })
    .unwrap();
    db.close().unwrap();
}

#[test]
fn lsm_only() {
    let dir = tempfile::tempdir().unwrap();
    let opt = Options::lsm_only(dir.path())
        .with_mem_table_size(1 << 20)
        .with_value_threshold(1 << 10);
    let db = DB::open(opt.clone()).unwrap();
    let value = vec![b'v'; 900];
    for i in 0..300 {
        db.update(|txn| txn.set(format!("k{i}"), value.clone()))
            .unwrap();
    }
    db.close().unwrap();
    let db = DB::open(opt).unwrap();
    db.view(|txn| {
        assert_eq!(txn.get(b"k299")?.value_copy()?, value);
        Ok(())
    })
    .unwrap();
    db.close().unwrap();
}

#[test]
fn pid_file_and_exclusive_lock() {
    let dir = tempfile::tempdir().unwrap();
    let db = DB::open(test_options(dir.path())).unwrap();
    assert!(dir.path().join("LOCK").exists());
    assert!(DB::open(test_options(dir.path())).is_err());
    db.close().unwrap();
    assert!(!dir.path().join("LOCK").exists());
}

/// A crash leaves WALs and an untruncated value log; reopening recovers
/// everything that was committed (simulated by leaking the DB, so nothing is
/// flushed or truncated).
#[test]
fn recover_without_close() {
    let dir = tempfile::tempdir().unwrap();
    let opt = test_options(dir.path()).with_bypass_lock_guard(true);
    let db = DB::open(opt.clone()).unwrap();
    for i in 0..500 {
        db.update(|txn| txn.set(format!("k{i:04}"), format!("{i}-{}", "x".repeat(i % 100))))
            .unwrap();
    }
    // Wait for queued flushes, then "crash".
    while !db.inner.mem.lock().unwrap().imm.is_empty() {
        std::thread::sleep(Duration::from_millis(10));
    }
    std::mem::forget(db);

    let db = DB::open(opt).unwrap();
    db.view(|txn| {
        for i in 0..500 {
            let v = txn.get(format!("k{i:04}").as_bytes())?.value_copy()?;
            assert_eq!(v, format!("{i}-{}", "x".repeat(i % 100)).as_bytes());
        }
        Ok(())
    })
    .unwrap();
    // New commits continue after the recovered versions.
    let before = db.max_version();
    db.update(|txn| txn.set("new", "v")).unwrap();
    assert_eq!(db.max_version(), before + 1);
    db.close().unwrap();
}
