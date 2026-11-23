//! Ports of Go's iterator tests (`iterator_test.go`, and the iterator tests
//! of `db_test.go` and `txn_test.go`), plus value prefetching tests.

use super::*;
use crate::iterator::{Item, IteratorOptions};

fn bkey(i: usize) -> Vec<u8> {
    format!("{i:09}").into_bytes()
}

fn bval(i: usize) -> Vec<u8> {
    format!("{i:025}").into_bytes()
}

/// Go's getTestOptions (`DefaultOptions` without sync writes).
fn go_test_options(dir: &std::path::Path) -> Options {
    Options::new(dir).with_sync_writes(false)
}

fn with_db(opt: impl FnOnce(&std::path::Path) -> Options, f: impl FnOnce(&DB)) {
    let dir = tempfile::tempdir().unwrap();
    let db = DB::open(opt(dir.path())).unwrap();
    f(&db);
    db.close().unwrap();
}

/// Go's txnSet.
fn txn_set(db: &DB, key: Vec<u8>, val: Vec<u8>, meta: u8) {
    db.update(|txn| txn.set_entry(Entry::new(key, val).with_meta(meta)))
        .unwrap();
}

/// Go's getItemValue: the value via `value` and `value_copy`, which agree.
fn item_value(item: &Item<'_>) -> Vec<u8> {
    let v = item.value(<[u8]>::to_vec).unwrap();
    assert_eq!(item.value_copy().unwrap(), v);
    v
}

fn prefetch(size: usize) -> IteratorOptions {
    IteratorOptions {
        prefetch_values: true,
        prefetch_size: size,
        ..Default::default()
    }
}

#[test]
fn iterate_since_ts() {
    with_db(go_test_options, |db| {
        let n = 100_000;
        let mut wb = db.new_write_batch();
        for i in 0..n {
            wb.set(format!("{i:04}"), "OK").unwrap();
        }
        wb.flush().unwrap();

        let max = db.max_version();
        let since_ts = max - max / 10;
        db.view(|txn| {
            let mut it = txn.iter(IteratorOptions {
                since_ts,
                ..Default::default()
            });
            it.rewind();
            while it.valid() {
                assert!(it.item().version() >= since_ts);
                it.next();
            }
            Ok(())
        })
        .unwrap();
    });
}

#[test]
fn iterate_since_ts_with_pending_writes() {
    // Pending writes have no version yet; they are visible with since_ts 0.
    with_db(go_test_options, |db| {
        let mut txn = db.new_transaction(true);
        txn.set("key1", "value1").unwrap();
        txn.set("key2", "value2").unwrap();
        let mut it = txn.iter(IteratorOptions::default());
        it.rewind();
        let mut count = 0;
        while it.valid() {
            count += 1;
            it.next();
        }
        assert_eq!(count, 2);
    });
}

#[test]
fn iterate_prefix() {
    // Manual-only in Go; small enough to run here. Also checks the key
    // iterator on every key, with and without the caches.
    let configs: [fn(&std::path::Path) -> Options; 4] = [
        go_test_options,
        |d| go_test_options(d).with_index_cache_size(100 << 20),
        |d| {
            go_test_options(d)
                .with_index_cache_size(100 << 20)
                .with_block_cache_size(100 << 20)
        },
        |d| go_test_options(d).with_block_cache_size(100 << 20),
    ];
    for opt in configs {
        with_db(opt, |db| {
            let n = 10_000;
            let key = |i: usize| format!("{i:04}").into_bytes();
            let mut wb = db.new_write_batch();
            for i in 0..n {
                wb.set(key(i), "OK").unwrap();
            }
            wb.flush().unwrap();

            let count_keys = |prefix: &str| -> usize {
                db.view(|txn| {
                    let opt = IteratorOptions {
                        prefix: prefix.as_bytes().to_vec(),
                        ..Default::default()
                    };
                    let mut it = txn.iter(opt);
                    it.rewind();
                    let mut count = 0;
                    while it.valid() {
                        let item = it.item();
                        assert_eq!(item.value_copy().unwrap(), b"OK");
                        assert!(item.key().starts_with(prefix.as_bytes()));
                        count += 1;
                        it.next();
                    }
                    Ok(count)
                })
                .unwrap()
            };
            for i in 0..=9 {
                assert_eq!(count_keys(&format!("{i}{i}{i}{i}")), 1);
                assert_eq!(count_keys(&format!("{i}{i}{i}")), 10);
                assert_eq!(count_keys(&format!("{i}{i}")), 100);
                assert_eq!(count_keys(&format!("{i}")), 1000);
            }
            assert_eq!(count_keys(""), 10_000);

            db.view(|txn| {
                for i in (0..n).step_by(7) {
                    let mut it = txn.key_iter(&key(i), IteratorOptions::default());
                    it.rewind();
                    let mut count = 0;
                    while it.valid() {
                        assert_eq!(it.item().key(), key(i));
                        assert_eq!(it.item().value_copy().unwrap(), b"OK");
                        count += 1;
                        it.next();
                    }
                    assert_eq!(count, 1, "key {i}");
                }
                Ok(())
            })
            .unwrap();
        });
    }
}

#[test]
fn iterator_read_only_with_no_data() {
    let dir = tempfile::tempdir().unwrap();
    DB::open(go_test_options(dir.path()))
        .unwrap()
        .close()
        .unwrap();
    let db = DB::open(go_test_options(dir.path()).with_read_only(true)).unwrap();
    db.view(|txn| {
        let mut it = txn.iter(IteratorOptions {
            prefix: b"xxx".to_vec(),
            ..Default::default()
        });
        it.rewind();
        assert!(!it.valid());
        Ok(())
    })
    .unwrap();
    db.close().unwrap();
}

#[test]
fn reverse_iterator() {
    with_db(go_test_options, |db| {
        let mut key = [0u8; 6];
        key[..2].copy_from_slice(&5u16.to_be_bytes());
        db.update(|txn| {
            key[2..].copy_from_slice(&1u32.to_be_bytes());
            txn.set(key.to_vec(), "value1")?;
            key[2..].copy_from_slice(&2u32.to_be_bytes());
            txn.set(key.to_vec(), "value2")
        })
        .unwrap();
        db.view(|txn| {
            let mut search = 5u16.to_be_bytes().to_vec();
            search.push(0xFF);
            let mut it = txn.iter(IteratorOptions {
                reverse: true,
                prefetch_values: false,
                prefix: search,
                ..Default::default()
            });
            it.rewind();
            assert_eq!(it.item().key(), key);
            Ok(())
        })
        .unwrap();
    });
}

fn iterate2_basic(db: &DB) {
    let n = 10_000;
    for i in 0..n {
        txn_set(db, bkey(i), bval(i), (i % 127) as u8);
    }
    let txn = db.new_transaction(false);
    let mut it = txn.iter(prefetch(10));
    let mut count = 0;
    let mut rewound = false;
    it.rewind();
    while it.valid() {
        if !rewound && count == 5000 {
            // Rewind skips to the head; `next` below skips key 0.
            count = 1;
            it.rewind();
            rewound = true;
            it.next();
            continue;
        }
        let item = it.item();
        assert_eq!(item.key(), bkey(count));
        assert_eq!(item_value(item), bval(count));
        assert_eq!(item.user_meta(), (count % 127) as u8);
        count += 1;
        it.next();
    }
    assert_eq!(count, n);

    let mut idx = 5030;
    it.seek(&bkey(idx));
    while it.valid() {
        let item = it.item();
        assert_eq!(item.key(), bkey(idx));
        assert_eq!(item_value(item), bval(idx));
        idx += 1;
        it.next();
    }
    assert_eq!(idx, n);
}

#[test]
fn iterate2_basic_disk() {
    with_db(go_test_options, iterate2_basic);
    // Values in the value log, so prefetching reads them.
    with_db(
        |d| go_test_options(d).with_value_threshold(16),
        iterate2_basic,
    );
}

#[test]
fn iterate2_basic_in_memory() {
    let db = DB::open(Options::new("").with_in_memory(true)).unwrap();
    iterate2_basic(&db);
    db.close().unwrap();
}

#[test]
fn iterator_prefetch_size() {
    with_db(go_test_options, |db| {
        let n = 100;
        for i in 0..n {
            txn_set(db, bkey(i), bval(i), (i % 127) as u8);
        }
        // Go also tries -10 (as 0 here): any size iterates over everything.
        for size in [0, 1, 10] {
            let txn = db.new_transaction(false);
            let mut it = txn.iter(prefetch(size));
            it.rewind();
            let mut count = 0;
            while it.valid() {
                count += 1;
                it.next();
            }
            assert_eq!(count, n, "prefetch size {size}");
        }
    });
}

#[test]
fn txn_iteration_edge_case3() {
    with_db(go_test_options, |db| {
        let (kb, kc, kd) = (b"abc", b"acd", b"ade");
        let read_ts = || {
            let ts = db.inner.orc.read_ts();
            db.inner.orc.done_read(ts);
            ts
        };
        db.update(|txn| txn.set(kc.to_vec(), "c1")).unwrap();
        assert_eq!(read_ts(), 1);
        db.update(|txn| txn.set(kb.to_vec(), "b2")).unwrap();
        assert_eq!(read_ts(), 2);

        let mut txn2 = db.new_transaction(true);
        txn2.set(kd.to_vec(), "d2").unwrap();
        txn2.delete(kc.to_vec()).unwrap();
        let txn = db.new_transaction(true);
        let rev = IteratorOptions {
            reverse: true,
            ..Default::default()
        };

        // (seek key, expected key); an empty seek key is a rewind.
        let check = |txn: &Txn<'_>, opt: IteratorOptions, steps: &[(&[u8], &[u8])]| {
            let mut it = txn.iter(opt);
            for &(seek, want) in steps {
                it.seek(seek);
                assert!(it.valid(), "seek {seek:?}");
                assert_eq!(it.item().key(), want, "seek {seek:?}");
            }
        };
        let default = IteratorOptions::default;
        check(
            &txn,
            default(),
            &[(b"ab", kb), (b"ac", kc), (b"", kb), (b"", kb), (b"ac", kc)],
        );
        // Pending writes: keys "abc", "ade".
        check(
            &txn2,
            default(),
            &[(b"ab", kb), (b"ac", kd), (b"", kb), (b"", kb), (b"ad", kd)],
        );
        check(
            &txn,
            rev.clone(),
            &[(b"ac", kb), (b"ad", kc), (b"", kc), (b"", kc), (b"ad", kc)],
        );
        check(
            &txn2,
            rev,
            &[(b"ad", kb), (b"ae", kd), (b"", kd), (b"", kd), (b"ac", kb)],
        );
    });
}

#[test]
fn iterate_parallel() {
    // Manual-only in Go. Several iterators of one transaction, read-only and
    // read-write, iterate concurrently.
    let key = |i: u32| [b"account-".as_slice(), &i.to_be_bytes()].concat();
    let n = 20_000;
    with_db(go_test_options, |db| {
        let pending: Vec<_> = (0..n)
            .map(|i| {
                let mut txn = db.new_transaction(true);
                txn.set(key(i), "1000").unwrap();
                txn.start_commit().unwrap()
            })
            .collect();
        for p in pending {
            p.wait().unwrap();
        }
        let iterate = |txn: &Txn<'_>| {
            let mut it = txn.iter(IteratorOptions::default());
            it.rewind();
            let mut count = 0;
            while it.valid() {
                let item = it.item();
                assert_eq!(&item.key()[..8], b"account-");
                assert_eq!(item.value_copy().unwrap(), b"1000");
                count += 1;
                it.next();
            }
            assert_eq!(count, n);
        };
        for update in [false, true] {
            let txn = db.new_transaction(update);
            std::thread::scope(|s| {
                for _ in 0..3 {
                    s.spawn(|| iterate(&txn));
                }
            });
        }
    });
}

#[test]
fn prefetch_reads_value_log_values() {
    // Values in the value log, prefetched in batches big enough to use
    // several threads, forward and reverse, with seeks in between.
    let opt = |d: &std::path::Path| {
        go_test_options(d)
            .with_value_threshold(16)
            .with_value_log_file_size(1 << 20)
    };
    with_db(opt, |db| {
        let n = 3000;
        let mut wb = db.new_write_batch();
        for i in 0..n {
            wb.set(bkey(i), bval(i)).unwrap();
        }
        wb.flush().unwrap();

        db.view(|txn| {
            for reverse in [false, true] {
                let mut it = txn.iter(IteratorOptions {
                    reverse,
                    ..prefetch(256)
                });
                it.rewind();
                let mut seen = 0;
                while it.valid() {
                    let item = it.item();
                    let i = if reverse { n - 1 - seen } else { seen };
                    assert_eq!(item.key(), bkey(i));
                    assert!(item.val.is_some(), "value of {i} not prefetched");
                    assert_eq!(item_value(item), bval(i));
                    seen += 1;
                    it.next();
                }
                assert_eq!(seen, n);
            }
            let mut it = txn.iter(prefetch(64));
            for i in [2500, 10, 1999] {
                it.seek(&bkey(i));
                assert_eq!(it.item().key(), bkey(i));
                assert_eq!(item_value(it.item()), bval(i));
            }
            // Without prefetching, values are read on demand.
            let mut it = txn.iter(IteratorOptions {
                prefetch_values: false,
                ..Default::default()
            });
            it.rewind();
            assert!(it.item().val.is_none());
            assert_eq!(item_value(it.item()), bval(0));
            Ok(())
        })
        .unwrap();
    });
}
