//! Ports of Go's `batch_test.go` and the write batch tests of
//! `managed_db_test.go`.

use std::path::Path;

use crate::iterator::IteratorOptions;
use crate::{DB, Entry, Error, Options};

fn key(i: usize) -> Vec<u8> {
    format!("{i:10}").into_bytes()
}

fn val(i: usize) -> Vec<u8> {
    format!("{i:128}").into_bytes()
}

/// Go's getTestOptions.
fn test_options(dir: &Path) -> Options {
    Options::new(dir)
        .with_mem_table_size(1 << 15)
        .with_base_table_size(1 << 15)
        .with_base_level_size(4 << 15)
        .with_value_log_file_size(1 << 20)
        .with_value_threshold(32)
        .with_compression(crate::options::CompressionType::None)
}

fn with_db(opt: impl FnOnce(&Path) -> Options, managed: bool, f: impl FnOnce(&DB)) {
    let dir = tempfile::tempdir().unwrap();
    let opt = opt(dir.path());
    let db = if managed {
        DB::open_managed(opt).unwrap()
    } else {
        DB::open(opt).unwrap()
    };
    f(&db);
    db.close().unwrap();
}

/// Keys `m..n` remain, each with `val(i)`, at `version` if given.
fn check_range(db: &DB, m: usize, n: usize, version: Option<u64>) {
    db.view(|txn| {
        let mut it = txn.iter(IteratorOptions::default());
        it.rewind();
        let mut i = m;
        while it.valid() {
            let item = it.item();
            assert_eq!(item.key(), key(i));
            if let Some(v) = version {
                assert_eq!(item.version(), v);
            }
            assert_eq!(item.value_copy().unwrap(), val(i));
            i += 1;
            it.next();
        }
        assert_eq!(i, n);
        Ok(())
    })
    .unwrap();
}

#[test]
fn write_batch() {
    with_db(test_options, false, |db| {
        let mut wb = db.new_write_batch();
        // SetEntryAt needs managed mode.
        assert!(wb.set_entry_at(Entry::default(), 12).is_err());
        let (n, m) = (50_000, 1000);
        for i in 0..n {
            wb.set(key(i), val(i)).unwrap();
        }
        for i in 0..m {
            wb.delete(key(i)).unwrap();
        }
        wb.flush().unwrap();
        check_range(db, m, n, None);
    });
}

#[test]
fn empty_write_batch() {
    with_db(test_options, false, |db| {
        for _ in 0..3 {
            db.new_write_batch().flush().unwrap();
        }
        // The flushed transactions released their read marks: a new read
        // does not wait for them.
        db.view(|_| Ok(())).unwrap();
    });
    with_db(test_options, true, |db| {
        for ts in [2, 208, 31] {
            db.new_write_batch_at(ts).flush().unwrap();
        }
        for _ in 0..3 {
            db.new_managed_write_batch().flush().unwrap();
        }
    });
}

#[test]
fn batch_err_deadlock() {
    with_db(
        |d| Options::new(d),
        true,
        |db| {
            let mut wb = db.new_managed_write_batch();
            // Version 0 in managed mode: the commit needs a timestamp.
            wb.set_entry_at(Entry::new("foo", ""), 0).unwrap();
            assert!(wb.flush().is_err());
        },
    );
}

#[test]
fn write_batch_error_is_sticky() {
    with_db(
        |d| Options::new(d),
        true,
        |db| {
            let mut wb = db.new_managed_write_batch();
            wb.set_max_pending_txns(1);
            // Version 0 everywhere: once the transaction is full, its commit
            // fails (no commit timestamp) and the error sticks.
            let mut failed = false;
            for i in 0..db.max_batch_count() {
                if wb.set_entry_at(Entry::new(key(i as usize), ""), 0).is_err() {
                    failed = true;
                    break;
                }
            }
            assert!(failed);
            assert!(wb.error().is_some());
            // As in Go, later writes are buffered but never committed.
            wb.set_entry_at(Entry::new("bar", ""), 1).unwrap();
            assert!(wb.flush().is_err());
            db.view(|txn| {
                assert!(matches!(txn.get(b"bar"), Err(Error::KeyNotFound)));
                Ok(())
            })
            .unwrap();
        },
    );
}

#[test]
fn write_batch_managed_mode() {
    // Big enough to need several transactions.
    let opt = |dir: &Path| Options::new(dir).with_base_table_size(1 << 20);
    with_db(opt, true, |db| {
        let mut wb = db.new_write_batch_at(1);
        let (n, m) = (50_000, 1000);
        for i in 0..n {
            wb.set(key(i), val(i)).unwrap();
        }
        for i in 0..m {
            wb.delete(key(i)).unwrap();
        }
        wb.flush().unwrap();
        check_range(db, m, n, Some(1));
    });
}

#[test]
fn write_batch_managed() {
    let opt = |dir: &Path| Options::new(dir).with_base_table_size(1 << 15);
    with_db(opt, true, |db| {
        let mut wb = db.new_managed_write_batch();
        let (n, m) = (50_000, 1000);
        for i in 0..n {
            wb.set_entry_at(Entry::new(key(i), val(i)), 1).unwrap();
        }
        for i in 0..m {
            wb.delete_at(key(i), 2).unwrap();
        }
        wb.flush().unwrap();
        check_range(db, m, n, Some(1));
    });
}

#[test]
fn write_batch_duplicate() {
    const N: u64 = 10;
    let read_verify = |db: &DB, versions: &[u64]| {
        db.view(|txn| {
            let mut it = txn.iter(IteratorOptions {
                all_versions: true,
                ..Default::default()
            });
            it.rewind();
            let mut got = Vec::new();
            while it.valid() {
                let item = it.item();
                assert_eq!(item.key(), b"key");
                assert_eq!(item.value_copy().unwrap(), b"val");
                got.push(item.version());
                it.next();
            }
            assert_eq!(got, versions);
            Ok(())
        })
        .unwrap();
    };
    let opt = |dir: &Path| Options::new(dir).with_base_table_size(1 << 15);

    with_db(opt, false, |db| {
        let mut wb = db.new_write_batch();
        for _ in 0..N {
            wb.set_entry(Entry::new("key", "val")).unwrap();
        }
        wb.flush().unwrap();
        read_verify(db, &[1]);
    });
    with_db(opt, true, |db| {
        let mut wb = db.new_write_batch_at(10);
        for _ in 0..N {
            wb.set_entry(Entry::new("key", "val")).unwrap();
        }
        wb.flush().unwrap();
        read_verify(db, &[10]);
    });
    with_db(opt, true, |db| {
        let mut wb = db.new_managed_write_batch();
        for i in 1..=N {
            wb.set_entry_at(Entry::new("key", "val"), i).unwrap();
        }
        wb.flush().unwrap();
        let versions: Vec<u64> = (1..=N).rev().collect();
        read_verify(db, &versions);
    });
}

#[test]
fn dropping_a_batch_discards_uncommitted_writes() {
    with_db(test_options, false, |db| {
        let mut wb = db.new_write_batch();
        wb.set("a", "1").unwrap();
        drop(wb);
        db.view(|txn| {
            assert!(matches!(txn.get(b"a"), Err(Error::KeyNotFound)));
            Ok(())
        })
        .unwrap();
    });
}

#[test]
fn start_commit_then_read() {
    // A pending commit becomes visible to new transactions without waiting.
    with_db(test_options, false, |db| {
        let mut txns = Vec::new();
        for i in 0..20 {
            let mut txn = db.new_transaction(true);
            txn.set(key(i), val(i)).unwrap();
            txns.push(txn.start_commit().unwrap());
        }
        // A new transaction waits for every earlier commit.
        db.view(|txn| {
            for i in 0..20 {
                assert_eq!(txn.get(&key(i)).unwrap().value_copy().unwrap(), val(i));
            }
            Ok(())
        })
        .unwrap();
        for p in txns {
            p.wait().unwrap();
        }
    });
}
