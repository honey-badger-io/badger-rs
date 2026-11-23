//! Port of Go's `merge_test.go`.

use std::path::Path;
use std::time::Duration;

use crate::iterator::IteratorOptions;
use crate::{DB, Error, Options};

fn u64_bytes(i: u64) -> Vec<u8> {
    i.to_be_bytes().to_vec()
}

fn bytes_u64(b: &[u8]) -> u64 {
    u64::from_be_bytes(b.try_into().unwrap())
}

/// Merge function adding two big-endian u64s.
fn add(existing: &[u8], latest: &[u8]) -> Vec<u8> {
    u64_bytes(bytes_u64(existing) + bytes_u64(latest))
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

fn run_test(f: impl FnOnce(&DB)) {
    let dir = tempfile::tempdir().unwrap();
    let db = DB::open(test_options(dir.path())).unwrap();
    f(&db);
    db.close().unwrap();
}

const MS: fn(u64) -> Duration = Duration::from_millis;

#[test]
fn get_before_add() {
    run_test(|db| {
        let m = db.get_merge_operator("merge", add, MS(200));
        assert!(matches!(m.get(), Err(Error::KeyNotFound)));
    });
}

#[test]
fn add_and_get() {
    // "Add and Get" and "Get Before Compact" (500ms) in Go.
    for dur in [200, 500] {
        run_test(|db| {
            let m = db.get_merge_operator("merge", add, MS(dur));
            for i in 1..=3 {
                m.add(u64_bytes(i)).unwrap();
            }
            assert_eq!(bytes_u64(&m.get().unwrap()), 6);
        });
    }
}

#[test]
fn add_and_get_slices() {
    run_test(|db| {
        let concat = |a: &[u8], b: &[u8]| [a, b].concat();
        let m = db.get_merge_operator("fooprefix", concat, MS(2));
        for v in ["A", "B", "C"] {
            m.add(v).unwrap();
        }
        assert_eq!(m.get().unwrap(), b"ABC");
    });
}

#[test]
fn get_after_delete() {
    run_test(|db| {
        let m = db.get_merge_operator("merge", add, MS(200));
        for i in 1..=3 {
            m.add(u64_bytes(i)).unwrap();
        }
        m.stop();
        // Stopping merged and wrote the value back; a new operator reads it.
        let m = db.get_merge_operator("merge", add, MS(200));
        assert_eq!(bytes_u64(&m.get().unwrap()), 6);
        drop(m);

        db.update(|txn| txn.delete("merge")).unwrap();
        let m = db.get_merge_operator("merge", add, MS(200));
        m.add(u64_bytes(1)).unwrap();
        m.stop();
        let m = db.get_merge_operator("merge", add, MS(200));
        assert_eq!(bytes_u64(&m.get().unwrap()), 1);
    });
}

#[test]
fn get_after_stop() {
    run_test(|db| {
        let m = db.get_merge_operator("merge", add, Duration::from_secs(1));
        for i in 1..=3 {
            m.add(u64_bytes(i)).unwrap();
        }
        m.stop();
        // The last merge wrote "discard earlier versions" at the newest
        // version: a plain read sees the sum.
        db.view(|txn| {
            let v = txn.get(b"merge")?.value_copy()?;
            assert_eq!(bytes_u64(&v), 6);
            Ok(())
        })
        .unwrap();
    });
}

#[test]
fn old_keys_removed_after_compaction() {
    let dir = tempfile::tempdir().unwrap();
    let opts = test_options(dir.path()).with_compact_l0_on_close(true);
    let db = DB::open(opts.clone()).unwrap();
    let m = db.get_merge_operator("foo", add, MS(2));
    // Enough to compact L0 → L1.
    let count = 5000;
    for _ in 0..count {
        m.add(u64_bytes(1)).unwrap();
    }
    assert_eq!(bytes_u64(&m.get().unwrap()), count);
    m.stop();
    // Closing compacts L0, which drops the merged operands.
    db.close().unwrap();

    let db = DB::open(opts).unwrap();
    db.view(|txn| {
        let mut it = txn.key_iter(b"foo", IteratorOptions::default());
        it.rewind();
        let mut n = 0;
        while it.valid() {
            n += 1;
            it.next();
        }
        assert_eq!(n, 1);
        Ok(())
    })
    .unwrap();
    db.close().unwrap();
}
