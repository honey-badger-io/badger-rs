//! Encryption key rotation: ports of Go's `badger/cmd/rotate_test.go` (the
//! master key, through `key_registry::rotate_master_key`), plus data key
//! rotation by age (`encryption_key_rotation_duration`).

use std::path::Path;
use std::time::Duration;

use badger::key_registry::{KeyRegistry, KeyRegistryOptions, rotate_master_key};
use badger::{DB, Error, IteratorOptions, Options};

fn key(seed: u8) -> Vec<u8> {
    (0..32)
        .map(|i| seed.wrapping_mul(31).wrapping_add(i))
        .collect()
}

fn opts(dir: &Path, key: &[u8]) -> Options {
    Options::new(dir)
        .with_encryption_key(key.to_vec())
        .with_block_cache_size(1 << 20)
}

fn count(db: &DB) -> usize {
    db.view(|txn| {
        let mut it = txn.iter(IteratorOptions::default());
        it.rewind();
        let mut n = 0;
        while it.valid() {
            n += 1;
            it.next();
        }
        Ok(n)
    })
    .unwrap()
}

fn is_key_mismatch(e: &Error) -> bool {
    matches!(e.root(), Error::EncryptionKeyMismatch)
}

#[test]
fn rotate() {
    let dir = tempfile::tempdir().unwrap();
    let (k1, k2) = (key(1), key(2));
    let db = DB::open(opts(dir.path(), &k1)).unwrap();
    db.update(|txn| txn.set("foo", "bar")).unwrap();
    db.close().unwrap();
    DB::open(opts(dir.path(), &k1)).unwrap().close().unwrap();

    // The wrong old key is refused.
    let err = rotate_master_key(dir.path(), &k2, &k2).unwrap_err();
    assert!(is_key_mismatch(&err), "{err}");

    // k1 → k2: only k2 opens the DB now.
    rotate_master_key(dir.path(), &k1, &k2).unwrap();
    let err = DB::open(opts(dir.path(), &k1)).err().unwrap();
    assert!(is_key_mismatch(&err), "{err}");
    let db = DB::open(opts(dir.path(), &k2)).unwrap();
    assert_eq!(count(&db), 1);
    db.close().unwrap();

    // k2 → none: the data keys are stored in plain text, and the
    // encrypted tables still read.
    rotate_master_key(dir.path(), &k2, &[]).unwrap();
    let db = DB::open(Options::new(dir.path())).unwrap();
    assert_eq!(count(&db), 1);
    db.close().unwrap();
}

#[test]
fn rotate_plain_text_to_encrypted() {
    let dir = tempfile::tempdir().unwrap();
    let db = DB::open(Options::new(dir.path())).unwrap();
    db.update(|txn| txn.set("foo", "bar")).unwrap();
    db.close().unwrap();

    let k = key(3);
    rotate_master_key(dir.path(), &[], &k).unwrap();
    let err = DB::open(Options::new(dir.path()).with_block_cache_size(1 << 20))
        .err()
        .unwrap();
    assert!(is_key_mismatch(&err), "{err}");
    let db = DB::open(opts(dir.path(), &k)).unwrap();
    assert_eq!(count(&db), 1);
    // New tables are encrypted from now on.
    db.update(|txn| txn.set("baz", "qux")).unwrap();
    db.close().unwrap();
    let db = DB::open(opts(dir.path(), &k)).unwrap();
    assert_eq!(count(&db), 2);
    db.close().unwrap();
}

#[test]
fn rotate_needs_a_closed_db() {
    let dir = tempfile::tempdir().unwrap();
    let k = key(4);
    let db = DB::open(opts(dir.path(), &k)).unwrap();
    assert!(rotate_master_key(dir.path(), &k, &key(5)).is_err());
    db.close().unwrap();
    // A bad new key is refused before anything is written.
    let err = rotate_master_key(dir.path(), &k, b"short").unwrap_err();
    assert!(matches!(err.root(), Error::InvalidEncryptionKey), "{err}");
    DB::open(opts(dir.path(), &k)).unwrap().close().unwrap();
}

#[test]
fn data_keys_rotate_by_age() {
    // A zero rotation period: every new table gets a fresh data key. All of
    // them stay readable, after reopening and after a master key rotation.
    let dir = tempfile::tempdir().unwrap();
    let k = key(6);
    let o = |key: &[u8]| {
        opts(dir.path(), key)
            .with_encryption_key_rotation_duration(Duration::ZERO)
            .with_mem_table_size(1 << 16)
            .with_value_threshold(64)
            .with_index_cache_size(1 << 20)
    };
    let db = DB::open(o(&k)).unwrap();
    for chunk in 0..10 {
        let mut wb = db.new_write_batch();
        for i in 0..300 {
            wb.set(format!("k{chunk:02}-{i:04}"), vec![chunk as u8; 100])
                .unwrap();
        }
        wb.flush().unwrap();
    }
    db.close().unwrap();

    let registry = KeyRegistry::open(KeyRegistryOptions {
        dir: dir.path().to_path_buf(),
        read_only: true,
        encryption_key: k.clone(),
        ..Default::default()
    })
    .unwrap();
    let keys = registry.data_keys().len();
    assert!(keys > 2, "{keys} data keys");
    drop(registry);

    let db = DB::open(o(&k)).unwrap();
    assert_eq!(count(&db), 3000);
    db.close().unwrap();
    let k2 = key(7);
    rotate_master_key(dir.path(), &k, &k2).unwrap();
    let db = DB::open(o(&k2)).unwrap();
    assert_eq!(count(&db), 3000);
    let v = db.view(|txn| txn.get(b"k07-0123")?.value_copy()).unwrap();
    assert_eq!(v, vec![7u8; 100]);
    db.close().unwrap();
}
