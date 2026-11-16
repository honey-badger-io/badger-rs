//! Memtable and WAL tests: replay, transactions, truncation, encryption.
//! (Go's memtable has no dedicated tests; its WAL logic is covered by the DB
//! and value-log tests, which come with later phases.)

use std::time::Duration;

use super::*;
use crate::key_registry::KeyRegistryOptions;
use crate::log_file::VLOG_HEADER_SIZE;
use crate::structs::{BIT_DELETE, BIT_TXN};
use crate::util::keys::key_with_ts;

fn options(dir: &Path) -> Options {
    Options::new(dir).with_mem_table_size(1 << 20)
}

fn registry(opt: &Options) -> KeyRegistry {
    KeyRegistry::open(KeyRegistryOptions::from_options(opt)).unwrap()
}

fn vs(value: &[u8], meta: u8) -> ValueStruct<'_> {
    ValueStruct {
        value,
        meta,
        ..Default::default()
    }
}

/// All (key, value, meta) in the skiplist, in order.
fn contents(mt: &MemTable) -> Vec<(Vec<u8>, Vec<u8>, u8)> {
    let mut it = mt.skiplist().iter();
    it.seek_to_first();
    let mut out = Vec::new();
    while it.valid() {
        let v = it.value();
        out.push((it.key().to_vec(), v.value.to_vec(), v.meta));
        it.next();
    }
    out
}

/// Writes a committed transaction: its entries with `BIT_TXN`, then the
/// finish marker (as the DB write path does).
fn put_txn(mt: &MemTable, ts: u64, kvs: &[(&str, &str)]) {
    for (k, v) in kvs {
        mt.put(&key_with_ts(k.as_bytes(), ts), vs(v.as_bytes(), BIT_TXN))
            .unwrap();
    }
    let fin = ts.to_string();
    mt.put(
        &key_with_ts(b"!badger!txn", ts),
        vs(fin.as_bytes(), BIT_FIN_TXN),
    )
    .unwrap();
}

#[test]
fn put_replay_round_trip() {
    let dir = tempfile::tempdir().unwrap();
    let opt = options(dir.path());
    let reg = registry(&opt);
    let mt = MemTable::create(&opt, &reg, 1).unwrap();
    put_txn(&mt, 5, &[("a", "1"), ("b", "2")]);
    mt.put(&key_with_ts(b"c", 7), vs(b"3", BIT_DELETE)).unwrap();
    put_txn(&mt, 9, &[("a", "4")]);
    let before = contents(&mt);
    assert_eq!(before.len(), 4, "finish markers are not in the skiplist");
    assert_eq!(mt.max_version(), 9);
    mt.sync_wal().unwrap();
    let wal_end = mt.wal_write_at().unwrap();
    drop(mt);
    let err = MemTable::create(&opt, &reg, 1).err().unwrap();
    assert!(err.to_string().contains("already exists"), "{err}");

    let (mts, next) = open_mem_tables(&opt, &reg).unwrap();
    assert_eq!(next, 2);
    assert_eq!(mts.len(), 1);
    assert_eq!(contents(&mts[0]), before);
    assert_eq!(mts[0].max_version(), 9);
    // Replay truncated the preallocated file to the data.
    let path = mem_file_path(dir.path(), 1);
    assert_eq!(fs::metadata(&path).unwrap().len(), wal_end as u64);

    let got = mts[0].skiplist().get(&key_with_ts(b"a", 8)).unwrap();
    assert_eq!((got.value, got.version), (&b"1"[..], 5));
}

/// A transaction without its finish marker is dropped on replay, and the WAL
/// is cut after the last complete transaction.
#[test]
fn incomplete_txn_is_dropped() {
    let dir = tempfile::tempdir().unwrap();
    let opt = options(dir.path());
    let reg = registry(&opt);
    let mt = MemTable::create(&opt, &reg, 3).unwrap();
    put_txn(&mt, 1, &[("a", "1")]);
    let complete_end = mt.wal_write_at().unwrap();
    mt.put(&key_with_ts(b"b", 2), vs(b"2", BIT_TXN)).unwrap();
    mt.put(&key_with_ts(b"c", 2), vs(b"3", BIT_TXN)).unwrap();
    drop(mt);

    // Read-only replay refuses to open a WAL that needs truncating.
    let ro = options(dir.path()).with_read_only(true);
    let err = MemTable::open(&ro, &reg, 3, false).err().unwrap();
    assert!(matches!(err.root(), Error::TruncateNeeded), "{err}");

    let (mt, new) = MemTable::open(&opt, &reg, 3, false).unwrap();
    assert!(!new);
    assert_eq!(contents(&mt).len(), 1);
    assert_eq!(mt.wal_write_at().unwrap(), complete_end);
    let path = mem_file_path(dir.path(), 3);
    assert_eq!(fs::metadata(&path).unwrap().len(), complete_end as u64);
}

/// Damage in the middle of the WAL: replay keeps everything before it.
#[test]
fn corrupt_entry_stops_replay() {
    let dir = tempfile::tempdir().unwrap();
    let opt = options(dir.path());
    let reg = registry(&opt);
    let mt = MemTable::create(&opt, &reg, 1).unwrap();
    let mut ends = Vec::new();
    for i in 0..10u64 {
        mt.put(&key_with_ts(format!("k{i}").as_bytes(), 1), vs(b"value", 0))
            .unwrap();
        ends.push(mt.wal_write_at().unwrap());
    }
    drop(mt);

    // Flip a byte inside entry 6 (its CRC no longer matches).
    let path = mem_file_path(dir.path(), 1);
    let mut bytes = fs::read(&path).unwrap();
    bytes[ends[5] as usize + 4] ^= 0xff;
    fs::write(&path, &bytes).unwrap();

    let (mts, _) = open_mem_tables(&opt, &reg).unwrap();
    assert_eq!(contents(&mts[0]).len(), 6);
    assert_eq!(fs::metadata(&path).unwrap().len(), ends[5] as u64);
}

/// A WAL with no entries is deleted on open; ids continue after the highest.
#[test]
fn empty_wal_is_removed() {
    let dir = tempfile::tempdir().unwrap();
    let opt = options(dir.path());
    let reg = registry(&opt);
    drop(MemTable::create(&opt, &reg, 4).unwrap());
    let mt = MemTable::create(&opt, &reg, 7).unwrap();
    mt.put(&key_with_ts(b"x", 1), vs(b"y", 0)).unwrap();
    drop(mt);

    let (mts, next) = open_mem_tables(&opt, &reg).unwrap();
    assert_eq!(next, 8);
    assert_eq!(mts.len(), 1);
    assert!(!mem_file_path(dir.path(), 4).exists());
    assert!(mem_file_path(dir.path(), 7).exists());
}

#[test]
fn delete_wal_on_drop() {
    let dir = tempfile::tempdir().unwrap();
    let opt = options(dir.path());
    let reg = registry(&opt);
    let mt = MemTable::create(&opt, &reg, 1).unwrap();
    let path = mt.wal_path().unwrap();
    // The skiplist can outlive the memtable (e.g. an open iterator).
    let sl = Arc::clone(mt.skiplist());
    mt.delete_wal_on_drop();
    drop(mt);
    assert!(!path.exists());
    assert!(sl.is_empty());
}

#[test]
fn is_full() {
    let dir = tempfile::tempdir().unwrap();
    let opt = Options::new(dir.path()).with_mem_table_size(64 << 10);
    let reg = registry(&opt);
    let mt = MemTable::create(&opt, &reg, 1).unwrap();
    let value = vec![7u8; 1000];
    let mut n = 0;
    while !mt.is_full() {
        mt.put(&key_with_ts(format!("{n:06}").as_bytes(), 1), vs(&value, 0))
            .unwrap();
        n += 1;
    }
    // Each entry takes ~1 KB in both the WAL and the arena.
    assert!((55..70).contains(&n), "{n}");
}

#[test]
fn in_memory_has_no_wal() {
    let opt = Options::new("").with_in_memory(true);
    let reg = registry(&opt);
    let mt = MemTable::create(&opt, &reg, 1).unwrap();
    mt.put(&key_with_ts(b"k", 1), vs(b"v", 0)).unwrap();
    assert!(mt.wal_path().is_none());
    assert_eq!(contents(&mt).len(), 1);
    assert!(open_mem_tables(&opt, &reg).unwrap().0.is_empty());
}

#[test]
#[cfg_attr(miri, ignore)] // AES intrinsics
fn encrypted_wal() {
    let dir = tempfile::tempdir().unwrap();
    let opt = options(dir.path())
        .with_encryption_key(vec![9u8; 32])
        .with_encryption_key_rotation_duration(Duration::from_secs(3600));
    let reg = registry(&opt);
    let mt = MemTable::create(&opt, &reg, 1).unwrap();
    put_txn(&mt, 3, &[("secret-key", "secret-value")]);
    drop(mt);

    let path = mem_file_path(dir.path(), 1);
    let bytes = fs::read(&path).unwrap();
    assert_eq!(
        u64::from_be_bytes(bytes[..8].try_into().unwrap()),
        1,
        "key id"
    );
    let find = |needle: &[u8]| bytes.windows(needle.len()).any(|w| w == needle);
    assert!(!find(b"secret-key") && !find(b"secret-value"));

    // Reopen with a fresh registry read from disk.
    let reg = registry(&opt);
    let (mts, _) = open_mem_tables(&opt, &reg).unwrap();
    assert_eq!(
        contents(&mts[0]),
        vec![(
            key_with_ts(b"secret-key", 3),
            b"secret-value".to_vec(),
            BIT_TXN
        )]
    );
}

/// The WAL header is the key id and a random base IV; entries start after it.
#[test]
fn wal_layout() {
    let dir = tempfile::tempdir().unwrap();
    let opt = options(dir.path());
    let reg = registry(&opt);
    let mt = MemTable::create(&opt, &reg, 1).unwrap();
    assert_eq!(mt.wal_write_at().unwrap(), VLOG_HEADER_SIZE);
    mt.put(&key_with_ts(b"k", 1), vs(b"v", 0)).unwrap();
    // header (2 + 1 + 1 + 1) + key (1 + 8) + value (1) + crc (4)
    assert_eq!(mt.wal_write_at().unwrap(), VLOG_HEADER_SIZE + 5 + 9 + 1 + 4);
    let path = mt.wal_path().unwrap();
    drop(mt);
    let bytes = fs::read(&path).unwrap();
    assert_eq!(bytes.len() as u64, 2 * opt.mem_table_size, "preallocated");
    assert_eq!(&bytes[..8], &[0; 8], "no data key");
    assert_eq!(&bytes[20..25], &[0, 0, 9, 1, 0]);
}
