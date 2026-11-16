//! Memtable WAL compatibility with Go Badger: replaying the `.mem` files of the
//! crash fixtures (copied from a live Go DB, so the WAL is preallocated and
//! never truncated) must give exactly the DB contents Go recovers.
//!
//! These fixtures have no SSTs, so the memtables hold everything. Values above
//! the threshold are value pointers, resolved here by reading the vlog file
//! with the same `LogFile` code.

use std::fs;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use badger::error::Error;
use badger::key_registry::{KeyRegistry, KeyRegistryOptions};
use badger::log_file::LogFile;
use badger::memtable::{MemTable, open_mem_tables};
use badger::structs::{BIT_DELETE, BIT_DISCARD_EARLIER_VERSIONS, BIT_VALUE_POINTER, ValuePointer};
use badger::util::keys::{parse_key, parse_ts};
use badger::{Options, options::CompressionType};
use badger_fixtures::{DbEntry, DbExpected, Fixtures, Kind};

const CRASH_FIXTURES: [&str; 2] = ["crash-wal", "crash-wal-encrypted"];

fn copy_dir(src: &Path, dst: &Path) {
    for entry in fs::read_dir(src).unwrap() {
        let entry = entry.unwrap();
        fs::copy(entry.path(), dst.join(entry.file_name())).unwrap();
    }
}

fn options(dir: &Path, exp: &DbExpected) -> Options {
    let o = &exp.options;
    Options::new(dir)
        .with_compression(CompressionType::parse(&o.compression).unwrap().0)
        .with_encryption_key(o.encryption_key.clone())
        .with_value_threshold(o.value_threshold as u64)
        .with_mem_table_size(o.mem_table_size as u64)
        .with_value_log_file_size(o.value_log_file_size as u64)
}

fn registry(opt: &Options) -> KeyRegistry {
    KeyRegistry::open(KeyRegistryOptions::from_options(opt)).unwrap()
}

/// Every entry in the memtables, as the fixture JSON describes it, plus how
/// many values came from the vlog.
fn dump(mts: &[MemTable], dir: &Path, reg: &KeyRegistry) -> (Vec<DbEntry>, usize) {
    let vlog_path = dir.join("000001.vlog");
    let (vlog, _) = LogFile::open(&vlog_path, 1, false, true, 0, reg).unwrap();
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let mut out = Vec::new();
    let mut from_vlog = 0;
    // Newer memtables shadow older ones; each key appears in one here, so a
    // plain concatenation sorted by key (versions descending) suffices.
    for mt in mts {
        let mut it = mt.skiplist().iter();
        it.seek_to_first();
        while it.valid() {
            let (k, v) = (it.key(), it.value());
            let value = if v.meta & BIT_VALUE_POINTER != 0 {
                let vp = ValuePointer::decode(v.value).unwrap();
                assert_eq!(vp.fid, 1, "fixture has one vlog file");
                let buf = vlog.read(vp).unwrap();
                from_vlog += 1;
                vlog.decode_entry(buf, vp.offset).unwrap().value
            } else {
                v.value.to_vec()
            };
            out.push(DbEntry {
                key: parse_key(k).to_vec(),
                version: parse_ts(k),
                value,
                user_meta: v.user_meta,
                expires_at: v.expires_at,
                deleted_or_expired: v.meta & BIT_DELETE != 0
                    || (v.expires_at != 0 && v.expires_at <= now),
                discard_earlier_versions: v.meta & BIT_DISCARD_EARLIER_VERSIONS != 0,
            });
            it.next();
        }
    }
    out.sort_by(|a, b| a.key.cmp(&b.key).then(b.version.cmp(&a.version)));
    (out, from_vlog)
}

#[test]
#[cfg_attr(miri, ignore)]
fn replays_go_crash_wals() {
    let fx = Fixtures::load().expect("fixtures missing: run `make fixtures`");
    for name in CRASH_FIXTURES {
        let entry = fx.get(Kind::Db, name).unwrap();
        let exp = fx.db_expected(entry).unwrap();
        assert!(exp.crash_snapshot);

        // Replay truncates the WAL, so work on a copy.
        let tmp = tempfile::tempdir().unwrap();
        copy_dir(&fx.path(entry), tmp.path());
        let opt = options(tmp.path(), &exp);
        let reg = registry(&opt);

        // Go cannot open these read-only, and neither can we.
        let ro = opt.clone().with_read_only(true);
        let err = open_mem_tables(&ro, &registry(&ro)).err().unwrap();
        assert!(matches!(err.root(), Error::TruncateNeeded), "{name}: {err}");

        let (mts, next_fid) = open_mem_tables(&opt, &reg).unwrap();
        assert!(!mts.is_empty(), "{name}: no memtables");
        let max_version = mts.iter().map(|m| m.max_version()).max().unwrap();
        assert_eq!(max_version, exp.max_version, "{name}: max version");

        let (got, from_vlog) = dump(&mts, tmp.path(), &reg);
        // The fixture exercises value pointers and deletes.
        assert!(from_vlog > 100, "{name}: {from_vlog} vlog values");
        assert!(
            got.iter().any(|e| e.deleted_or_expired),
            "{name}: no deletes"
        );
        assert_eq!(got.len(), exp.entries.len(), "{name}: entry count");
        for (g, w) in got.iter().zip(&exp.entries) {
            assert_eq!(g, w, "{name}: entry {}", hex::encode(&w.key));
        }

        // The WAL is now truncated, and a second replay gives the same result.
        let wal_len = |fid: u32| {
            fs::metadata(tmp.path().join(format!("{fid:05}.mem")))
                .unwrap()
                .len()
        };
        let first_fid = next_fid - 1;
        let truncated = wal_len(first_fid);
        assert!(truncated < 2 * exp.options.mem_table_size as u64);
        drop(mts);
        let (mts, _) = open_mem_tables(&ro, &registry(&ro)).unwrap();
        assert_eq!(
            dump(&mts, tmp.path(), &reg).0,
            got,
            "{name}: read-only reopen"
        );
        assert_eq!(wal_len(first_fid), truncated);
    }
}
