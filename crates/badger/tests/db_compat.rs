//! Phase 4 milestone: Rust opens every Go-written fixture DB read-only and
//! reads exactly what Go reads (the fixture's JSON dump, made by a Go
//! iterator with `AllVersions` at the newest read timestamp).
//!
//! Besides the all-versions iteration, this checks point gets, latest-version
//! iteration (forward and reverse), prefix iteration, seeks and key
//! iterators against what the dump implies.

use std::collections::BTreeMap;
use std::path::Path;

use badger::options::CompressionType;
use badger::{DB, Error, IteratorOptions, Options, Txn};
use badger_fixtures::{DbEntry, DbExpected, Fixtures, IndexEntry};

fn fixtures() -> Fixtures {
    Fixtures::load().expect("fixtures missing: run `make fixtures`")
}

fn options(dir: &Path, exp: &DbExpected) -> Options {
    let o = &exp.options;
    let mut opt = Options::new(dir)
        .with_read_only(true)
        .with_compression(CompressionType::parse(&o.compression).unwrap().0)
        .with_value_threshold(o.value_threshold as u64)
        .with_mem_table_size(o.mem_table_size as u64)
        .with_value_log_file_size(o.value_log_file_size as u64)
        .with_base_table_size(o.base_table_size as u64)
        .with_num_versions_to_keep(o.num_versions_to_keep as usize)
        .with_verify_value_checksum(true)
        .with_checksum_verification_mode(
            badger::options::ChecksumVerificationMode::OnTableAndBlockRead,
        );
    if !o.encryption_key.is_empty() {
        opt = opt
            .with_encryption_key(o.encryption_key.clone())
            .with_index_cache_size(1 << 20);
    }
    opt
}

fn open(fx: &Fixtures, entry: &IndexEntry) -> (DB, DbExpected) {
    let exp = fx.db_expected(entry).unwrap();
    let db = DB::open(options(&fx.path(entry), &exp))
        .unwrap_or_else(|e| panic!("{}: open: {e}", entry.name));
    (db, exp)
}

/// Fixtures Go can open read-only (the crash snapshots need WAL truncation).
fn readable(fx: &Fixtures) -> Vec<IndexEntry> {
    fx.dbs()
        .filter(|e| !e.name.starts_with("crash-wal"))
        .cloned()
        .collect()
}

fn dump_all_versions(txn: &Txn<'_>, reverse: bool) -> Vec<DbEntry> {
    let mut it = txn.iter(IteratorOptions {
        all_versions: true,
        reverse,
        ..Default::default()
    });
    let mut out = Vec::new();
    it.rewind();
    while it.valid() {
        let item = it.item();
        out.push(DbEntry {
            key: item.key().to_vec(),
            version: item.version(),
            value: item.value_copy().unwrap(),
            user_meta: item.user_meta(),
            expires_at: item.expires_at(),
            deleted_or_expired: item.is_deleted_or_expired(),
            discard_earlier_versions: item.discard_earlier_versions(),
        });
        it.next();
    }
    out
}

/// What a normal (latest-version) read sees: for each key, its newest
/// version, unless that one is deleted or expired.
fn latest(entries: &[DbEntry]) -> BTreeMap<Vec<u8>, &DbEntry> {
    let mut out = BTreeMap::new();
    let mut seen = std::collections::HashSet::new();
    for e in entries {
        // Entries are sorted by key, newest version first.
        if seen.insert(e.key.clone()) && !e.deleted_or_expired {
            out.insert(e.key.clone(), e);
        }
    }
    out
}

#[test]
fn all_versions_match_go() {
    let fx = fixtures();
    let dbs = readable(&fx);
    assert!(dbs.len() >= 9, "{} fixtures", dbs.len());
    for entry in dbs {
        let (db, exp) = open(&fx, &entry);
        assert_eq!(
            db.max_version(),
            exp.max_version,
            "{}: max version",
            entry.name
        );
        db.view(|txn| {
            let got = dump_all_versions(txn, false);
            assert_eq!(got.len(), exp.entries.len(), "{}: entry count", entry.name);
            for (i, (g, w)) in got.iter().zip(&exp.entries).enumerate() {
                assert_eq!(g, w, "{}: entry {i}", entry.name);
            }
            // Reverse all-versions iteration gives the same entries backwards
            // (versions ascending within a key).
            let mut rev = dump_all_versions(txn, true);
            rev.reverse();
            assert_eq!(rev, got, "{}: reverse all versions", entry.name);
            Ok(())
        })
        .unwrap();
        db.close().unwrap();
    }
}

#[test]
fn latest_versions_and_gets_match_go() {
    let fx = fixtures();
    for entry in readable(&fx) {
        let (db, exp) = open(&fx, &entry);
        let want = latest(&exp.entries);
        db.view(|txn| {
            // Forward and reverse latest-version iteration.
            for reverse in [false, true] {
                let mut it = txn.iter(IteratorOptions {
                    reverse,
                    ..Default::default()
                });
                let mut got = Vec::new();
                it.rewind();
                while it.valid() {
                    let item = it.item();
                    got.push((item.key().to_vec(), item.version(), item.value_copy()?));
                    it.next();
                }
                let mut expected: Vec<_> = want
                    .values()
                    .map(|e| (e.key.clone(), e.version, e.value.clone()))
                    .collect();
                if reverse {
                    expected.reverse();
                }
                assert_eq!(got, expected, "{}: reverse={reverse}", entry.name);
            }

            // Point gets of every key: the newest live version, or not found.
            let mut keys: Vec<&Vec<u8>> = exp.entries.iter().map(|e| &e.key).collect();
            keys.dedup();
            for key in keys {
                match (txn.get(key), want.get(key)) {
                    (Ok(item), Some(w)) => {
                        assert_eq!(item.version(), w.version, "{}", entry.name);
                        assert_eq!(item.value_copy()?, w.value, "{}", entry.name);
                        assert_eq!(item.user_meta(), w.user_meta);
                        assert_eq!(item.expires_at(), w.expires_at);
                    }
                    (Err(Error::KeyNotFound), None) => {}
                    (got, want) => panic!(
                        "{}: get {}: got {got:?}, want {want:?}",
                        entry.name,
                        hex::encode(key)
                    ),
                }
            }
            assert!(matches!(txn.get(b"no-such-key"), Err(Error::KeyNotFound)));
            assert!(matches!(txn.get(b""), Err(Error::EmptyKey)));
            Ok(())
        })
        .unwrap();
    }
}

#[test]
fn prefix_seek_and_key_iterators() {
    let fx = fixtures();
    for entry in readable(&fx) {
        let (db, exp) = open(&fx, &entry);
        let want = latest(&exp.entries);
        let keys: Vec<&Vec<u8>> = want.keys().collect();
        if keys.is_empty() {
            continue;
        }
        db.view(|txn| {
            // A prefix: the first 5 bytes of a key in the middle.
            let mid = keys[keys.len() / 2];
            let prefix = &mid[..mid.len().min(5)];
            let mut it = txn.iter(IteratorOptions {
                prefix: prefix.to_vec(),
                ..Default::default()
            });
            let mut got = Vec::new();
            it.rewind();
            while it.valid() {
                got.push(it.item().key().to_vec());
                it.next();
            }
            let expected: Vec<Vec<u8>> = keys
                .iter()
                .filter(|k| k.starts_with(prefix))
                .map(|k| k.to_vec())
                .collect();
            assert!(!expected.is_empty());
            assert_eq!(
                got,
                expected,
                "{}: prefix {}",
                entry.name,
                hex::encode(prefix)
            );

            // Seek to every 97th key, and just past it.
            let mut it = txn.iter(IteratorOptions::default());
            for (i, k) in keys.iter().enumerate().step_by(97) {
                it.seek(k);
                assert!(it.valid());
                assert_eq!(it.item().key(), k.as_slice(), "{}", entry.name);
                let mut past = k.to_vec();
                past.push(0);
                it.seek(&past);
                match keys.get(i + 1) {
                    Some(next) => assert_eq!(it.item().key(), next.as_slice()),
                    None => assert!(!it.valid()),
                }
            }
            // Reverse seek lands on the key itself or the one before.
            let mut it = txn.iter(IteratorOptions {
                reverse: true,
                ..Default::default()
            });
            for (i, k) in keys.iter().enumerate().skip(1).step_by(89) {
                it.seek(k);
                assert_eq!(it.item().key(), k.as_slice(), "{}", entry.name);
                let mut before = k.to_vec();
                before.pop();
                if before.as_slice() > keys[i - 1].as_slice() {
                    it.seek(&before);
                    assert_eq!(it.item().key(), keys[i - 1].as_slice());
                }
            }

            // Key iterators return every version of one key.
            for e in exp.entries.iter().step_by(131) {
                let mut it = txn.key_iter(&e.key, IteratorOptions::default());
                let mut versions = Vec::new();
                it.rewind();
                while it.valid() {
                    versions.push(it.item().version());
                    it.next();
                }
                let expected: Vec<u64> = exp
                    .entries
                    .iter()
                    .filter(|x| x.key == e.key)
                    .map(|x| x.version)
                    .collect();
                assert_eq!(versions, expected, "{}", entry.name);
            }
            Ok(())
        })
        .unwrap();
    }
}

/// Reads at older timestamps see older versions (managed mode).
#[test]
fn managed_reads_at_older_timestamps() {
    let fx = fixtures();
    let entry = fx.get(badger_fixtures::Kind::Db, "managed").unwrap();
    let exp = fx.db_expected(entry).unwrap();
    let db = DB::open_managed(options(&fx.path(entry), &exp)).unwrap();
    for read_ts in [0, 10, 25, 50, u64::MAX] {
        let visible: Vec<&DbEntry> = exp
            .entries
            .iter()
            .filter(|e| e.version <= read_ts)
            .collect();
        let want = latest(&visible.iter().map(|e| (*e).clone()).collect::<Vec<_>>())
            .into_iter()
            .map(|(k, e)| (k, e.version))
            .collect::<Vec<_>>();
        let txn = db.new_transaction_at(read_ts, false);
        let mut it = txn.iter(IteratorOptions::default());
        let mut got = Vec::new();
        it.rewind();
        while it.valid() {
            got.push((it.item().key().to_vec(), it.item().version()));
            it.next();
        }
        assert_eq!(got, want, "read_ts={read_ts}");
    }
}

#[test]
fn crash_snapshots_need_truncation() {
    let fx = fixtures();
    for entry in fx.dbs().filter(|e| e.name.starts_with("crash-wal")) {
        let exp = fx.db_expected(entry).unwrap();
        let err = DB::open(options(&fx.path(entry), &exp)).err().unwrap();
        assert!(
            matches!(err.root(), Error::TruncateNeeded),
            "{}: {err}",
            entry.name
        );
    }
}

#[test]
fn shared_lock_and_wrong_key() {
    let fx = fixtures();
    let entry = fx.get(badger_fixtures::Kind::Db, "encrypted").unwrap();
    let exp = fx.db_expected(entry).unwrap();
    let opt = options(&fx.path(entry), &exp);
    // Read-only opens share the directory lock.
    let a = DB::open(opt.clone()).unwrap();
    let b = DB::open(opt.clone()).unwrap();
    drop((a, b));
    let err = DB::open(opt.with_encryption_key(vec![0xee; 32]))
        .err()
        .unwrap();
    assert!(matches!(err.root(), Error::EncryptionKeyMismatch), "{err}");
}
