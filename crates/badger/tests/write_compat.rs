//! Phase 5 milestone: Rust writes databases that Go Badger accepts.
//!
//! Each scenario writes a DB with the real write path (transactions, the
//! value log, memtable flushes to L0), closes it, reopens it read-only and
//! checks the contents against an in-memory model. The all-versions dump is
//! saved next to the DB in the fixture JSON format. With
//! `BADGER_COMPAT_OUT=<dir>` set, the DBs and JSON go there, and
//! `go-fixtures verify <dir>/db/<name> <dir>/db/<name>.json` checks them with
//! Go (`make compat`, CI).

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use badger::options::CompressionType;
use badger::{DB, Entry, IteratorOptions, Options};
use badger_fixtures::{DbEntry, DbExpected, DbOptions};

/// Fixed expiry times, as in the Go fixtures: long expired, and 2100-01-01.
const EXPIRED_AT: u64 = 1;
const NOT_EXPIRED_AT: u64 = 4_102_444_800;

struct Scenario {
    name: &'static str,
    opts: DbOptions,
    write: fn(&DB, &mut Model),
}

/// What a latest-version read should return: key → (value, user meta).
type Model = BTreeMap<Vec<u8>, (Vec<u8>, u8)>;

fn base() -> DbOptions {
    DbOptions {
        compression: "none".into(),
        encryption_key: Vec::new(),
        value_threshold: 16 << 10,
        mem_table_size: 512 << 10,
        value_log_file_size: 1 << 20,
        base_table_size: 256 << 10,
        num_versions_to_keep: 1,
        managed: false,
    }
}

fn options(dir: &Path, o: &DbOptions) -> Options {
    let mut opt = Options::new(dir)
        .with_compression(CompressionType::parse(&o.compression).unwrap().0)
        .with_value_threshold(o.value_threshold as u64)
        .with_mem_table_size(o.mem_table_size as u64)
        .with_value_log_file_size(o.value_log_file_size as u64)
        .with_base_table_size(o.base_table_size as u64)
        .with_num_versions_to_keep(o.num_versions_to_keep as usize)
        .with_num_compactors(0);
    if !o.encryption_key.is_empty() {
        opt = opt
            .with_encryption_key(o.encryption_key.clone())
            .with_index_cache_size(1 << 20);
    }
    opt
}

/// A deterministic pseudo-random generator (xorshift64*).
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_f491_4f6c_dd1d)
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
    fn bytes(&mut self, n: usize) -> Vec<u8> {
        (0..n).map(|_| self.next() as u8).collect()
    }
    /// `base` plus up to `extra - 1` random bytes.
    fn bytes_between(&mut self, base: usize, extra: u64) -> Vec<u8> {
        let n = base + self.below(extra) as usize;
        self.bytes(n)
    }
}

/// Writes `entries` in transactions of up to `per_txn`, starting a new one
/// early when a transaction gets too big, and records them in `model`.
fn write_entries(db: &DB, model: &mut Model, entries: Vec<Entry>, per_txn: usize) {
    for chunk in entries.chunks(per_txn) {
        let mut txn = db.new_transaction(true);
        for e in chunk {
            if let Err(badger::Error::TxnTooBig) = txn.set_entry(e.clone()) {
                txn.commit().unwrap();
                txn = db.new_transaction(true);
                txn.set_entry(e.clone()).unwrap();
            }
        }
        txn.commit().unwrap();
        for e in chunk {
            if e.expires_at == EXPIRED_AT {
                model.remove(&e.key);
            } else {
                model.insert(e.key.clone(), (e.value.clone(), e.user_meta));
            }
        }
    }
}

fn delete_keys(db: &DB, model: &mut Model, keys: &[Vec<u8>]) {
    let mut txn = db.new_transaction(true);
    for k in keys {
        if let Err(badger::Error::TxnTooBig) = txn.delete(k.clone()) {
            txn.commit().unwrap();
            txn = db.new_transaction(true);
            txn.delete(k.clone()).unwrap();
        }
        model.remove(k);
    }
    txn.commit().unwrap();
}

fn key(prefix: &str, i: u64) -> Vec<u8> {
    format!("{prefix}-{i:07}").into_bytes()
}

fn write_basic(db: &DB, model: &mut Model) {
    let mut r = Rng(0xbad6e5);
    let mut es: Vec<Entry> = (0..8000)
        .map(|i| Entry::new(key("basic", i), r.bytes_between(8, 56)))
        .collect();
    // Binary keys with 0x00 / 0xff bytes.
    for _ in 0..100 {
        let mut k = r.bytes_between(1, 40);
        k[0] = [0x00, 0x01, 0xfe, 0xff][r.below(4) as usize];
        if k.starts_with(b"!badger!") {
            continue;
        }
        let v = r.bytes_between(0, 32);
        es.push(Entry::new(k, v));
    }
    // Duplicate binary keys in one transaction keep the last write; avoid
    // ambiguity in the model by deduplicating.
    let mut seen = std::collections::HashSet::new();
    es.retain(|e| seen.insert(e.key.clone()));
    write_entries(db, model, es, 400);
}

fn write_vlog(db: &DB, model: &mut Model) {
    let mut r = Rng(0x7106);
    let es: Vec<Entry> = (0..2500)
        .map(|i| {
            let n = if i % 5 == 0 {
                r.below(64)
            } else {
                100 + r.below(1400)
            };
            Entry::new(key("vlog", i), r.bytes(n as usize))
        })
        .collect();
    write_entries(db, model, es, 50);
}

fn write_versions(db: &DB, model: &mut Model) {
    let mut r = Rng(0x7e75);
    let n = 1000;
    for round in 0..4 {
        let picked: Vec<u64> = (0..n)
            .filter(|i| round == 0 || r.below(3) == 0 || i % 7 == round)
            .collect();
        let es: Vec<Entry> = picked
            .into_iter()
            .map(|i| {
                let v = r.bytes_between(16, 384);
                let mut e = Entry::new(key("ver", i), v).with_meta((i % 4) as u8);
                match i % 11 {
                    0 => e.expires_at = EXPIRED_AT,
                    1 => e.expires_at = NOT_EXPIRED_AT,
                    2 if round == 3 => e = e.with_discard(),
                    _ => {}
                }
                e
            })
            .collect();
        write_entries(db, model, es, 100);
        let dels: Vec<Vec<u8>> = (0..40).map(|_| key("ver", r.below(n))).collect();
        delete_keys(db, model, &dels);
    }
}

fn write_text(prefix: &'static str) -> impl Fn(&DB, &mut Model) {
    move |db, model| {
        let words = [
            "badger", "level", "table", "value", "log", "rust", "go", "txn",
        ];
        let mut r = Rng(0x7e47);
        let es: Vec<Entry> = (0..3000)
            .map(|i| {
                let mut v = Vec::new();
                let n = 20 + r.below(480) as usize;
                while v.len() < n {
                    v.extend_from_slice(words[r.below(words.len() as u64) as usize].as_bytes());
                    v.push(b' ');
                }
                v.truncate(n);
                Entry::new(key(prefix, i), v)
            })
            .collect();
        write_entries(db, model, es, 100);
    }
}

fn scenarios() -> Vec<Scenario> {
    let with = |f: fn(&mut DbOptions)| {
        let mut o = base();
        f(&mut o);
        o
    };
    vec![
        Scenario {
            name: "rust-basic",
            opts: base(),
            write: write_basic,
        },
        Scenario {
            name: "rust-vlog",
            opts: with(|o| o.value_threshold = 64),
            write: write_vlog,
        },
        Scenario {
            name: "rust-versions",
            opts: with(|o| {
                o.value_threshold = 128;
                o.num_versions_to_keep = 1000;
            }),
            write: write_versions,
        },
        Scenario {
            // Small memtables: versions of a key spread over many L0 tables.
            name: "rust-l0",
            opts: with(|o| {
                o.mem_table_size = 16 << 10;
                o.value_threshold = 128;
                o.num_versions_to_keep = 1000;
            }),
            write: write_versions,
        },
        Scenario {
            name: "rust-zstd",
            opts: with(|o| {
                o.compression = "zstd".into();
                o.value_threshold = 256;
            }),
            write: |db, m| write_text("zstd")(db, m),
        },
        Scenario {
            name: "rust-encrypted",
            opts: with(|o| {
                o.encryption_key = (0..32).collect();
                o.value_threshold = 128;
            }),
            write: write_vlog,
        },
        Scenario {
            name: "rust-encrypted-snappy",
            opts: with(|o| {
                o.encryption_key = (0..16).collect();
                o.compression = "snappy".into();
                o.value_threshold = 256;
            }),
            write: |db, m| write_text("snappy")(db, m),
        },
    ]
}

/// Every version of every key, as the Go fixture dump lists them.
fn dump(db: &DB) -> Vec<DbEntry> {
    db.view(|txn| {
        let mut it = txn.iter(IteratorOptions {
            all_versions: true,
            ..Default::default()
        });
        let mut out = Vec::new();
        it.rewind();
        while it.valid() {
            let item = it.item();
            out.push(DbEntry {
                key: item.key().to_vec(),
                version: item.version(),
                value: item.value_copy()?,
                user_meta: item.user_meta(),
                expires_at: item.expires_at(),
                deleted_or_expired: item.is_deleted_or_expired(),
                discard_earlier_versions: item.discard_earlier_versions(),
            });
            it.next();
        }
        Ok(out)
    })
    .unwrap()
}

fn latest(db: &DB) -> Model {
    db.view(|txn| {
        let mut it = txn.iter(IteratorOptions::default());
        let mut out = Model::new();
        it.rewind();
        while it.valid() {
            let item = it.item();
            out.insert(item.key().to_vec(), (item.value_copy()?, item.user_meta()));
            it.next();
        }
        Ok(out)
    })
    .unwrap()
}

fn out_dir() -> (Option<tempfile::TempDir>, PathBuf) {
    match std::env::var_os("BADGER_COMPAT_OUT") {
        Some(out) => (None, PathBuf::from(out).join("db")),
        None => {
            let tmp = tempfile::tempdir().unwrap();
            let p = tmp.path().to_path_buf();
            (Some(tmp), p)
        }
    }
}

#[test]
fn writes_dbs_go_can_read() {
    let (_tmp, out) = out_dir();
    for s in scenarios() {
        let dir = out.join(s.name);
        let _ = fs::remove_dir_all(&dir);
        let opt = options(&dir, &s.opts);
        let mut model = Model::new();
        let db = DB::open(opt.clone()).unwrap();
        (s.write)(&db, &mut model);
        db.close().unwrap();

        // Nothing is left to replay: Go can open it read-only.
        let names: Vec<String> = fs::read_dir(&dir)
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        assert!(
            !names.iter().any(|n| n.ends_with(".mem")),
            "{}: {names:?}",
            s.name
        );
        assert!(
            names.iter().any(|n| n.ends_with(".sst")),
            "{}: {names:?}",
            s.name
        );

        let db = DB::open(opt.with_read_only(true)).unwrap();
        assert_eq!(latest(&db), model, "{}: latest versions", s.name);
        let entries = dump(&db);
        let max_version = db.max_version();
        db.close().unwrap();

        if s.name == "rust-l0" {
            let ssts = names.iter().filter(|n| n.ends_with(".sst")).count();
            assert!(ssts >= 5, "{}: {ssts} tables", s.name);
        }
        if s.name == "rust-vlog" {
            let vlogs = names.iter().filter(|n| n.ends_with(".vlog")).count();
            assert!(vlogs >= 2, "{}: {vlogs} vlog files", s.name);
        }
        DbExpected {
            format_version: 1,
            name: s.name.into(),
            options: s.opts.clone(),
            crash_snapshot: false,
            max_version,
            entries,
        }
        .write(&out.join(format!("{}.json", s.name)))
        .unwrap();
    }
}

/// Managed mode: explicit commit timestamps.
#[test]
fn managed_db_go_can_read() {
    let (_tmp, out) = out_dir();
    let name = "rust-managed";
    let dir = out.join(name);
    let _ = fs::remove_dir_all(&dir);
    let mut o = base();
    o.managed = true;
    o.num_versions_to_keep = 1000;
    let opt = options(&dir, &o);
    let db = DB::open_managed(opt.clone()).unwrap();
    for (round, ts) in [10u64, 20, 30, 40, 50].into_iter().enumerate() {
        let mut txn = db.new_transaction_at(ts, true);
        for i in 0..200u64 {
            if (i + round as u64).is_multiple_of(3) {
                txn.set(key("managed", i), format!("v{ts}-{i}")).unwrap();
            }
        }
        if round == 2 {
            txn.delete(key("managed", 5)).unwrap();
        }
        txn.commit_at(ts).unwrap();
    }
    db.close().unwrap();

    let db = DB::open_managed(opt.with_read_only(true)).unwrap();
    assert_eq!(db.max_version(), 50);
    let entries = dump(&db);
    // Reads at 25 see versions 10 and 20 only.
    let txn = db.new_transaction_at(25, false);
    let item = txn.get(&key("managed", 2)).unwrap();
    assert_eq!(item.version(), 20);
    drop(txn);
    db.close().unwrap();
    DbExpected {
        format_version: 1,
        name: name.into(),
        options: o,
        crash_snapshot: false,
        max_version: 50,
        entries,
    }
    .write(&out.join(format!("{name}.json")))
    .unwrap();
}

/// Phase 7: a DB copied with Stream → StreamWriter, which builds the
/// tables directly (bottom level, value log for big values).
#[test]
fn stream_copy_go_can_read() {
    let (_tmp, out) = out_dir();
    let name = "rust-stream-copy";
    let mut o = base();
    o.value_threshold = 128;
    o.num_versions_to_keep = 1000;

    // The source: versions, deletes, expiries, value log values.
    let src_dir = tempfile::tempdir().unwrap();
    let src = DB::open(options(src_dir.path(), &o)).unwrap();
    let mut model = Model::new();
    write_versions(&src, &mut model);
    write_vlog(&src, &mut model);

    let dir = out.join(name);
    let _ = fs::remove_dir_all(&dir);
    let opt = options(&dir, &o);
    let dst = DB::open(opt.clone()).unwrap();
    let mut sw = dst.new_stream_writer();
    sw.prepare().unwrap();
    let mut stream = src.new_stream();
    stream.send_done_markers(true);
    stream.orchestrate(|list| sw.write(&list)).unwrap();
    drop(stream);
    sw.flush().unwrap();
    assert_eq!(latest(&dst), model, "copy right after the stream write");
    // New commits get timestamps above every copied version.
    assert!(dst.max_version() >= src.max_version());
    src.close().unwrap();
    dst.close().unwrap();

    let names: Vec<String> = fs::read_dir(&dir)
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    assert!(names.iter().any(|n| n.ends_with(".vlog")), "{names:?}");
    let db = DB::open(opt.with_read_only(true)).unwrap();
    assert_eq!(latest(&db), model, "copy after reopening");
    // All tables sit on the last level.
    assert!(
        db.tables().iter().all(|t| t.level == 6),
        "{:?}",
        db.tables()
    );
    let entries = dump(&db);
    let max_version = db.max_version();
    db.close().unwrap();
    DbExpected {
        format_version: 1,
        name: name.into(),
        options: o,
        crash_snapshot: false,
        max_version,
        entries,
    }
    .write(&out.join(format!("{name}.json")))
    .unwrap();
}
