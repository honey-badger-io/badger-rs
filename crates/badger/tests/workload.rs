//! Phase 6 milestone: a long randomized workload with compaction and value
//! log GC running, checked against a `BTreeMap` model.
//!
//! A writer applies random sets (inline and value-log values), deletes,
//! expired entries, drop_prefix and drop_all, runs value log GC, and reopens
//! the DB now and then. Reader threads check point reads and scans against
//! the snapshot they read at, while compaction and GC move data around. At
//! the end the DB is reopened read-only, compared with the model, and dumped
//! in the fixture JSON format so Go can verify it (`make compat`), like
//! `write_compat.rs`.
//!
//! `BADGER_WORKLOAD_OPS=<n>` changes the number of operations per scenario
//! (default 20,000).

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use badger::options::CompressionType;
use badger::{DB, Entry, Error, IteratorOptions, Options};
use badger_fixtures::{DbEntry, DbExpected, DbOptions};

/// Key → value. Values encode the key and a write counter, so readers can
/// check what they see without sharing the model.
type Model = BTreeMap<Vec<u8>, Vec<u8>>;

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
}

const KEYS: u64 = 3000;

fn key(i: u64) -> Vec<u8> {
    // Two-digit groups so drop_prefix("kNN") hits about 1% of the keys.
    format!("k{:02}-{i:06}", i % 100).into_bytes()
}

/// A value whose head names the key and the write; small values stay in the
/// LSM tree, big ones go to the value log.
fn value(k: &[u8], n: u64, len: usize) -> Vec<u8> {
    let mut v = format!("{}#{n}|", String::from_utf8_lossy(k)).into_bytes();
    while v.len() < len {
        v.push(b'a' + (v.len() % 26) as u8);
    }
    v
}

/// A value read back must belong to its key.
fn check_value(k: &[u8], v: &[u8]) {
    let head = format!("{}#", String::from_utf8_lossy(k));
    assert!(
        v.starts_with(head.as_bytes()),
        "value of {} is {:?}",
        String::from_utf8_lossy(k),
        String::from_utf8_lossy(&v[..v.len().min(40)])
    );
}

struct Scenario {
    name: &'static str,
    compression: &'static str,
    encryption_key: Vec<u8>,
}

fn db_options(s: &Scenario) -> DbOptions {
    DbOptions {
        compression: s.compression.into(),
        encryption_key: s.encryption_key.clone(),
        value_threshold: 512,
        mem_table_size: 128 << 10,
        value_log_file_size: 1 << 20,
        base_table_size: 32 << 10,
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
        .with_base_level_size(128 << 10)
        .with_num_versions_to_keep(o.num_versions_to_keep as usize)
        .with_num_level_zero_tables(2)
        .with_num_level_zero_tables_stall(6)
        .with_num_compactors(3);
    if !o.encryption_key.is_empty() {
        opt = opt
            .with_encryption_key(o.encryption_key.clone())
            .with_index_cache_size(1 << 20);
    }
    opt
}

fn latest(db: &DB) -> Model {
    db.view(|txn| {
        let mut it = txn.iter(IteratorOptions::default());
        let mut out = Model::new();
        it.rewind();
        while it.valid() {
            let item = it.item();
            out.insert(item.key().to_vec(), item.value_copy()?);
            it.next();
        }
        Ok(out)
    })
    .unwrap()
}

/// Asserts `got == want`, describing the first differences briefly.
fn assert_same(got: &Model, want: &Model, what: &str) {
    assert_same_db(got, want, what, None)
}

fn assert_same_db(got: &Model, want: &Model, what: &str, db: Option<&DB>) {
    if got == want {
        return;
    }
    if let Some(db) = db {
        let first = got
            .iter()
            .find(|(k, v)| want.get(*k) != Some(v))
            .map(|(k, _)| k.clone());
        if let Some(k) = first {
            for e in dump(db).iter().filter(|e| e.key == k) {
                eprintln!(
                    "{} v{} del={} {:?}",
                    String::from_utf8_lossy(&e.key),
                    e.version,
                    e.deleted_or_expired,
                    String::from_utf8_lossy(&e.value[..e.value.len().min(20)])
                );
            }
            for t in db.tables() {
                eprintln!("table L{} id{} maxv{}", t.level, t.id, t.max_version);
            }
        }
    }
    let short = |v: Option<&Vec<u8>>| {
        v.map(|v| String::from_utf8_lossy(&v[..v.len().min(24)]).into_owned())
    };
    let mut diffs = Vec::new();
    for k in got
        .keys()
        .chain(want.keys())
        .collect::<std::collections::BTreeSet<_>>()
    {
        let (g, w) = (got.get(k), want.get(k));
        if g != w {
            diffs.push(format!(
                "{}: got {:?}, want {:?}",
                String::from_utf8_lossy(k),
                short(g),
                short(w)
            ));
        }
    }
    let n = diffs.len();
    diffs.truncate(15);
    panic!("{what}: {n} keys differ:\n{}", diffs.join("\n"));
}

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

/// One reader round: point reads and a prefix scan in one snapshot. Every
/// value must belong to its key and a scan must be sorted.
fn read_round(db: &DB, r: &mut Rng) -> badger::Result<()> {
    db.view(|txn| {
        for _ in 0..20 {
            let k = key(r.below(KEYS));
            match txn.get(&k) {
                Ok(item) => item.value(|v| check_value(&k, v))?,
                Err(Error::KeyNotFound) => {}
                Err(e) => return Err(e),
            }
        }
        let mut it = txn.iter(IteratorOptions {
            prefix: format!("k{:02}", r.below(100)).into_bytes(),
            ..Default::default()
        });
        let mut prev: Option<Vec<u8>> = None;
        it.rewind();
        while it.valid() {
            let item = it.item();
            let k = item.key().to_vec();
            item.value(|v| check_value(&k, v))?;
            assert!(prev.as_ref().is_none_or(|p| *p < k), "scan out of order");
            prev = Some(k);
            it.next();
        }
        Ok(())
    })
}

#[derive(Default, Debug)]
struct Stats {
    sets: u64,
    deletes: u64,
    expired: u64,
    gc_rewrites: u64,
    drop_prefixes: u64,
    drop_alls: u64,
    reopens: u64,
    max_level: usize,
}

fn run(s: &Scenario, dir: &Path, ops: u64) -> (DbOptions, Model, Stats) {
    let o = db_options(s);
    let opt = options(dir, &o);
    let mut r = Rng(0x5eed ^ s.name.len() as u64);
    let mut model = Model::new();
    let mut stats = Stats::default();
    let mut writes = 0u64;

    let db = Arc::new(Mutex::new(Some(DB::open(opt.clone()).unwrap())));
    let stop = AtomicBool::new(false);
    std::thread::scope(|scope| {
        // Readers hold the DB only for a few views at a time, so the writer
        // can swap it for a reopen.
        let readers: Vec<_> = (0..2u64)
            .map(|i| {
                let (db, stop) = (Arc::clone(&db), &stop);
                scope.spawn(move || {
                    let mut r = Rng(0x7ead + i);
                    let mut rounds = 0u64;
                    while !stop.load(Ordering::Relaxed) {
                        {
                            let guard = db.lock().unwrap();
                            if let Some(d) = guard.as_ref() {
                                for _ in 0..3 {
                                    read_round(d, &mut r).unwrap();
                                    rounds += 1;
                                }
                            }
                        }
                        std::thread::yield_now();
                    }
                    rounds
                })
            })
            .collect();

        let mut done = 0;
        while done < ops {
            let guard = db.lock().unwrap();
            let d = guard.as_ref().unwrap();
            // A batch of operations in one transaction.
            let n = 1 + r.below(30);
            let mut txn = d.new_transaction(true);
            let mut pending: Vec<(Vec<u8>, Option<Vec<u8>>)> = Vec::new();
            for _ in 0..n {
                let k = key(r.below(KEYS));
                let roll = r.below(100);
                let res = if roll < 70 {
                    writes += 1;
                    let len = if r.below(3) == 0 {
                        600 + r.below(3000) as usize
                    } else {
                        20 + r.below(200) as usize
                    };
                    let v = value(&k, writes, len);
                    pending.push((k.clone(), Some(v.clone())));
                    stats.sets += 1;
                    txn.set(k, v)
                } else if roll < 90 {
                    pending.push((k.clone(), None));
                    stats.deletes += 1;
                    txn.delete(k)
                } else {
                    // Already expired: reads see nothing. The value stays
                    // inline: GC does not keep expired values, so one in the
                    // value log could not be read by the all-versions dump
                    // (in Go either) once its file is collected.
                    writes += 1;
                    let mut e = Entry::new(k.clone(), value(&k, writes, 100));
                    e.expires_at = 1;
                    pending.push((k, None));
                    stats.expired += 1;
                    txn.set_entry(e)
                };
                if let Err(Error::TxnTooBig) = res {
                    pending.pop();
                    break;
                }
                res.unwrap();
            }
            txn.commit().unwrap();
            for (k, v) in pending {
                match v {
                    Some(v) => model.insert(k, v),
                    None => model.remove(&k),
                };
            }
            done += n;

            // Once, past 60%: drop everything and carry on.
            if stats.drop_alls == 0 && done > ops * 3 / 5 {
                d.drop_all().unwrap();
                model.clear();
                stats.drop_alls += 1;
            }
            match r.below(1000) {
                // Value log GC, as an application would run it.
                0..=19 => {
                    while d.run_value_log_gc(0.3).is_ok() {
                        stats.gc_rewrites += 1;
                    }
                }
                20..=21 => {
                    let p = format!("k{:02}", r.below(100));
                    d.drop_prefix(&[p.as_bytes()]).unwrap();
                    model.retain(|k, _| !k.starts_with(p.as_bytes()));
                    stats.drop_prefixes += 1;
                }
                23..=25 => {
                    // Reopen: everything acknowledged must survive.
                    drop(guard);
                    let mut g = db.lock().unwrap();
                    g.take().unwrap().close().unwrap();
                    *g = Some(DB::open(opt.clone()).unwrap());
                    stats.reopens += 1;
                    continue;
                }
                26..=35 => {
                    // Spot check against the model.
                    let k = key(r.below(KEYS));
                    let got = d.view(|txn| match txn.get(&k) {
                        Ok(item) => item.value_copy().map(Some),
                        Err(Error::KeyNotFound) => Ok(None),
                        Err(e) => Err(e),
                    });
                    assert_eq!(got.unwrap(), model.get(&k).cloned());
                }
                36 => assert_same_db(&latest(d), &model, "full scan", Some(d)),
                _ => {}
            }
            let max_level = d.tables().iter().map(|t| t.level).max().unwrap_or(0);
            stats.max_level = stats.max_level.max(max_level);
        }
        stop.store(true, Ordering::Relaxed);
        for r in readers {
            assert!(r.join().unwrap() > 0, "readers never ran");
        }
    });

    let d = db.lock().unwrap().take().unwrap();
    // Give GC a last go after compaction settles.
    while d.run_value_log_gc(0.3).is_ok() {
        stats.gc_rewrites += 1;
    }
    assert_same_db(&latest(&d), &model, "before close", Some(&d));
    // GC writes old versions back; compaction drops such a copy once a newer
    // version shadows it, but a stale copy deeper down may still point into
    // the collected file (in Go too). No read can see those versions, but an
    // all-versions dump reads every value: flatten so all versions of a key
    // meet and the stale ones go.
    d.flatten(2).unwrap();
    assert_same_db(&latest(&d), &model, "after flatten", Some(&d));
    d.close().unwrap();
    (o, model, stats)
}

#[test]
fn randomized_workload_with_compaction_and_gc() {
    let ops: u64 = std::env::var("BADGER_WORKLOAD_OPS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(20_000);
    let (_tmp, out) = match std::env::var_os("BADGER_COMPAT_OUT") {
        Some(out) => (None, PathBuf::from(out).join("db")),
        None => {
            let tmp = tempfile::tempdir().unwrap();
            let p = tmp.path().to_path_buf();
            (Some(tmp), p)
        }
    };
    let scenarios = [
        Scenario {
            name: "rust-workload",
            compression: "none",
            encryption_key: Vec::new(),
        },
        Scenario {
            name: "rust-workload-zstd-aes",
            compression: "zstd",
            encryption_key: (0..32).collect(),
        },
    ];
    for s in &scenarios {
        let dir = out.join(s.name);
        let _ = fs::remove_dir_all(&dir);
        let (o, model, stats) = run(s, &dir, ops);
        eprintln!("{}: {stats:?}", s.name);
        assert!(stats.max_level > 0, "{}: compaction never ran", s.name);
        assert_eq!(stats.drop_alls, 1);
        assert!(stats.gc_rewrites > 0, "{}: GC never rewrote a file", s.name);

        let db = DB::open(options(&dir, &o).with_read_only(true)).unwrap();
        assert_same(&latest(&db), &model, &format!("{}: after reopen", s.name));
        let entries = dump(&db);
        let max_version = db.max_version();
        db.close().unwrap();
        DbExpected {
            format_version: 1,
            name: s.name.into(),
            options: o,
            crash_snapshot: false,
            max_version,
            entries,
        }
        .write(&out.join(format!("{}.json", s.name)))
        .unwrap();
    }
}
