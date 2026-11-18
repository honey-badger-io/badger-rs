//! Ports of Go's value log GC tests (`value_test.go`), drop tests
//! (`managed_db_test.go`, `db2_test.go`) and banned namespaces
//! (`db_test.go`).

use std::time::Duration;

use super::*;
use crate::compaction::CompactionPriority;
use crate::structs::ValuePointer;
use crate::util::keys::key_with_ts;

fn gc_options(dir: &std::path::Path) -> Options {
    // Go's getTestOptions, with value log GC test sizes.
    Options::new(dir)
        .with_mem_table_size(1 << 15)
        .with_base_table_size(1 << 15)
        .with_base_level_size(4 << 15)
        .with_value_log_file_size(1 << 20)
        .with_value_threshold(1 << 10)
        .with_compression(crate::options::CompressionType::None)
}

/// A deterministic 32 KiB value, partly random as in Go.
fn big_value(i: u64) -> Vec<u8> {
    let mut v = vec![0u8; 32 << 10];
    let mut x = i.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1;
    let n = (x % v.len() as u64) as usize;
    for b in &mut v[..n] {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        *b = x as u8;
    }
    v
}

fn key(i: u64) -> Vec<u8> {
    format!("key{i}").into_bytes()
}

/// Writes key0..key99 with 32 KiB values, in transactions of up to 20.
fn write_100(db: &DB) {
    let mut txn = db.new_transaction(true);
    for i in 0..100 {
        txn.set(key(i), big_value(i)).unwrap();
        if i % 20 == 0 {
            txn.commit().unwrap();
            txn = db.new_transaction(true);
        }
    }
    txn.commit().unwrap();
}

fn delete(db: &DB, k: Vec<u8>) {
    db.update(|txn| txn.delete(k)).unwrap();
}

fn rewrite_first_file(db: &DB) {
    let fid = db.inner.vlog.sorted_fids()[0];
    crate::vlog::rewrite_for_test(&db.inner, fid).unwrap();
}

fn get(db: &DB, k: &[u8]) -> Result<Vec<u8>> {
    db.view(|txn| txn.get(k)?.value_copy())
}

#[test]
fn value_gc() {
    let dir = tempfile::tempdir().unwrap();
    let db = DB::open(gc_options(dir.path())).unwrap();
    write_100(&db);
    for i in 0..45 {
        delete(&db, key(i));
    }
    rewrite_first_file(&db);
    for i in 45..100 {
        assert_eq!(get(&db, &key(i)).unwrap(), big_value(i));
    }
    db.close().unwrap();
}

#[test]
fn value_gc2() {
    let dir = tempfile::tempdir().unwrap();
    let db = DB::open(gc_options(dir.path())).unwrap();
    write_100(&db);
    for i in 0..5 {
        delete(&db, key(i));
    }
    for i in 5..10 {
        db.update(|txn| txn.set(key(i), format!("value{i}")))
            .unwrap();
    }
    rewrite_first_file(&db);
    for i in 0..5 {
        assert!(matches!(get(&db, &key(i)), Err(Error::KeyNotFound)));
    }
    for i in 5..10 {
        assert_eq!(get(&db, &key(i)).unwrap(), format!("value{i}").into_bytes());
    }
    // Moved entries.
    for i in 10..100 {
        assert_eq!(get(&db, &key(i)).unwrap(), big_value(i));
    }
    // The file is gone, and the moved values point elsewhere.
    let first = 1;
    assert!(!db.inner.vlog.sorted_fids().contains(&first));
    let vp = db
        .view(|txn| {
            let item = txn.get(&key(50))?;
            Ok(ValuePointer::decode(&item.vptr).unwrap())
        })
        .unwrap();
    assert_ne!(vp.fid, first);
    db.close().unwrap();

    // Everything survives a reopen.
    let db = DB::open(gc_options(dir.path())).unwrap();
    for i in 10..100 {
        assert_eq!(get(&db, &key(i)).unwrap(), big_value(i));
    }
    db.close().unwrap();
}

/// An open iterator keeps the rewritten file until it is dropped (Go's
/// TestValueGC3).
#[test]
fn value_gc_open_iterator_defers_delete() {
    let dir = tempfile::tempdir().unwrap();
    let db = DB::open(gc_options(dir.path())).unwrap();
    write_100(&db);
    for i in 0..45 {
        delete(&db, key(i));
    }
    let txn = db.new_transaction(false);
    let mut it = txn.iter(IteratorOptions::default());
    it.rewind();
    assert_eq!(it.item().key(), b"key45");
    let fid = db.inner.vlog.sorted_fids()[0];
    let path = crate::vlog::vlog_file_path(dir.path(), fid);
    rewrite_first_file(&db);
    assert!(path.exists(), "deleted under an open iterator");
    // The iterator still reads the old values.
    let mut n = 0;
    while it.valid() {
        let k = it.item().key().to_vec();
        let i: u64 = std::str::from_utf8(&k[3..]).unwrap().parse().unwrap();
        assert_eq!(it.item().value_copy().unwrap(), big_value(i));
        n += 1;
        it.next();
    }
    assert_eq!(n, 55);
    drop(it);
    drop(txn);
    assert!(!path.exists(), "deleted once the iterator is gone");
    db.close().unwrap();
}

/// Compaction fills the discard stats, and `run_value_log_gc` uses them.
#[test]
fn run_value_log_gc_after_compaction() {
    let dir = tempfile::tempdir().unwrap();
    let opt = gc_options(dir.path());
    let db = DB::open(opt.clone()).unwrap();
    assert!(matches!(db.run_value_log_gc(0.5), Err(Error::NoRewrite)));
    assert!(matches!(
        db.run_value_log_gc(1.0),
        Err(Error::InvalidRequest)
    ));
    write_100(&db);
    for i in 0..100 {
        if i % 10 != 0 {
            delete(&db, key(i));
        }
    }
    db.close().unwrap();
    // Compact L0 down: the dropped values are counted.
    let db = DB::open(opt.clone()).unwrap();
    while db.tables().iter().any(|t| t.level == 0) {
        db.inner
            .lc
            .do_compact(&db.inner, 0, CompactionPriority::new(0, 1.0))
            .unwrap();
    }
    let mut layout = db.tables();
    layout.sort_by_key(|t| t.level);
    let fids = db.inner.vlog.sorted_fids();
    assert!(fids.len() >= 3, "{fids:?}");
    assert!(
        fids.iter().any(|&f| db.inner.vlog.discard_of(f) > 0),
        "no discard stats after compaction: {layout:?}"
    );
    let mut rewritten = 0;
    while db.run_value_log_gc(0.5).is_ok() {
        rewritten += 1;
    }
    assert!(rewritten > 0);
    assert!(db.inner.vlog.sorted_fids().len() < fids.len());
    for i in 0..100 {
        let got = get(&db, &key(i));
        if i % 10 == 0 {
            assert_eq!(got.unwrap(), big_value(i));
        } else {
            assert!(matches!(got, Err(Error::KeyNotFound)));
        }
    }
    db.close().unwrap();

    let db = DB::open(opt).unwrap();
    for i in (0..100).step_by(10) {
        assert_eq!(get(&db, &key(i)).unwrap(), big_value(i));
    }
    db.close().unwrap();
}

#[test]
fn gc_rejected_in_memory_and_read_only() {
    let db = DB::open(Options::new("").with_in_memory(true)).unwrap();
    assert!(matches!(
        db.run_value_log_gc(0.5),
        Err(Error::GcInMemoryMode)
    ));
    db.close().unwrap();
    let dir = tempfile::tempdir().unwrap();
    DB::open(gc_options(dir.path())).unwrap().close().unwrap();
    let db = DB::open(gc_options(dir.path()).with_read_only(true)).unwrap();
    assert!(matches!(
        db.run_value_log_gc(0.5),
        Err(Error::GcInReadOnlyMode)
    ));
    db.close().unwrap();
}

/// The discard stats survive a reopen (Go's TestReloadDiscardStats).
#[test]
fn reload_discard_stats() {
    let dir = tempfile::tempdir().unwrap();
    let db = DB::open(gc_options(dir.path())).unwrap();
    let mut stats = std::collections::HashMap::new();
    stats.insert(1, 1);
    stats.insert(2, 1);
    db.inner.vlog.update_discard_stats(&stats);
    stats.clear();
    stats.insert(1, -1);
    db.inner.vlog.update_discard_stats(&stats);
    db.close().unwrap();
    let db = DB::open(gc_options(dir.path())).unwrap();
    assert_eq!(db.inner.vlog.discard_of(1), 0);
    assert_eq!(db.inner.vlog.discard_of(2), 1);
    db.close().unwrap();
}

fn num_keys(db: &DB) -> usize {
    db.view(|txn| {
        let mut it = txn.iter(IteratorOptions::default());
        let mut n = 0;
        it.rewind();
        while it.valid() {
            n += 1;
            it.next();
        }
        Ok(n)
    })
    .unwrap()
}

fn drop_options(dir: &std::path::Path) -> Options {
    gc_options(dir)
        .with_value_threshold(32)
        .with_value_log_file_size(5 << 20)
}

fn populate(db: &DB, n: u64) {
    let mut txn = db.new_transaction(true);
    for i in 0..n {
        // Go's key(): four digits, so "key000" matches ten keys.
        let k = format!("key{i:04}");
        let v = format!("value-{i:0>40}");
        if let Err(Error::TxnTooBig) = txn.set(k.clone(), v.clone()) {
            txn.commit().unwrap();
            txn = db.new_transaction(true);
            txn.set(k, v).unwrap();
        }
    }
    txn.commit().unwrap();
}

#[test]
fn drop_all() {
    let dir = tempfile::tempdir().unwrap();
    let opt = drop_options(dir.path());
    let db = DB::open(opt.clone()).unwrap();
    populate(&db, 10_000);
    assert_eq!(num_keys(&db), 10_000);
    db.drop_all().unwrap();
    assert_eq!(num_keys(&db), 0);
    // Writes work afterwards, twice over.
    populate(&db, 10_000);
    assert_eq!(num_keys(&db), 10_000);
    db.drop_all().unwrap();
    db.drop_all().unwrap();
    assert_eq!(num_keys(&db), 0);
    populate(&db, 100);
    db.close().unwrap();
    // Nothing comes back on replay.
    let db = DB::open(opt).unwrap();
    assert_eq!(num_keys(&db), 100);
    db.close().unwrap();
}

#[test]
fn drop_all_managed() {
    let dir = tempfile::tempdir().unwrap();
    let opt = drop_options(dir.path());
    let db = DB::open_managed(opt.clone()).unwrap();
    let write = |db: &DB, ts: u64| {
        for chunk in 0..50 {
            let mut txn = db.new_transaction_at(ts, true);
            for i in chunk * 20..(chunk + 1) * 20 {
                txn.set(format!("key{i:05}"), b"v".to_vec()).unwrap();
            }
            txn.commit_at(ts).unwrap();
        }
    };
    write(&db, 10);
    assert_eq!(num_keys(&db), 1000);
    db.drop_all().unwrap();
    assert_eq!(num_keys(&db), 0);
    // Lower timestamps work after a drop.
    write(&db, 5);
    assert_eq!(num_keys(&db), 1000);
    db.close().unwrap();
    let db = DB::open_managed(opt).unwrap();
    assert_eq!(num_keys(&db), 1000);
    db.close().unwrap();
}

#[test]
#[should_panic(expected = "read-only")]
fn drop_read_only() {
    let dir = tempfile::tempdir().unwrap();
    let opt = drop_options(dir.path());
    let db = DB::open(opt.clone()).unwrap();
    populate(&db, 100);
    db.close().unwrap();
    let db = DB::open(opt.with_read_only(true)).unwrap();
    let _ = db.drop_all();
}

/// Writes racing with drops are applied or rejected, never lost silently
/// (Go's TestDropAllRace / TestDropPrepareDrainDoesNotDropWrites).
#[test]
fn drop_all_with_concurrent_writes() {
    let dir = tempfile::tempdir().unwrap();
    let db = DB::open(drop_options(dir.path())).unwrap();
    let stop = std::sync::atomic::AtomicBool::new(false);
    std::thread::scope(|s| {
        let writer = s.spawn(|| {
            let mut ok = 0u64;
            let mut i = 0u64;
            while !stop.load(Ordering::SeqCst) {
                match db.update(|txn| txn.set(format!("w{i:08}"), b"v".to_vec())) {
                    Ok(()) => ok += 1,
                    Err(e) => assert!(matches!(e.root(), Error::BlockedWrites), "{e}"),
                }
                i += 1;
            }
            ok
        });
        for _ in 0..5 {
            std::thread::sleep(Duration::from_millis(20));
            db.drop_all().unwrap();
        }
        stop.store(true, Ordering::SeqCst);
        assert!(writer.join().unwrap() > 0);
    });
    // Writes after the last drop are all there.
    let before = num_keys(&db);
    db.update(|txn| txn.set(b"after".to_vec(), b"v".to_vec()))
        .unwrap();
    assert_eq!(num_keys(&db), before + 1);
    db.close().unwrap();
}

#[test]
fn drop_prefix() {
    let dir = tempfile::tempdir().unwrap();
    let opt = drop_options(dir.path());
    let db = DB::open(opt.clone()).unwrap();
    let n = 10_000;
    populate(&db, n);
    assert_eq!(num_keys(&db), n as usize);
    db.drop_prefix(&[b"key000"]).unwrap();
    assert_eq!(num_keys(&db), n as usize - 10);
    db.drop_prefix(&[b"key00"]).unwrap();
    assert_eq!(num_keys(&db), n as usize - 100);
    let mut expected = n as usize;
    for i in 0..10 {
        let p = format!("key{i}");
        db.drop_prefix(&[p.as_bytes()]).unwrap();
        expected -= 1000;
        assert_eq!(num_keys(&db), expected);
    }
    db.drop_prefix(&[b"key1"]).unwrap();
    assert_eq!(num_keys(&db), 0);
    db.drop_prefix(&[b"key"]).unwrap();
    assert_eq!(num_keys(&db), 0);

    populate(&db, n);
    assert_eq!(num_keys(&db), n as usize);
    db.drop_prefix(&[b"key"]).unwrap();
    assert_eq!(num_keys(&db), 0);
    db.close().unwrap();

    let db = DB::open(opt).unwrap();
    assert_eq!(num_keys(&db), 0);
    db.close().unwrap();
}

/// Dropping a prefix keeps the other keys, wherever they are in the tree.
#[test]
fn drop_prefix_keeps_others_across_levels() {
    let dir = tempfile::tempdir().unwrap();
    let opt = drop_options(dir.path()).with_num_compactors(2);
    let db = DB::open(opt.clone()).unwrap();
    for round in 0..3 {
        let mut txn = db.new_transaction(true);
        for i in 0..2000 {
            let p = if i % 2 == 0 { "a" } else { "b" };
            let k = format!("{p}{i:05}");
            let v = format!("{round}-{i:0>60}");
            if let Err(Error::TxnTooBig) = txn.set(k.clone(), v.clone()) {
                txn.commit().unwrap();
                txn = db.new_transaction(true);
                txn.set(k, v).unwrap();
            }
        }
        txn.commit().unwrap();
        if round == 1 {
            db.flatten(1).unwrap();
        }
    }
    db.drop_prefix(&[b"a"]).unwrap();
    assert_eq!(num_keys(&db), 1000);
    db.view(|txn| {
        let item = txn.get(b"b00001")?;
        assert_eq!(item.value_copy()?, format!("2-{:0>60}", 1).into_bytes());
        assert!(matches!(txn.get(b"a00000"), Err(Error::KeyNotFound)));
        Ok(())
    })
    .unwrap();
    db.close().unwrap();
    let db = DB::open(opt).unwrap();
    assert_eq!(num_keys(&db), 1000);
    db.close().unwrap();
}

#[test]
fn drop_prefix_with_no_data() {
    let dir = tempfile::tempdir().unwrap();
    let db = DB::open(drop_options(dir.path())).unwrap();
    populate(&db, 100);
    db.drop_prefix(&[b"nothing"]).unwrap();
    assert_eq!(num_keys(&db), 100);
    db.drop_prefix(&[]).unwrap();
    db.close().unwrap();
}

#[test]
fn drop_all_drop_prefix() {
    let dir = tempfile::tempdir().unwrap();
    let db = DB::open(drop_options(dir.path())).unwrap();
    populate(&db, 1000);
    std::thread::scope(|s| {
        s.spawn(|| db.drop_all());
        s.spawn(|| db.drop_prefix(&[b"key"]));
    });
    // Either order leaves nothing (one of them may have been rejected).
    let n = num_keys(&db);
    assert!(n == 0 || n == 1000, "{n}");
    db.drop_all().unwrap();
    assert_eq!(num_keys(&db), 0);
    db.close().unwrap();
}

/// Go's TestBannedPrefixes.
#[test]
fn banned_namespaces() {
    let dir = tempfile::tempdir().unwrap();
    let opt = drop_options(dir.path()).with_namespace_offset(Some(0));
    let db = DB::open(opt.clone()).unwrap();
    let ns_key = |ns: u64, suffix: &str| {
        let mut k = ns.to_be_bytes().to_vec();
        k.extend_from_slice(suffix.as_bytes());
        k
    };
    for ns in 0..5u64 {
        db.update(|txn| txn.set(ns_key(ns, "-key"), b"v".to_vec()))
            .unwrap();
    }
    db.ban_namespace(1).unwrap();
    db.ban_namespace(3).unwrap();
    assert_eq!(db.banned_namespaces(), vec![1, 3]);
    let check = |db: &DB| {
        for ns in 0..5u64 {
            let got = db.view(|txn| txn.get(&ns_key(ns, "-key")).map(|_| ()));
            let set = db.update(|txn| txn.set(ns_key(ns, "-other"), b"v".to_vec()));
            if ns == 1 || ns == 3 {
                assert!(matches!(got, Err(Error::BannedKey)), "{ns}: {got:?}");
                assert!(matches!(set, Err(Error::BannedKey)), "{ns}: {set:?}");
            } else {
                got.unwrap();
                set.unwrap();
            }
        }
    };
    check(&db);
    db.close().unwrap();
    // The bans are stored.
    let db = DB::open(opt).unwrap();
    assert_eq!(db.banned_namespaces(), vec![1, 3]);
    check(&db);
    db.close().unwrap();

    let dir = tempfile::tempdir().unwrap();
    let db = DB::open(drop_options(dir.path())).unwrap();
    assert!(matches!(db.ban_namespace(1), Err(Error::NamespaceMode)));
    db.close().unwrap();
}

/// Go's TestCompactL0OnClose: closing compacts (the oldest overlapping run
/// of) L0 into the levels below.
#[test]
fn compact_l0_on_close() {
    let dir = tempfile::tempdir().unwrap();
    let opt = drop_options(dir.path())
        .with_compact_l0_on_close(true)
        .with_num_compactors(0);
    let db = DB::open(opt.clone()).unwrap();
    populate(&db, 2000);
    db.close().unwrap();
    let db = DB::open(opt.with_read_only(true)).unwrap();
    let tables = db.tables();
    assert!(tables.iter().any(|t| t.level > 0), "{tables:?}");
    assert_eq!(num_keys(&db), 2000);
    db.close().unwrap();
}

/// Flatten leaves one level.
#[test]
fn flatten() {
    let dir = tempfile::tempdir().unwrap();
    let opt = drop_options(dir.path());
    let db = DB::open(opt.clone()).unwrap();
    for round in 0..4 {
        populate(&db, 2000);
        if round % 2 == 0 {
            db.flatten(2).unwrap();
        }
    }
    // Push the latest memtables into L0 first.
    db.close().unwrap();
    let db = DB::open(opt).unwrap();
    db.flatten(2).unwrap();
    let levels: std::collections::BTreeSet<usize> = db.tables().iter().map(|t| t.level).collect();
    assert_eq!(levels.len(), 1, "{levels:?}");
    assert_eq!(num_keys(&db), 2000);
    db.close().unwrap();
}

/// Entry keys written by GC keep their versions.
#[test]
fn gc_keeps_versions() {
    let dir = tempfile::tempdir().unwrap();
    let db = DB::open(gc_options(dir.path())).unwrap();
    write_100(&db);
    let before = db.view(|txn| Ok(txn.get(&key(60))?.version())).unwrap();
    for i in 0..45 {
        delete(&db, key(i));
    }
    rewrite_first_file(&db);
    let after = db.view(|txn| Ok(txn.get(&key(60))?.version())).unwrap();
    assert_eq!(before, after);
    // The value is in the LSM tree twice at that version; reads see one.
    let txn = db.new_transaction(false);
    let item = db
        .inner
        .get(&key_with_ts(&key(60), before))
        .unwrap()
        .unwrap();
    assert_eq!(item.version, before);
    drop(txn);
    db.close().unwrap();
}

/// GC writes back an old version shadowed by a delete; once compaction
/// drops the delete (and the original old version) in the last level, the
/// written-back copy must not resurface. Found by `tests/workload.rs`.
#[test]
fn gc_write_back_does_not_resurrect_deleted_key() {
    use crate::compaction::CompactDef;
    let dir = tempfile::tempdir().unwrap();
    let opt = gc_options(dir.path()).with_num_compactors(0);
    let db = DB::open_managed(opt.clone()).unwrap();
    let commit = |ts: u64, f: &dyn Fn(&mut Txn<'_>)| {
        let mut txn = db.new_transaction_at(ts, true);
        f(&mut txn);
        txn.commit_at(ts).unwrap();
    };
    commit(1, &|t| t.set(b"k".to_vec(), big_value(1)).unwrap());
    commit(2, &|t| t.delete(b"k".to_vec()).unwrap());
    // Fill the first value log file so it is not the one being written.
    for i in 0..40u64 {
        commit(3 + i, &|t| t.set(key(i), big_value(i)).unwrap());
    }
    db.close().unwrap();

    let db = DB::open_managed(opt).unwrap();
    let lc = &db.inner.lc;
    // Discard ts 0: compaction keeps both versions of k.
    while db.tables().iter().any(|t| t.level == 0) {
        lc.do_compact(&db.inner, 0, CompactionPriority::new(0, 1.0))
            .unwrap();
    }
    let fid = db.inner.vlog.sorted_fids()[0];
    crate::vlog::rewrite_for_test(&db.inner, fid).unwrap();

    // Now the delete and the old version can go from the last level.
    db.set_discard_ts(100);
    let last = lc.levels.len() - 1;
    let last_tables = lc.levels[last].read().tables.clone();
    for t in last_tables {
        let mut cd = CompactDef {
            compactor_id: -1,
            t: lc.level_targets(&db.inner.opt),
            this_level: last,
            next_level: last,
            top: vec![t],
            ..Default::default()
        };
        lc.run_compact_def(&db.inner, -1, last, &mut cd).unwrap();
    }
    let got = db.view(|txn| txn.get(b"k").map(|_| ()));
    assert!(matches!(got, Err(Error::KeyNotFound)), "{got:?}");
    db.close().unwrap();
}
