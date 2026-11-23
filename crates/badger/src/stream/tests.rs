//! Port of Go's `stream_test.go`, plus tests of ranges, versions, errors
//! and non-managed streams.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Mutex;

use crate::{DB, Entry, Error, Options, pb};

fn key_with_prefix(prefix: &str, k: usize) -> Vec<u8> {
    format!("{prefix}-{k}").into_bytes()
}

fn key_to_int(k: &[u8]) -> (String, usize) {
    let s = std::str::from_utf8(k).unwrap();
    let (p, n) = s.split_once('-').unwrap();
    (p.to_string(), n.parse().unwrap())
}

fn value(k: usize) -> Vec<u8> {
    format!("{k:08}").into_bytes()
}

/// Go's collector: keeps every KV except done markers.
fn collect(stream: &crate::Stream<'_>) -> Vec<pb::Kv> {
    let mut out = Vec::new();
    stream
        .orchestrate(|list| {
            out.extend(list.kv.into_iter().filter(|kv| !kv.stream_done));
            Ok(())
        })
        .unwrap();
    out
}

/// Counts KVs per prefix, checking each value.
fn count_by_prefix(kvs: &[pb::Kv]) -> HashMap<String, usize> {
    let mut m = HashMap::new();
    for kv in kvs {
        let (prefix, k) = key_to_int(&kv.key);
        assert_eq!(kv.value, value(k));
        *m.entry(prefix).or_insert(0) += 1;
    }
    m
}

fn open_managed(dir: &Path) -> DB {
    DB::open_managed(Options::new(dir)).unwrap()
}

/// Three prefixes of 100 keys each, committed at version 5.
fn fill_managed(db: &DB) {
    for prefix in ["p0", "p1", "p2"] {
        let mut txn = db.new_transaction_at(u64::MAX, true);
        for i in 1..=100 {
            txn.set_entry(Entry::new(key_with_prefix(prefix, i), value(i)))
                .unwrap();
        }
        txn.commit_at(5).unwrap();
    }
}

#[test]
fn stream() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_managed(dir.path());
    fill_managed(&db);

    let mut stream = db.new_stream_at(u64::MAX);
    stream.log_prefix = "Testing".into();

    // 1. Everything.
    let m = count_by_prefix(&collect(&stream));
    assert_eq!(m.len(), 3);
    assert!(m.values().all(|&c| c == 100), "{m:?}");

    // 2. One prefix.
    stream.prefix = b"p1".to_vec();
    let m = count_by_prefix(&collect(&stream));
    assert_eq!(m, HashMap::from([("p1".to_string(), 100)]));

    // 3. Chosen keys within the prefix.
    stream.choose_key = Some(Box::new(|item| key_to_int(item.key()).1.is_multiple_of(2)));
    let m = count_by_prefix(&collect(&stream));
    assert_eq!(m, HashMap::from([("p1".to_string(), 50)]));

    // 4. Chosen keys of every prefix.
    stream.prefix.clear();
    let m = count_by_prefix(&collect(&stream));
    assert_eq!(m.len(), 3);
    assert!(m.values().all(|&c| c == 50), "{m:?}");
    drop(stream);
    db.close().unwrap();
}

#[test]
fn stream_with_thread_id() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_managed(dir.path());
    fill_managed(&db);
    let mut stream = db.new_stream_at(u64::MAX);
    let num_go = stream.num_go;
    let db_inner = &*db.inner;
    stream.key_to_list = Some(Box::new(move |key, it, thread_id| {
        assert!(thread_id < num_go);
        super::to_list(db_inner, key, it)
    }));
    let m = count_by_prefix(&collect(&stream));
    assert_eq!(m.len(), 3);
    assert!(m.values().all(|&c| c == 100), "{m:?}");
    drop(stream);
    db.close().unwrap();
}

#[test]
fn stream_custom_key_to_list() {
    let dir = tempfile::tempdir().unwrap();
    let db = open_managed(dir.path());
    for key in ["p0", "p1", "p2"] {
        for i in 1..=100 {
            let mut txn = db.new_transaction_at(u64::MAX, true);
            txn.set(key, value(i)).unwrap();
            txn.commit_at(i as u64).unwrap();
        }
    }
    let mut stream = db.new_stream_at(u64::MAX);
    // Only the newest version, without advancing the iterator.
    stream.key_to_list = Some(Box::new(|_key, it, _| {
        let item = it.item();
        Ok(pb::KvList {
            kv: vec![pb::Kv {
                key: item.key().to_vec(),
                value: item.value_copy()?,
                ..Default::default()
            }],
            ..Default::default()
        })
    }));
    let mut keys: Vec<Vec<u8>> = collect(&stream).into_iter().map(|kv| kv.key).collect();
    keys.sort();
    assert_eq!(keys, [b"p0".to_vec(), b"p1".to_vec(), b"p2".to_vec()]);
    drop(stream);
    db.close().unwrap();
}

#[test]
fn big_stream() {
    // Go's TestBigStream and TestStreamMaxSize (manual there, with 3M keys),
    // smaller: enough for several tables and value log files, and a small
    // max_size so batches are split.
    let dir = tempfile::tempdir().unwrap();
    let opt = Options::new(dir.path())
        .with_mem_table_size(1 << 20)
        .with_base_table_size(1 << 18)
        .with_value_threshold(8);
    let db = DB::open_managed(opt.clone()).unwrap();
    let n = 30_000;
    let mut wb = db.new_write_batch_at(5);
    for prefix in ["p0", "p1", "p2"] {
        for i in 1..=n {
            wb.set(key_with_prefix(prefix, i), value(i)).unwrap();
        }
    }
    wb.flush().unwrap();
    // Reopening flushes every memtable, so the data is in tables.
    db.close().unwrap();
    let db = DB::open_managed(opt).unwrap();
    assert!(db.tables().len() > 3, "{} tables", db.tables().len());

    let mut stream = db.new_stream_at(u64::MAX);
    assert_eq!(stream.max_size, super::MAX_STREAM_SIZE);
    stream.max_size = 64 << 10;
    let mut calls = 0;
    let mut kvs = Vec::new();
    stream
        .orchestrate(|list| {
            calls += 1;
            kvs.extend(list.kv);
            Ok(())
        })
        .unwrap();
    assert_eq!(kvs.len(), 3 * n);
    let m = count_by_prefix(&kvs);
    assert!(m.values().all(|&c| c == n), "{m:?}");
    // Every key exactly once.
    let mut keys: Vec<_> = kvs.iter().map(|kv| kv.key.clone()).collect();
    keys.sort();
    keys.dedup();
    assert_eq!(keys.len(), 3 * n);
    assert!(calls > 1);
    drop(stream);
    db.close().unwrap();
}

#[test]
fn ranges_cover_every_key_once() {
    let dir = tempfile::tempdir().unwrap();
    let opt = Options::new(dir.path())
        .with_mem_table_size(1 << 16)
        .with_base_table_size(1 << 14)
        .with_value_threshold(1 << 10)
        .with_num_compactors(0);
    let db = DB::open(opt).unwrap();
    for chunk in 0..20 {
        let mut wb = db.new_write_batch();
        for i in 0..1000 {
            wb.set(format!("k{:06}", chunk * 1000 + i), "v").unwrap();
        }
        wb.flush().unwrap();
    }
    let ranges = super::ranges(&db.inner, b"", 8);
    assert!(ranges.len() > 1, "{} ranges", ranges.len());
    // Sorted by key, the ranges tile the key space.
    let mut sorted = ranges.clone();
    sorted.sort_by(|a, b| a.left.cmp(&b.left));
    assert!(sorted[0].left.is_empty());
    assert!(sorted.last().unwrap().right.is_empty());
    for w in sorted.windows(2) {
        assert_eq!(w[0].right, w[1].left);
    }
    let kvs = collect(&db.new_stream());
    assert_eq!(kvs.len(), 20_000);
    db.close().unwrap();
}

#[test]
fn stream_versions_and_since_ts() {
    // The default to_list sends every live version, newest first, down to
    // a delete; since_ts skips older versions.
    let dir = tempfile::tempdir().unwrap();
    let db = DB::open_managed(Options::new(dir.path()).with_num_versions_to_keep(10)).unwrap();
    let write = |ts: u64, f: &dyn Fn(&mut crate::Txn<'_>)| {
        let mut txn = db.new_transaction_at(ts, true);
        f(&mut txn);
        txn.commit_at(ts).unwrap();
    };
    write(1, &|t| t.set("a", "a1").unwrap());
    write(2, &|t| t.set("a", "a2").unwrap());
    write(3, &|t| t.delete("b").unwrap());
    write(4, &|t| t.set("b", "b4").unwrap());
    write(5, &|t| t.set("a", "a5").unwrap());

    let versions = |since_ts: u64| -> Vec<(String, u64)> {
        let mut stream = db.new_stream_at(u64::MAX);
        stream.since_ts = since_ts;
        let mut out: Vec<_> = collect(&stream)
            .into_iter()
            .map(|kv| (String::from_utf8(kv.key).unwrap(), kv.version))
            .collect();
        out.sort();
        out
    };
    assert_eq!(
        versions(0),
        [
            ("a".into(), 1),
            ("a".into(), 2),
            ("a".into(), 5),
            ("b".into(), 4)
        ]
    );
    assert_eq!(versions(2), [("a".into(), 5), ("b".into(), 4)]);
    db.close().unwrap();
}

#[test]
fn stream_non_managed_and_done_markers() {
    let dir = tempfile::tempdir().unwrap();
    let db = DB::open(Options::new(dir.path())).unwrap();
    db.update(|txn| {
        for i in 0..50 {
            txn.set(key_with_prefix("x", i), value(i))?;
        }
        txn.set_entry(Entry::new("x-50", value(50)).with_meta(9))
    })
    .unwrap();
    let mut stream = db.new_stream();
    stream.send_done_markers(true);
    let mut kvs = Vec::new();
    stream
        .orchestrate(|list| {
            kvs.extend(list.kv);
            Ok(())
        })
        .unwrap();
    let (done, data): (Vec<_>, Vec<_>) = kvs.into_iter().partition(|kv| kv.stream_done);
    assert_eq!(data.len(), 51);
    assert!(!done.is_empty());
    // One done marker per stream id, after that stream's data.
    let mut ids: Vec<u32> = done.iter().map(|kv| kv.stream_id).collect();
    ids.sort();
    ids.dedup();
    assert_eq!(ids.len(), done.len());
    let x50 = data.iter().find(|kv| kv.key == b"x-50").unwrap();
    assert_eq!(x50.user_meta, [9]);
    assert_eq!(x50.version, 1);
    drop(stream);
    db.close().unwrap();
}

#[test]
fn send_error_stops_the_stream() {
    let dir = tempfile::tempdir().unwrap();
    let db = DB::open(Options::new(dir.path())).unwrap();
    let mut wb = db.new_write_batch();
    for i in 0..20_000 {
        wb.set(key_with_prefix("k", i), vec![0u8; 100]).unwrap();
    }
    wb.flush().unwrap();
    let mut stream = db.new_stream();
    stream.max_size = 1;
    let calls = Mutex::new(0);
    let err = stream
        .orchestrate(|_| {
            *calls.lock().unwrap() += 1;
            Err(Error::InvalidRequest)
        })
        .unwrap_err();
    assert!(matches!(err, Error::InvalidRequest), "{err}");
    assert_eq!(*calls.lock().unwrap(), 1);
    drop(stream);
    db.close().unwrap();
}
