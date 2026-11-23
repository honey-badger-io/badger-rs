//! Port of Go's `stream_writer_test.go`.

use std::path::Path;

use crate::iterator::IteratorOptions;
use crate::{DB, Entry, Error, Options, pb};

fn kv(key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>, version: u64) -> pb::Kv {
    pb::Kv {
        key: key.into(),
        value: value.into(),
        version,
        ..Default::default()
    }
}

fn list(kvs: Vec<pb::Kv>) -> pb::KvList {
    pb::KvList {
        kv: kvs,
        ..Default::default()
    }
}

/// Go's getSortedKVList: big-endian u64 keys `0..n`, all at version 20.
fn sorted_kv_list(value_size: usize, n: u64) -> pb::KvList {
    let value = vec![7u8; value_size];
    list(
        (0..n)
            .map(|i| kv(i.to_be_bytes(), value.clone(), 20))
            .collect(),
    )
}

#[derive(Clone, Copy, Debug)]
enum Mode {
    Normal,
    Managed,
    InMemory,
}

/// Go's getTestOptions in each mode the tests run in.
fn open(mode: Mode, dir: &Path) -> DB {
    let opt = |d: &Path| Options::new(d).with_sync_writes(false);
    match mode {
        Mode::Normal => DB::open(opt(dir)).unwrap(),
        Mode::Managed => DB::open_managed(opt(dir)).unwrap(),
        Mode::InMemory => DB::open(opt(Path::new("")).with_in_memory(true)).unwrap(),
    }
}

fn each_mode(f: impl Fn(Mode, &DB)) {
    for mode in [Mode::Normal, Mode::Managed, Mode::InMemory] {
        let dir = tempfile::tempdir().unwrap();
        let db = open(mode, dir.path());
        f(mode, &db);
        db.close().unwrap();
    }
}

fn count_keys(db: &DB) -> usize {
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

/// Writes one key in a transaction of `mode` (managed: at `commit_ts`).
fn write_txn(db: &DB, mode: Mode, commit_ts: u64, f: impl FnOnce(&mut crate::Txn<'_>)) {
    match mode {
        Mode::Managed => {
            let mut txn = db.new_transaction_at(u64::MAX, true);
            f(&mut txn);
            txn.commit_at(commit_ts).unwrap();
        }
        _ => {
            let mut txn = db.new_transaction(true);
            f(&mut txn);
            txn.commit().unwrap();
        }
    }
}

fn stream_write(db: &DB, l: &pb::KvList) {
    let mut sw = db.new_stream_writer();
    sw.prepare().unwrap();
    sw.write(l).unwrap();
    sw.flush().unwrap();
}

#[test]
fn stream_writer1() {
    each_mode(|mode, db| {
        stream_write(db, &sorted_kv_list(128, 10));
        db.view(|txn| txn.get(&7u64.to_be_bytes()).map(|_| ()))
            .unwrap_or_else(|e| panic!("{mode:?}: {e}"));
        assert_eq!(count_keys(db), 10, "{mode:?}");
    });
}

#[test]
fn stream_writer2() {
    // Delete every key written by the stream writer.
    each_mode(|mode, db| {
        let n = 1000;
        stream_write(db, &sorted_kv_list(128, n));
        for i in 0..n {
            write_txn(db, mode, 20, |txn| txn.delete(i.to_be_bytes()).unwrap());
        }
        assert_eq!(count_keys(db), 0, "{mode:?}");
    });
}

#[test]
fn stream_writer3() {
    // Even keys from the stream writer, odd keys from transactions.
    each_mode(|mode, db| {
        let n = 1000u64;
        stream_write(
            db,
            &list(
                (0..n)
                    .map(|i| kv((2 * i).to_be_bytes(), vec![1u8; 128], 20))
                    .collect(),
            ),
        );
        for i in 0..n {
            write_txn(db, mode, 20, |txn| {
                txn.set_entry(Entry::new((2 * i + 1).to_be_bytes(), vec![2u8; 128]))
                    .unwrap()
            });
        }
        let keys: Vec<u64> = db
            .view(|txn| {
                let mut it = txn.iter(IteratorOptions::default());
                it.rewind();
                let mut out = Vec::new();
                while it.valid() {
                    out.push(u64::from_be_bytes(it.item().key().try_into().unwrap()));
                    it.next();
                }
                Ok(out)
            })
            .unwrap();
        assert_eq!(keys, (0..2 * n).collect::<Vec<_>>(), "{mode:?}");
    });
}

#[test]
fn stream_writer4() {
    // Prepare drops what was written before.
    let dir = tempfile::tempdir().unwrap();
    let db = open(Mode::Normal, dir.path());
    for i in 0..10 {
        db.update(|txn| txn.set(format!("key-{i}"), format!("val-{i}")))
            .unwrap();
    }
    stream_write(&db, &list(vec![kv("key-1", "value-1", 1)]));
    assert_eq!(count_keys(&db), 1);
    db.close().unwrap();
}

#[test]
fn stream_writer5() {
    // Keys with 0x00 and 0xff first bytes; the DB reopens.
    let dir = tempfile::tempdir().unwrap();
    let db = open(Mode::Normal, dir.path());
    let left = [b"\x00".as_slice(), b"break"].concat();
    let right = [b"\xff".as_slice(), b"break"].concat();
    stream_write(&db, &list(vec![kv(left, "val", 1), kv(right, "val", 1)]));
    db.close().unwrap();
    let db = open(Mode::Normal, dir.path());
    assert_eq!(count_keys(&db), 2);
    db.close().unwrap();
}

#[test]
fn stream_writer6() {
    // Keys as big as a table: each key's versions stay in one table.
    let dir = tempfile::tempdir().unwrap();
    let opt = Options::new(dir.path())
        .with_sync_writes(false)
        .with_base_table_size(1 << 15);
    let db = DB::open(opt.clone()).unwrap();
    let key_count = 40u64;
    let mut kvs = Vec::new();
    for s in ["a", "b", "c"] {
        for j in 0..key_count {
            kvs.push(kv(s.repeat(1 << 15), "val", key_count - j));
        }
    }
    stream_write(&db, &list(kvs));
    let tables = db.tables();
    assert_eq!(tables.len(), 3);
    for t in &tables {
        assert_eq!(t.key_count as u64, key_count, "level {}", t.level);
    }
    db.close().unwrap();
    DB::open(opt).unwrap().close().unwrap();
}

#[test]
fn stream_writer_cancel() {
    let dir = tempfile::tempdir().unwrap();
    let opt = Options::new(dir.path())
        .with_sync_writes(false)
        .with_base_table_size(1 << 15);
    let db = DB::open(opt).unwrap();
    let mut kvs = Vec::new();
    let mut ver = 1;
    for s in ["a", "a", "b", "b", "c", "c"] {
        kvs.push(kv(s.repeat(1 << 15), "val", ver));
        ver = (ver + 1) % 2;
    }
    let mut sw = db.new_stream_writer();
    sw.prepare().unwrap();
    // Versions of a key must be newest first: "a"@1 then "a"@0 is fine.
    sw.write(&list(kvs)).unwrap();
    drop(sw); // Go's Cancel.
    let mut sw1 = db.new_stream_writer();
    sw1.prepare().unwrap();
    sw1.flush().unwrap();
    // Writes work again.
    db.update(|txn| txn.set("x", "y")).unwrap();
    db.close().unwrap();
}

#[test]
fn stream_done() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(Mode::Normal, dir.path());
    let mut sw = db.new_stream_writer();
    sw.prepare().unwrap();
    for i in 0..10u32 {
        let mut data = kv(i.to_string(), [3u8; 10], 1);
        data.stream_id = i;
        let done = pb::Kv {
            stream_id: i,
            stream_done: true,
            ..Default::default()
        };
        sw.write(&list(vec![data, done])).unwrap();
    }
    sw.flush().unwrap();
    db.close().unwrap();
    let db = open(Mode::Normal, dir.path());
    assert_eq!(count_keys(&db), 10);
    db.close().unwrap();
}

fn on_stream(mut kv: pb::Kv, id: u32) -> pb::Kv {
    kv.stream_id = id;
    kv
}

fn done_marker(id: u32) -> pb::Kv {
    pb::Kv {
        stream_id: id,
        stream_done: true,
        ..Default::default()
    }
}

#[test]
fn send_on_closed_stream() {
    // Go panics; here the write fails and the writer can still be flushed.
    let dir = tempfile::tempdir().unwrap();
    let db = open(Mode::Normal, dir.path());
    let mut sw = db.new_stream_writer();
    sw.prepare().unwrap();
    sw.write(&list(vec![on_stream(kv("1", "v", 1), 1), done_marker(1)]))
        .unwrap();
    let err = sw
        .write(&list(vec![on_stream(kv("2", "v", 1), 1)]))
        .unwrap_err();
    assert!(matches!(err.root(), Error::InvalidRequest), "{err}");
    sw.flush().unwrap();
    db.close().unwrap();
}

#[test]
fn send_on_closed_stream2() {
    // The same within one write.
    let dir = tempfile::tempdir().unwrap();
    let db = open(Mode::Normal, dir.path());
    let mut sw = db.new_stream_writer();
    sw.prepare().unwrap();
    let err = sw
        .write(&list(vec![
            on_stream(kv("1", "v", 1), 1),
            done_marker(1),
            on_stream(kv("2", "v", 1), 1),
        ]))
        .unwrap_err();
    assert!(matches!(err.root(), Error::InvalidRequest), "{err}");
    sw.flush().unwrap();
    db.close().unwrap();
}

#[test]
fn keys_out_of_order() {
    // Go panics in the writer goroutine.
    let dir = tempfile::tempdir().unwrap();
    let db = open(Mode::Normal, dir.path());
    let mut sw = db.new_stream_writer();
    sw.prepare().unwrap();
    let err = sw
        .write(&list(vec![kv("b", "v", 1), kv("a", "v", 1)]))
        .unwrap_err();
    assert!(err.to_string().contains("not in sorted order"), "{err}");
    drop(sw);
    db.close().unwrap();
}

#[test]
fn stream_writer_encrypted() {
    let dir = tempfile::tempdir().unwrap();
    let opt = Options::new(dir.path())
        .with_encryption_key(b"badgerkey16bytes".to_vec())
        .with_block_cache_size(100 << 20)
        .with_index_cache_size(100 << 20);
    let db = DB::open(opt.clone()).unwrap();
    stream_write(&db, &list(vec![kv("mykey", "myvalue", 20)]));
    let check = |db: &DB| {
        db.view(|txn| {
            assert_eq!(txn.get(b"mykey")?.value_copy()?, b"myvalue");
            Ok(())
        })
        .unwrap()
    };
    check(&db);
    db.close().unwrap();
    let db = DB::open(opt).unwrap();
    check(&db);
    db.close().unwrap();
}

#[test]
fn stream_writer_with_large_value() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(Mode::Managed, dir.path());
    let val: Vec<u8> = (0..10 << 20).map(|i: u32| (i * 7 + 3) as u8).collect();
    stream_write(&db, &list(vec![kv("key", val.clone(), 1)]));
    let got = db.view(|txn| txn.get(b"key")?.value_copy()).unwrap();
    assert!(got == val);
    db.close().unwrap();
}

fn add_incremental(db: &DB, keys: &[&str]) {
    let mut sw = db.new_stream_writer();
    sw.prepare_incremental().unwrap();
    sw.write(&list(keys.iter().map(|k| kv(*k, "val", 1)).collect()))
        .unwrap();
    sw.flush().unwrap();
}

fn assert_found(db: &DB, keys: &[&str]) {
    db.view(|txn| {
        for k in keys {
            txn.get(k.as_bytes()).unwrap_or_else(|e| panic!("{k}: {e}"));
        }
        Ok(())
    })
    .unwrap();
}

#[test]
fn stream_writer_incremental() {
    let with_db = |f: &dyn Fn(&DB)| {
        let dir = tempfile::tempdir().unwrap();
        let db = open(Mode::Normal, dir.path());
        f(&db);
        db.close().unwrap();
    };
    // On a non-empty DB.
    with_db(&|db| {
        stream_write(db, &list(vec![kv("key-1", "val", 1)]));
        add_incremental(db, &["key-2"]);
        assert_found(db, &["key-1", "key-2"]);
    });
    // On an empty DB.
    with_db(&|db| {
        add_incremental(db, &["key-1"]);
        assert_found(db, &["key-1"]);
    });
    // Several, with overlapping key ranges.
    with_db(&|db| {
        add_incremental(db, &["a1", "c1"]);
        add_incremental(db, &["a2", "c2"]);
        add_incremental(db, &["a3", "c3"]);
        assert_found(db, &["a1", "c1", "a2", "c2", "a3", "c3"]);
    });
    // A transaction's write in the memtable blocks incremental writes.
    with_db(&|db| {
        add_incremental(db, &["a1", "c1"]);
        db.update(|txn| txn.set("a3", "c3")).unwrap();
        let mut sw = db.new_stream_writer();
        let err = sw.prepare_incremental().unwrap_err();
        assert_eq!(
            err.to_string(),
            "Unable to do incremental writes because MemTable has data: Invalid request"
        );
        drop(sw);
        assert_found(db, &["a1", "c1", "a3"]);
    });
    // More incremental writes than levels: L0 gets flattened.
    with_db(&|db| {
        let mut all = Vec::new();
        for i in 1..=9 {
            let (a, c) = (format!("a{i}"), format!("c{i}"));
            add_incremental(db, &[&a, &c]);
            all.extend([a, c]);
        }
        let all: Vec<&str> = all.iter().map(String::as_str).collect();
        assert_found(db, &all);
    });
    // Older versions first: the oracle only moves forward, so later
    // commits shadow every streamed version.
    with_db(&|db| {
        for (k, v, ver) in [("a1", "val1", 11), ("a2", "val2", 9)] {
            let mut sw = db.new_stream_writer();
            sw.prepare_incremental().unwrap();
            sw.write(&list(vec![kv(k, v, ver)])).unwrap();
            sw.flush().unwrap();
        }
        db.update(|txn| txn.set("a1", "val3")).unwrap();
        db.update(|txn| txn.set("a3", "val4")).unwrap();
        let v = db.view(|txn| txn.get(b"a1")?.value_copy()).unwrap();
        assert_eq!(v, b"val3");
    });
}
