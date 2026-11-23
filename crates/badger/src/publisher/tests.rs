//! Port of Go's `publisher_test.go`, plus unsubscribing, closing, ignored
//! bytes and deletes.

use std::sync::Barrier;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use crate::{DB, Options, pb};

fn prefix(p: &str) -> pb::Match {
    pb::Match {
        prefix: p.as_bytes().to_vec(),
        ignore_bytes: String::new(),
    }
}

fn with_db(f: impl FnOnce(&DB)) {
    let dir = tempfile::tempdir().unwrap();
    let db = DB::open(Options::new(dir.path())).unwrap();
    f(&db);
    db.close().unwrap();
}

/// Receives until `n` KVs arrived (or panics after a while).
fn recv_n(sub: &crate::Subscription, n: usize) -> Vec<pb::Kv> {
    let mut out = Vec::new();
    while out.len() < n {
        let list = sub
            .recv_timeout(Duration::from_secs(10))
            .unwrap_or_else(|| panic!("got {} of {n} updates", out.len()));
        out.extend(list.kv);
    }
    out
}

#[test]
fn publisher_deadlock() {
    // A subscriber that stops reading while many writes go on must not
    // deadlock them (up to Go's queue sizes), and unsubscribing frees them.
    with_db(|db| {
        let sub = db.subscribe(&[prefix("ke")]).unwrap();
        db.update(|txn| txn.set("key0", "value0")).unwrap();
        // The first update arrives; then the subscriber stalls.
        assert!(!recv_n(&sub, 1).is_empty());
        let done = AtomicUsize::new(0);
        std::thread::scope(|s| {
            for t in 0..8 {
                let done = &done;
                s.spawn(move || {
                    for i in (1 + t..1110).step_by(8) {
                        db.update(|txn| txn.set(format!("key{i}"), format!("value{i}")))
                            .unwrap();
                        done.fetch_add(1, Ordering::SeqCst);
                    }
                });
            }
        });
        assert_eq!(done.load(Ordering::SeqCst), 1109);
        // Go's subscriber returns an error here; dropping unsubscribes.
        drop(sub);
        // Writes keep working without subscribers.
        for i in 0..3000 {
            db.update(|txn| txn.set(format!("key-after{i}"), "v"))
                .unwrap();
        }
    });
}

#[test]
fn publisher_ordering() {
    with_db(|db| {
        let sub = db.subscribe(&[prefix("ke")]).unwrap();
        for i in 0..5 {
            db.update(|txn| txn.set(format!("key{i}"), format!("value{i}")))
                .unwrap();
        }
        let order: Vec<String> = recv_n(&sub, 5)
            .into_iter()
            .map(|kv| String::from_utf8(kv.value).unwrap())
            .collect();
        let want: Vec<String> = (0..5).map(|i| format!("value{i}")).collect();
        assert_eq!(order, want);
    });
}

#[test]
fn multiple_prefix() {
    with_db(|db| {
        let sub = db.subscribe(&[prefix("ke"), prefix("hel")]).unwrap();
        db.update(|txn| txn.set("key", "value")).unwrap();
        db.update(|txn| txn.set("other", "ignored")).unwrap();
        db.update(|txn| txn.set("hello", "badger")).unwrap();
        let kvs = recv_n(&sub, 2);
        assert_eq!(kvs.len(), 2);
        for kv in &kvs {
            match kv.key.as_slice() {
                b"key" => assert_eq!(kv.value, b"value"),
                b"hello" => assert_eq!(kv.value, b"badger"),
                k => panic!("unexpected key {k:?}"),
            }
        }
    });
}

#[test]
fn several_subscribers_versions_and_deletes() {
    with_db(|db| {
        let a = db.subscribe(&[prefix("a")]).unwrap();
        let all = db.subscribe(&[prefix("")]).unwrap();
        db.update(|txn| {
            txn.set_entry(crate::Entry::new("a1", "x").with_meta(5))?;
            txn.set("b1", "y")
        })
        .unwrap();
        db.update(|txn| txn.delete("a1")).unwrap();

        let got = recv_n(&a, 2);
        assert_eq!(got[0].key, b"a1");
        assert_eq!(got[0].value, b"x");
        // Go puts the user meta in `meta`.
        assert_eq!(got[0].meta, [5]);
        assert_eq!(got[0].version, 1);
        // A delete arrives with an empty value.
        assert_eq!(
            (got[1].key.as_slice(), got[1].value.len()),
            (b"a1".as_slice(), 0)
        );
        assert_eq!(got[1].version, 2);

        // The empty prefix matches every key, Badger's transaction markers
        // (`!badger!txn`) included, as in Go: two writes and a marker, then
        // a delete and a marker.
        let keys: Vec<Vec<u8>> = recv_n(&all, 5).into_iter().map(|kv| kv.key).collect();
        assert!(keys.contains(&b"a1".to_vec()) && keys.contains(&b"b1".to_vec()));
        assert_eq!(
            keys.iter()
                .filter(|k| k.starts_with(b"!badger!txn"))
                .count(),
            2
        );
    });
}

#[test]
fn ignore_bytes() {
    // Byte 1 is ignored: "a?c" matches "abc" and "axc", not "abd".
    with_db(|db| {
        let sub = db
            .subscribe(&[pb::Match {
                prefix: b"abc".to_vec(),
                ignore_bytes: "1".into(),
            }])
            .unwrap();
        for k in ["abd", "abc", "axc"] {
            db.update(|txn| txn.set(k, "v")).unwrap();
        }
        let keys: Vec<Vec<u8>> = recv_n(&sub, 2).into_iter().map(|kv| kv.key).collect();
        assert_eq!(keys, [b"abc".to_vec(), b"axc".to_vec()]);
    });
}

#[test]
fn close_ends_subscriptions() {
    let dir = tempfile::tempdir().unwrap();
    let db = DB::open(Options::new(dir.path())).unwrap();
    let sub = db.subscribe(&[prefix("k")]).unwrap();
    let barrier = Barrier::new(2);
    std::thread::scope(|s| {
        let reader = s.spawn(|| {
            barrier.wait();
            let mut n = 0;
            // Ends once the DB is closed and the updates are drained.
            while let Some(list) = sub.recv() {
                n += list.kv.len();
            }
            n
        });
        for i in 0..10 {
            db.update(|txn| txn.set(format!("k{i}"), "v")).unwrap();
        }
        barrier.wait();
        db.close().unwrap();
        assert_eq!(reader.join().unwrap(), 10);
    });
    assert!(sub.is_closed());
}

#[test]
fn subscribe_after_close_fails_and_read_only_dbs_close() {
    let dir = tempfile::tempdir().unwrap();
    let db = DB::open(Options::new(dir.path())).unwrap();
    db.update(|txn| txn.set("k", "v")).unwrap();
    db.close().unwrap();
    let db = DB::open(Options::new(dir.path()).with_read_only(true)).unwrap();
    let sub = db.subscribe(&[prefix("k")]).unwrap();
    db.close().unwrap();
    assert!(sub.recv().is_none());
    assert!(sub.is_closed());
}
