//! Exhaustive interleaving checks with `loom` (small models). Run with
//! `make loom`:
//!
//! ```sh
//! RUSTFLAGS="--cfg loom" cargo test -p badger --lib --release skl::loom_tests
//! ```

// std Arc: the refcount is not under test, and `Skiplist::iter` takes it.
use std::sync::Arc;

use loom::thread;

use super::*;
use crate::util::keys::{compare_keys, key_with_ts};

const ARENA: usize = 2048;

fn vs(value: &[u8]) -> ValueStruct<'_> {
    ValueStruct {
        value,
        ..Default::default()
    }
}

fn k(s: &str) -> Vec<u8> {
    key_with_ts(s.as_bytes(), 1)
}

fn model(f: impl Fn() + Sync + Send + 'static) {
    let mut b = loom::model::Builder::new();
    b.preemption_bound = Some(3);
    b.check(f);
}

fn keys(l: &Arc<Skiplist>) -> Vec<Vec<u8>> {
    let mut it = l.iter();
    it.seek_to_first();
    let mut out = Vec::new();
    while it.valid() {
        out.push(it.key().to_vec());
        it.next();
    }
    out
}

/// Two threads insert different keys into the same gap, so their CASes race
/// on the same link. Both end up linked, in order.
#[test]
fn concurrent_inserts_same_gap() {
    model(|| {
        let l = Arc::new(Skiplist::new(ARENA));
        l.put(&k("a"), vs(b"a"));
        l.put(&k("d"), vs(b"d"));
        let l1 = l.clone();
        let t1 = thread::spawn(move || l1.put(&k("b"), vs(b"b")));
        l.put(&k("c"), vs(b"c"));
        t1.join().unwrap();
        assert_eq!(keys(&l), vec![k("a"), k("b"), k("c"), k("d")]);
        for key in ["a", "b", "c", "d"] {
            assert_eq!(l.get(&k(key)).unwrap().value, key.as_bytes());
        }
        // Every level is sorted and only holds linked nodes.
        for h in 0..MAX_HEIGHT {
            let mut prev: Option<Vec<u8>> = None;
            let mut n = l.next(l.head, h);
            while n != NIL {
                let key = l.key(n).to_vec();
                assert!(prev.as_ref().is_none_or(|p| compare_keys(p, &key).is_lt()));
                prev = Some(key);
                n = l.next(n, h);
            }
        }
    });
}

/// Two threads insert the same key: one node results, holding one of the values.
#[test]
fn concurrent_same_key() {
    model(|| {
        let l = Arc::new(Skiplist::new(ARENA));
        let l1 = l.clone();
        let t1 = thread::spawn(move || l1.put(&k("x"), vs(b"1")));
        l.put(&k("x"), vs(b"2"));
        t1.join().unwrap();
        assert_eq!(keys(&l), vec![k("x")]);
        let v = l.get(&k("x")).unwrap().value.to_vec();
        assert!(v == b"1" || v == b"2", "{v:?}");
    });
}

/// A reader racing with a put sees either nothing or the complete value, and
/// never a broken list.
#[test]
fn get_during_put() {
    model(|| {
        let l = Arc::new(Skiplist::new(ARENA));
        l.put(&k("a"), vs(b"old"));
        let w = l.clone();
        let t = thread::spawn(move || {
            w.put(&k("b"), vs(b"new-b"));
            w.put(&k("a"), vs(b"new-a"));
        });
        match l.get(&k("b")) {
            None => {}
            Some(v) => assert_eq!(v.value, b"new-b"),
        }
        let a = l.get(&k("a")).unwrap().value.to_vec();
        assert!(a == b"old" || a == b"new-a");
        let seen = keys(&l);
        assert!(seen == vec![k("a")] || seen == vec![k("a"), k("b")]);
        t.join().unwrap();
        assert_eq!(l.get(&k("a")).unwrap().value, b"new-a");
    });
}
