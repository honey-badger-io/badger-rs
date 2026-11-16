//! Port of Go's `skl/skl_test.go`, plus a model test against `BTreeMap`.
//! Sizes shrink under Miri, which runs these to check the unsafe arena code
//! (including data races between the threads).

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;

use super::*;
use crate::util::keys::key_with_ts;

const ARENA_SIZE: usize = 1 << 20;

/// Scales a thread or item count down under Miri.
fn scaled(n: usize) -> usize {
    if cfg!(miri) { n.div_ceil(20).max(2) } else { n }
}

/// `v` in decimal, zero-padded to `width` (Rust's format width is a u16).
fn padded(v: usize, width: usize) -> Vec<u8> {
    let s = v.to_string();
    let mut out = vec![b'0'; width.saturating_sub(s.len())];
    out.extend_from_slice(s.as_bytes());
    out
}

fn new_value(v: usize) -> Vec<u8> {
    format!("{v:05}").into_bytes()
}

fn vs(value: &[u8], meta: u8) -> ValueStruct<'_> {
    ValueStruct {
        value,
        meta,
        ..Default::default()
    }
}

fn key(s: &str, ts: u64) -> Vec<u8> {
    key_with_ts(s.as_bytes(), ts)
}

/// Number of nodes, by walking the base level.
fn length(s: &Skiplist) -> usize {
    let mut x = s.next(s.head, 0);
    let mut count = 0;
    while x != NIL {
        count += 1;
        x = s.next(x, 0);
    }
    count
}

#[test]
fn empty() {
    let k = key("aaa", 0);
    let l = Arc::new(Skiplist::new(ARENA_SIZE));
    assert!(l.get(&k).is_none());
    assert!(l.is_empty());
    for less in [true, false] {
        for allow_equal in [true, false] {
            assert_eq!(l.find_near(&k, less, allow_equal), (None, false));
        }
    }
    let mut it = l.iter();
    assert!(!it.valid());
    it.seek_to_first();
    assert!(!it.valid());
    it.seek_to_last();
    assert!(!it.valid());
    it.seek(&k);
    assert!(!it.valid());
    // Go checks reference counting here; with Arc, the iterator keeps it alive.
    let weak = Arc::downgrade(&l);
    drop(l);
    assert!(weak.upgrade().is_some());
    drop(it);
    assert!(weak.upgrade().is_none());
}

#[test]
fn basic() {
    let l = Skiplist::new(ARENA_SIZE);
    let (val1, val2, val3, val4) = (new_value(42), new_value(52), new_value(62), new_value(72));
    // 100 KB: more than u16::MAX.
    let val5 = padded(1, 102_400);

    l.put(&key("key1", 0), vs(&val1, 55));
    l.put(&key("key2", 2), vs(&val2, 56));
    l.put(&key("key3", 0), vs(&val3, 57));

    assert!(l.get(&key("key", 0)).is_none());

    let v = l.get(&key("key1", 0)).unwrap();
    assert_eq!(v.value, b"00042");
    assert_eq!(v.meta, 55);

    assert!(l.get(&key("key2", 0)).is_none());

    let v = l.get(&key("key3", 0)).unwrap();
    assert_eq!(v.value, b"00062");
    assert_eq!(v.meta, 57);

    l.put(&key("key3", 1), vs(&val4, 12));
    let v = l.get(&key("key3", 1)).unwrap();
    assert_eq!(v.value, b"00072");
    assert_eq!(v.meta, 12);
    assert_eq!(v.version, 1);

    l.put(&key("key4", 1), vs(&val5, 60));
    let v = l.get(&key("key4", 1)).unwrap();
    assert_eq!(v.value, &val5[..]);
    assert_eq!(v.meta, 60);
}

/// Concurrent writes followed by concurrent reads.
#[test]
fn concurrent_basic() {
    let n = scaled(1000);
    let l = Skiplist::new(ARENA_SIZE);
    let k = |i: usize| key(&format!("{i:05}"), 0);
    thread::scope(|s| {
        for i in 0..n {
            let (l, k) = (&l, &k);
            s.spawn(move || l.put(&k(i), vs(&new_value(i), 0)));
        }
    });
    thread::scope(|s| {
        for i in 0..n {
            let (l, k) = (&l, &k);
            s.spawn(move || assert_eq!(l.get(&k(i)).unwrap().value, new_value(i)));
        }
    });
    assert_eq!(length(&l), n);
}

#[test]
#[cfg_attr(miri, ignore)] // 120 MB arena
fn concurrent_basic_big_values() {
    let n = 100;
    let l = Skiplist::new(120 << 20);
    let k = |i: usize| key(&format!("{i:05}"), 0);
    // 1 MB values: more than u16::MAX.
    let big = |i: usize| padded(i, 1 << 20);
    thread::scope(|s| {
        for i in 0..n {
            let (l, k, big) = (&l, &k, &big);
            s.spawn(move || l.put(&k(i), vs(&big(i), 0)));
        }
    });
    thread::scope(|s| {
        for i in 0..n {
            let (l, k, big) = (&l, &k, &big);
            s.spawn(move || assert_eq!(l.get(&k(i)).unwrap().value, big(i)));
        }
    });
    assert_eq!(length(&l), n);
}

/// Reads while writing to one single key.
#[test]
fn one_key() {
    let n = scaled(100);
    let k = key("thekey", 0);
    let l = Skiplist::new(ARENA_SIZE);
    let saw_value = AtomicUsize::new(0);
    thread::scope(|s| {
        for i in 0..n {
            let (l, k) = (&l, &k);
            s.spawn(move || l.put(k, vs(&new_value(i), 0)));
        }
        for _ in 0..n {
            let (l, k, saw_value) = (&l, &k, &saw_value);
            s.spawn(move || {
                let Some(p) = l.get(k) else { return };
                saw_value.fetch_add(1, Ordering::Relaxed);
                let v: usize = std::str::from_utf8(p.value).unwrap().parse().unwrap();
                assert!(v < n, "invalid value {v}");
            });
        }
    });
    // Go only expects "some" reads to see a value, which threads make likely
    // but not certain. After all puts, a read must.
    assert!(l.get(&k).is_some());
    assert_eq!(length(&l), 1);
}

#[test]
fn find_near() {
    let l = Skiplist::new(ARENA_SIZE);
    for i in 0..1000 {
        l.put(&key(&format!("{:05}", i * 10 + 5), 0), vs(&new_value(i), 0));
    }
    let check = |k: &str, less: bool, allow_equal: bool, want: Option<&str>, eq: bool| {
        let (n, found) = l.find_near(&key(k, 0), less, allow_equal);
        assert_eq!(
            n.map(|n| l.key(n).to_vec()),
            want.map(|w| key(w, 0)),
            "find_near({k}, less={less}, allow_equal={allow_equal})"
        );
        assert_eq!(found, eq);
    };
    check("00001", false, false, Some("00005"), false);
    check("00001", false, true, Some("00005"), false);
    check("00001", true, false, None, false);
    check("00001", true, true, None, false);

    check("00005", false, false, Some("00015"), false);
    check("00005", false, true, Some("00005"), true);
    check("00005", true, false, None, false);
    check("00005", true, true, Some("00005"), true);

    check("05555", false, false, Some("05565"), false);
    check("05555", false, true, Some("05555"), true);
    check("05555", true, false, Some("05545"), false);
    check("05555", true, true, Some("05555"), true);

    check("05558", false, false, Some("05565"), false);
    check("05558", false, true, Some("05565"), false);
    check("05558", true, false, Some("05555"), false);
    check("05558", true, true, Some("05555"), false);

    check("09995", false, false, None, false);
    check("09995", false, true, Some("09995"), true);
    check("09995", true, false, Some("09985"), false);
    check("09995", true, true, Some("09995"), true);

    check("59995", false, false, None, false);
    check("59995", false, true, None, false);
    check("59995", true, false, Some("09995"), false);
    check("59995", true, true, Some("09995"), false);
}

#[test]
fn iterator_next() {
    let n = 100;
    let l = Arc::new(Skiplist::new(ARENA_SIZE));
    let mut it = l.iter();
    assert!(!it.valid());
    it.seek_to_first();
    assert!(!it.valid());
    for i in (0..n).rev() {
        l.put(&key(&format!("{i:05}"), 0), vs(&new_value(i), 0));
    }
    it.seek_to_first();
    for i in 0..n {
        assert!(it.valid());
        assert_eq!(it.value().value, new_value(i));
        it.next();
    }
    assert!(!it.valid());
}

#[test]
fn iterator_prev() {
    let n = 100;
    let l = Arc::new(Skiplist::new(ARENA_SIZE));
    let mut it = l.iter();
    assert!(!it.valid());
    it.seek_to_first();
    assert!(!it.valid());
    for i in 0..n {
        l.put(&key(&format!("{i:05}"), 0), vs(&new_value(i), 0));
    }
    it.seek_to_last();
    for i in (0..n).rev() {
        assert!(it.valid());
        assert_eq!(it.value().value, new_value(i));
        it.prev();
    }
    assert!(!it.valid());
}

#[test]
fn iterator_seek() {
    let n = 100;
    let l = Arc::new(Skiplist::new(ARENA_SIZE));
    let mut it = l.iter();
    assert!(!it.valid());
    it.seek_to_first();
    assert!(!it.valid());
    // 1000, 1010, 1020, ..., 1990.
    for i in (0..n).rev() {
        let v = i * 10 + 1000;
        l.put(&key(&format!("{v:05}"), 0), vs(&new_value(v), 0));
    }
    let value = |it: &Iter| it.value().value.to_vec();
    it.seek_to_first();
    assert!(it.valid());
    assert_eq!(value(&it), b"01000");

    it.seek(&key("01000", 0));
    assert_eq!(value(&it), b"01000");
    it.seek(&key("01005", 0));
    assert_eq!(value(&it), b"01010");
    it.seek(&key("01010", 0));
    assert_eq!(value(&it), b"01010");
    it.seek(&key("99999", 0));
    assert!(!it.valid());

    it.seek_for_prev(&key("00", 0));
    assert!(!it.valid());
    it.seek_for_prev(&key("01000", 0));
    assert_eq!(value(&it), b"01000");
    it.seek_for_prev(&key("01005", 0));
    assert_eq!(value(&it), b"01000");
    it.seek_for_prev(&key("01010", 0));
    assert_eq!(value(&it), b"01010");
    it.seek_for_prev(&key("99999", 0));
    assert_eq!(value(&it), b"01990");
}

/// Versions of a key sort newest first, and `get` finds the newest version at
/// or below the requested one.
#[test]
fn versions_and_uni_iter() {
    let l = Arc::new(Skiplist::new(ARENA_SIZE));
    for ts in [1, 5, 3] {
        l.put(&key("a", ts), vs(format!("a{ts}").as_bytes(), 0));
    }
    l.put(&key("b", 2), vs(b"b2", 0));
    assert_eq!(l.get(&key("a", 4)).unwrap().value, b"a3");
    assert_eq!(l.get(&key("a", 4)).unwrap().version, 3);
    assert_eq!(l.get(&key("a", 9)).unwrap().value, b"a5");
    assert!(l.get(&key("a", 0)).is_none());

    let collect = |reversed: bool| {
        let mut it = l.uni_iter(reversed);
        let mut out = Vec::new();
        it.rewind();
        while it.valid() {
            out.push(it.value().value.to_vec());
            it.next();
        }
        out
    };
    let fwd: Vec<&[u8]> = vec![b"a5", b"a3", b"a1", b"b2"];
    assert_eq!(collect(false), fwd);
    assert_eq!(collect(true), fwd.into_iter().rev().collect::<Vec<_>>());

    let mut it = l.uni_iter(true);
    it.seek(&key("a", 4));
    assert_eq!(it.value().value, b"a5", "reversed seek finds <= target");
}

/// Arena accounting matches Go: an empty skiplist holds one full-height head.
#[test]
fn mem_size_matches_go_layout() {
    assert_eq!(MAX_NODE_SIZE, 96);
    let l = Skiplist::new(ARENA_SIZE);
    // 1 (reserved) + 96 + 7 (alignment pad) + 0 (empty key) + 3 (empty value).
    assert_eq!(l.mem_size(), 1 + 96 + 7 + 3);
    let head_height = l.arena.node_height(l.head);
    assert_eq!(head_height as usize, MAX_HEIGHT);
}

#[test]
#[should_panic(expected = "Arena too small")]
fn full_arena_panics() {
    let l = Skiplist::new(1024);
    for i in 0..100 {
        l.put(&key(&format!("{i:05}"), 0), vs(b"value", 0));
    }
}

/// Random puts from several threads, checked against a `BTreeMap` model.
#[test]
fn concurrent_matches_model() {
    let threads = if cfg!(miri) { 3 } else { 8 };
    let per_thread = scaled(2000);
    let l = Arc::new(Skiplist::new(8 << 20));
    let model: Vec<BTreeMap<Vec<u8>, Vec<u8>>> = thread::scope(|s| {
        let handles: Vec<_> = (0..threads)
            .map(|t| {
                let l = &l;
                s.spawn(move || {
                    // Each thread owns the keys congruent to t, so the final
                    // value of each key is the thread's last write.
                    let mut model = BTreeMap::new();
                    let mut x = 0x9e37_79b9u64 + t as u64;
                    for i in 0..per_thread {
                        x = x
                            .wrapping_mul(6364136223846793005)
                            .wrapping_add(1442695040888963407);
                        let k = (x >> 33) as usize % (per_thread / 2).max(1);
                        let k = key_with_ts(format!("{:08}", k * threads + t).as_bytes(), 7);
                        let v = format!("{t}-{i}").into_bytes();
                        l.put(&k, vs(&v, 0));
                        // Interleave reads of our own keys.
                        assert_eq!(l.get(&k).unwrap().value, &v[..]);
                        model.insert(k, v);
                    }
                    model
                })
            })
            .collect();
        handles.into_iter().map(|h| h.join().unwrap()).collect()
    });
    let model: BTreeMap<_, _> = model.into_iter().flatten().collect();

    let mut it = l.iter();
    it.seek_to_first();
    for (k, v) in &model {
        assert!(it.valid());
        assert_eq!(it.key(), &k[..]);
        assert_eq!(it.value().value, &v[..]);
        it.next();
    }
    assert!(!it.valid());
}

/// Iterating while other threads insert sees a sorted sequence.
#[test]
fn iterate_while_inserting() {
    let n = scaled(2000);
    let l = Arc::new(Skiplist::new(8 << 20));
    thread::scope(|s| {
        for t in 0..2 {
            let l = &l;
            s.spawn(move || {
                for i in 0..n {
                    l.put(&key(&format!("{:06}", i * 2 + t), 0), vs(b"v", 0));
                }
            });
        }
        let l = &l;
        s.spawn(move || {
            for _ in 0..scaled(20) {
                let mut it = l.iter();
                it.seek_to_first();
                let mut last: Option<Vec<u8>> = None;
                while it.valid() {
                    if let Some(last) = &last {
                        assert!(compare_keys(last, it.key()).is_lt());
                    }
                    last = Some(it.key().to_vec());
                    it.next();
                }
            }
        });
    });
    assert_eq!(length(&l), 2 * n);
}
