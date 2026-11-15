//! Ports of Go's `table/table_test.go`, `builder_test.go` and
//! `merge_iterator_test.go`. Go's `Close`-counting checks become `Drop`
//! counters.

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use super::*;
use crate::util::iterator::KvIterator;
use crate::util::keys::{key_with_ts, parse_key};
use crate::util::value::ValueStruct;

static NEXT_ID: AtomicU64 = AtomicU64::new(1);

/// A temp dir that lives for the whole test run; tables get unique ids.
fn temp_path() -> PathBuf {
    static DIR: std::sync::OnceLock<tempfile::TempDir> = std::sync::OnceLock::new();
    let dir = DIR.get_or_init(|| tempfile::tempdir().unwrap());
    new_filename(NEXT_ID.fetch_add(1, Ordering::SeqCst), dir.path())
}

fn key(prefix: &str, i: i64) -> String {
    format!("{prefix}{i:04}")
}

fn test_opts() -> TableOptions {
    TableOptions {
        compression: CompressionType::Zstd,
        zstd_compression_level: 15,
        block_size: 4 * 1024,
        bloom_false_positive: 0.01,
        ..Default::default()
    }
}

fn build_table(mut kvs: Vec<(String, String)>, mut opts: TableOptions) -> Arc<Table> {
    if opts.block_size == 0 {
        opts.block_size = 4 * 1024;
    }
    kvs.sort();
    let mut b = TableBuilder::new(opts);
    for (k, v) in &kvs {
        let vs = ValueStruct {
            value: v.as_bytes(),
            meta: b'A',
            ..Default::default()
        };
        b.add(&key_with_ts(k.as_bytes(), 0), vs, 0);
    }
    let t = Table::create(temp_path(), b).expect("writing to file failed");
    t.delete_on_drop();
    t
}

fn build_test_table(prefix: &str, n: i64, opts: TableOptions) -> Arc<Table> {
    assert!(n <= 10000);
    build_table(
        (0..n).map(|i| (key(prefix, i), i.to_string())).collect(),
        opts,
    )
}

fn kv(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

fn user_key(it: &impl KvIterator) -> String {
    String::from_utf8(parse_key(it.key()).to_vec()).unwrap()
}

fn value_str(it: &impl KvIterator) -> String {
    String::from_utf8(it.value().value.to_vec()).unwrap()
}

#[test]
fn table_iterator() {
    for n in [99, 100, 101] {
        let table = build_test_table("key", n, test_opts());
        let mut it = table.iter(false, true);
        let mut count = 0;
        it.rewind();
        while it.valid() {
            assert_eq!(it.key(), key_with_ts(key("key", count).as_bytes(), 0));
            assert_eq!(value_str(&it), count.to_string());
            count += 1;
            it.next();
        }
        assert_eq!(count, n);
    }
}

#[test]
fn seek_to_first_and_last() {
    for n in [99, 100, 101, 199, 200, 250, 9999, 10000] {
        let table = build_test_table("key", n, test_opts());
        let mut it = table.iter(false, true);
        it.seek_to_first();
        assert!(it.valid());
        assert_eq!((value_str(&it), it.value().meta), ("0".into(), b'A'));

        it.seek_to_last();
        assert!(it.valid());
        assert_eq!(value_str(&it), (n - 1).to_string());
        it.prev();
        assert!(it.valid());
        assert_eq!(value_str(&it), (n - 2).to_string());
    }
}

#[test]
fn seek() {
    let table = build_test_table("k", 10000, test_opts());
    let mut it = table.iter(false, true);
    for (input, valid, out) in [
        ("abc", true, "k0000"),
        ("k0100", true, "k0100"),
        ("k0100b", true, "k0101"), // jumps to the next block
        ("k1234", true, "k1234"),
        ("k1234b", true, "k1235"),
        ("k9999", true, "k9999"),
        ("z", false, ""),
    ] {
        it.seek_forward(&key_with_ts(input.as_bytes(), 0));
        assert_eq!(it.valid(), valid, "{input}");
        if valid {
            assert_eq!(user_key(&it), out);
        }
    }
}

#[test]
fn seek_for_prev() {
    let table = build_test_table("k", 10000, test_opts());
    let mut it = table.iter(false, true);
    for (input, valid, out) in [
        ("abc", false, ""),
        ("k0100", true, "k0100"),
        ("k0100b", true, "k0100"),
        ("k1234", true, "k1234"),
        ("k1234b", true, "k1234"),
        ("k9999", true, "k9999"),
        ("z", true, "k9999"),
    ] {
        it.seek_for_prev(&key_with_ts(input.as_bytes(), 0));
        assert_eq!(it.valid(), valid, "{input}");
        if valid {
            assert_eq!(user_key(&it), out);
        }
    }
}

#[test]
fn iterate_from_start_and_end() {
    for n in [99, 100, 101, 199, 200, 250, 9999, 10000] {
        let table = build_test_table("key", n, test_opts());
        let mut it = table.iter(false, true);
        it.seek_to_first();
        let mut count = 0;
        while it.valid() {
            assert_eq!((value_str(&it), it.value().meta), (count.to_string(), b'A'));
            count += 1;
            it.next_forward();
        }
        assert_eq!(count, n);

        it.seek_forward(&key_with_ts(b"zzzzzz", 0)); // past the end
        assert!(!it.valid());
        for i in (0..n).rev() {
            it.prev();
            assert!(it.valid(), "n={n} i={i}");
            assert_eq!(value_str(&it), i.to_string());
        }
        it.prev();
        assert!(!it.valid());
    }
}

#[test]
fn table_seek_and_scan() {
    let table = build_test_table("key", 10000, test_opts());
    let mut it = table.iter(false, true);
    let mut kid = 1010;
    it.seek_forward(&key_with_ts(key("key", kid).as_bytes(), 0));
    while it.valid() {
        assert_eq!(user_key(&it), key("key", kid));
        kid += 1;
        it.next_forward();
    }
    assert_eq!(kid, 10000);

    it.seek_forward(&key_with_ts(key("key", 99999).as_bytes(), 0));
    assert!(!it.valid());
    it.seek_forward(&key_with_ts(key("key", -1).as_bytes(), 0));
    assert!(it.valid());
    assert_eq!(user_key(&it), key("key", 0));
}

#[test]
fn iterate_back_and_forth() {
    let table = build_test_table("key", 10000, test_opts());
    let seek = key_with_ts(key("key", 1010).as_bytes(), 0);
    let mut it = table.iter(false, true);
    it.seek_forward(&seek);
    assert!(it.valid());
    assert_eq!(it.key(), seek);

    it.prev();
    it.prev();
    assert_eq!(user_key(&it), key("key", 1008));
    it.next_forward();
    it.next_forward();
    assert_eq!(user_key(&it), key("key", 1010));

    it.seek_forward(&key_with_ts(key("key", 2000).as_bytes(), 0));
    assert_eq!(user_key(&it), key("key", 2000));
    it.prev();
    assert_eq!(user_key(&it), key("key", 1999));
    it.seek_to_first();
    assert_eq!(user_key(&it), key("key", 0));
}

#[test]
fn uni_iterator() {
    let table = build_test_table("key", 10000, test_opts());
    for reversed in [false, true] {
        let mut it = table.iter(reversed, true);
        let mut count = 0;
        it.rewind();
        while it.valid() {
            let want = if reversed { 10000 - 1 - count } else { count };
            assert_eq!((value_str(&it), it.value().meta), (want.to_string(), b'A'));
            count += 1;
            it.next();
        }
        assert_eq!(count, 10000);
    }
}

#[test]
fn concat_iterator_one_table() {
    let t = build_table(kv(&[("k1", "a1"), ("k2", "a2")]), test_opts());
    let mut it = ConcatIterator::new(vec![t], false, true);
    it.rewind();
    assert!(it.valid());
    assert_eq!(
        (user_key(&it), value_str(&it), it.value().meta),
        ("k1".into(), "a1".into(), b'A')
    );
}

#[test]
fn concat_iterator() {
    let tables = vec![
        build_test_table("keya", 10000, test_opts()),
        build_test_table("keyb", 10000, test_opts()),
        build_test_table("keyc", 10000, test_opts()),
    ];
    let seek = |it: &mut ConcatIterator, k: &str| it.seek(&key_with_ts(k.as_bytes(), 0));

    let mut it = ConcatIterator::new(tables.clone(), false, true);
    it.rewind();
    let mut count = 0;
    while it.valid() {
        assert_eq!(value_str(&it), (count % 10000).to_string());
        count += 1;
        it.next();
    }
    assert_eq!(count, 30000);
    seek(&mut it, "a");
    assert_eq!(
        (user_key(&it), value_str(&it)),
        ("keya0000".into(), "0".into())
    );
    seek(&mut it, "keyb");
    assert_eq!(
        (user_key(&it), value_str(&it)),
        ("keyb0000".into(), "0".into())
    );
    seek(&mut it, "keyb9999b");
    assert_eq!(
        (user_key(&it), value_str(&it)),
        ("keyc0000".into(), "0".into())
    );
    seek(&mut it, "keyd");
    assert!(!it.valid());

    let mut it = ConcatIterator::new(tables, true, true);
    it.rewind();
    let mut count = 0;
    while it.valid() {
        assert_eq!(value_str(&it), (10000 - (count % 10000) - 1).to_string());
        count += 1;
        it.next();
    }
    assert_eq!(count, 30000);
    seek(&mut it, "a");
    assert!(!it.valid());
    seek(&mut it, "keyb");
    assert_eq!(
        (user_key(&it), value_str(&it)),
        ("keya9999".into(), "9999".into())
    );
    seek(&mut it, "keyb9999b");
    assert_eq!(
        (user_key(&it), value_str(&it)),
        ("keyb9999".into(), "9999".into())
    );
    seek(&mut it, "keyd");
    assert_eq!(
        (user_key(&it), value_str(&it)),
        ("keyc9999".into(), "9999".into())
    );
}

fn collect(it: &mut dyn KvIterator) -> Vec<(String, String)> {
    let mut out = Vec::new();
    while it.valid() {
        out.push((
            String::from_utf8(parse_key(it.key()).to_vec()).unwrap(),
            String::from_utf8(it.value().value.to_vec()).unwrap(),
        ));
        it.next();
    }
    out
}

#[test]
fn merging_iterator_over_tables() {
    let t1 = build_table(kv(&[("k1", "a1"), ("k4", "a4"), ("k5", "a5")]), test_opts());
    let t2 = build_table(kv(&[("k2", "b2"), ("k3", "b3"), ("k4", "b4")]), test_opts());
    let mut it = MergeIterator::merge(
        vec![
            Box::new(t1.iter(false, true)),
            Box::new(ConcatIterator::new(vec![t2], false, true)),
        ],
        false,
    )
    .unwrap();
    it.rewind();
    assert_eq!(
        collect(it.as_mut()),
        kv(&[
            ("k1", "a1"),
            ("k2", "b2"),
            ("k3", "b3"),
            ("k4", "a4"),
            ("k5", "a5")
        ])
    );
    assert!(!it.valid());
}

#[test]
fn merging_iterator_reversed() {
    let t1 = build_table(
        kv(&[("k1", "a1"), ("k2", "a2"), ("k4", "a4"), ("k5", "a5")]),
        test_opts(),
    );
    let t2 = build_table(
        kv(&[("k1", "b2"), ("k3", "b3"), ("k4", "b4"), ("k5", "b5")]),
        test_opts(),
    );
    let mut it = MergeIterator::merge(
        vec![
            Box::new(t1.iter(true, true)),
            Box::new(ConcatIterator::new(vec![t2], true, true)),
        ],
        true,
    )
    .unwrap();
    it.rewind();
    assert_eq!(
        collect(it.as_mut()),
        kv(&[
            ("k5", "a5"),
            ("k4", "a4"),
            ("k3", "b3"),
            ("k2", "a2"),
            ("k1", "a1")
        ])
    );
}

#[test]
fn merging_iterator_take_one_and_two() {
    let t1 = build_table(kv(&[("k1", "a1"), ("k2", "a2")]), test_opts());
    let t2 = build_table(kv(&[("l1", "b1")]), test_opts());
    for (first, second) in [(&t1, &t2), (&t2, &t1)] {
        let mut it = MergeIterator::merge(
            vec![
                Box::new(ConcatIterator::new(vec![Arc::clone(first)], false, true)),
                Box::new(ConcatIterator::new(vec![Arc::clone(second)], false, true)),
            ],
            false,
        )
        .unwrap();
        it.rewind();
        assert_eq!(
            collect(it.as_mut()),
            kv(&[("k1", "a1"), ("k2", "a2"), ("l1", "b1")])
        );
    }
}

#[test]
fn table_big_values() {
    // 1 MiB values (> u16::MAX), zero-padded like Go's "%01048576d".
    let value = |i: usize| {
        let s = i.to_string();
        format!("{}{s}", "0".repeat((1 << 20) - s.len())).into_bytes()
    };
    let n = 100;
    let opts = TableOptions {
        compression: CompressionType::Zstd,
        block_size: 4 * 1024,
        bloom_false_positive: 0.01,
        table_size: (n as u64) << 20,
        ..Default::default()
    };
    let mut b = TableBuilder::new(opts);
    for i in 0..n {
        let v = value(i);
        let vs = ValueStruct {
            value: &v,
            ..Default::default()
        };
        b.add(
            &key_with_ts(key("", i as i64).as_bytes(), i as u64 + 1),
            vs,
            0,
        );
    }
    let t = Table::create(temp_path(), b).unwrap();
    t.delete_on_drop();
    let mut it = t.iter(false, true);
    let mut count = 0;
    it.rewind();
    while it.valid() {
        assert_eq!(parse_key(it.key()), key("", count as i64).as_bytes());
        assert_eq!(it.value().value, value(count));
        count += 1;
        it.next();
    }
    assert_eq!(count, n);
    assert_eq!(t.max_version(), n as u64);
}

#[test]
fn table_checksum() {
    let opts = TableOptions {
        checksum_mode: ChecksumVerificationMode::OnTableAndBlockRead,
        compression: CompressionType::None,
        ..test_opts()
    };
    let t = build_test_table("k", 10000, opts.clone());
    let mut data = fs::read(t.filename()).unwrap();
    for (i, b) in data[128..228].iter_mut().enumerate() {
        *b ^= 0x5a ^ i as u8;
    }
    let err = Table::open_in_memory(data, 999_999, opts).unwrap_err();
    assert!(
        matches!(err.root(), Error::ChecksumMismatch { .. }),
        "{err}"
    );
}

#[test]
fn invalid_compression() {
    let opts = TableOptions {
        block_size: 4 << 10,
        compression: CompressionType::Zstd,
        ..Default::default()
    };
    let t = build_test_table("key", 1000, opts.clone());
    let data = fs::read(t.filename()).unwrap();
    Table::open_in_memory(data.clone(), 1_000_001, opts.clone()).unwrap();
    let wrong = TableOptions {
        compression: CompressionType::Snappy,
        ..opts
    };
    assert!(Table::open_in_memory(data, 1_000_002, wrong).is_err());
}

#[test]
fn bloom_filter() {
    for with_bloom in [true, false] {
        let opts = TableOptions {
            bloom_false_positive: if with_bloom { 0.01 } else { 0.0 },
            ..Default::default()
        };
        let t = build_test_table("p", 1000, opts);
        assert_eq!(t.has_bloom_filter(), with_bloom);
        for reversed in [false, true] {
            let mut it = t.iter(reversed, true);
            let mut count = 0;
            it.rewind();
            while it.valid() {
                assert!(!t.does_not_have(bloom::hash(parse_key(it.key()))));
                count += 1;
                it.next();
            }
            assert_eq!(count, 1000);
        }
        assert_eq!(t.does_not_have(bloom::hash(b"foo")), with_bloom);
    }
}

#[test]
fn empty_builder() {
    let b = TableBuilder::new(TableOptions {
        bloom_false_positive: 0.1,
        ..Default::default()
    });
    assert!(b.is_empty());
    assert_eq!(b.finish().unwrap(), Vec::<u8>::new());
}

#[test]
fn max_version() {
    let mut b = TableBuilder::new(test_opts());
    for i in 0..1000u64 {
        b.add(
            &key_with_ts(format!("foo:{i}").as_bytes(), i + 1),
            ValueStruct::default(),
            0,
        );
    }
    let t = Table::create(temp_path(), b).unwrap();
    t.delete_on_drop();
    assert_eq!(t.max_version(), 1000);
}

#[test]
fn in_memory_table_block_error_is_not_a_panic() {
    let opts = test_opts();
    let src = build_test_table("key", 500, opts.clone());
    assert!(src.num_blocks() > 1, "need a block besides the last one");
    let mut data = fs::read(src.filename()).unwrap();
    let first_block_len = src.block_offset(0).unwrap().1;
    for b in &mut data[..first_block_len.min(256)] {
        *b = 0xff;
    }
    let t = Table::open_in_memory(data, src.id() + 1_000_000, opts).unwrap();
    assert!(t.block(0, false).is_err());
    assert_eq!(t.filename(), id_to_filename(src.id() + 1_000_000));
}

#[test]
fn does_not_have_from_threads() {
    let t = build_test_table("key", 10000, test_opts());
    std::thread::scope(|s| {
        for _ in 0..5 {
            s.spawn(|| assert!(t.does_not_have(1237882)));
        }
    });
}

/// Port of Go's TestTableIndex: block first keys and max version for every
/// combination of encryption and compression.
#[test]
fn table_index() {
    let data_key = Arc::new(crate::pb::DataKey {
        data: (0..32).collect(),
        ..Default::default()
    });
    let base = TableOptions {
        block_size: 4 * 1024,
        bloom_false_positive: 0.01,
        table_size: 30 << 20,
        ..Default::default()
    };
    let cases = [
        ("no encryption/compression", base.clone()),
        (
            "only encryption",
            TableOptions {
                data_key: Some(data_key.clone()),
                index_cache: Some(new_index_cache(1 << 20)),
                ..base.clone()
            },
        ),
        (
            "only compression",
            TableOptions {
                compression: CompressionType::Zstd,
                zstd_compression_level: 3,
                ..base.clone()
            },
        ),
        (
            "both",
            TableOptions {
                compression: CompressionType::Zstd,
                zstd_compression_level: 3,
                data_key: Some(data_key),
                index_cache: Some(new_index_cache(1 << 20)),
                ..base
            },
        ),
    ];
    let keys_count = 100_000;
    for (name, opts) in cases {
        let mut b = TableBuilder::new(opts.clone());
        let mut first_keys = Vec::new();
        for i in 0..keys_count {
            let k = key_with_ts(format!("{i:016x}").as_bytes(), i + 1);
            let v = i.to_string();
            let vs = ValueStruct {
                value: v.as_bytes(),
                ..Default::default()
            };
            if i == 0 || b.should_finish_block(&k, &vs) {
                first_keys.push(k.clone());
            }
            b.add(&k, vs, 0);
        }
        let t = Table::create(temp_path(), b).unwrap();
        t.delete_on_drop();
        if opts.data_key.is_none() {
            assert_eq!(t.key_id(), 0);
        }
        assert_eq!(t.num_blocks(), first_keys.len(), "{name}");
        for (i, want) in first_keys.iter().enumerate() {
            assert_eq!(&t.block_key(i).unwrap(), want, "{name}: block {i}");
        }
        assert_eq!(t.max_version(), keys_count, "{name}");
    }
}

#[test]
fn delete_on_drop_removes_the_file() {
    let t = build_test_table("key", 10, test_opts());
    let path = PathBuf::from(t.filename());
    assert!(path.exists());
    drop(t);
    assert!(!path.exists());

    let keep = Table::create(temp_path(), {
        let mut b = TableBuilder::new(test_opts());
        b.add(&key_with_ts(b"k", 1), ValueStruct::default(), 0);
        b
    })
    .unwrap();
    let path = PathBuf::from(keep.filename());
    drop(keep);
    assert!(path.exists(), "tables are kept unless marked for deletion");
}

#[test]
fn block_cache_serves_repeat_reads() {
    let cache = new_block_cache(64 << 20);
    let opts = TableOptions {
        block_cache: Some(cache.clone()),
        ..test_opts()
    };
    let t = build_test_table("key", 5000, opts);
    let mut it = t.iter(false, true);
    it.rewind();
    while it.valid() {
        it.next();
    }
    assert_eq!(cache.len(), t.num_blocks());
    let b1 = t.block(0, true).unwrap();
    let b2 = t.block(0, true).unwrap();
    assert!(Arc::ptr_eq(&b1, &b2));
}

// ---------------------------------------------------------------------------
// Merge iterator tests over in-memory iterators (Go's SimpleIterator).

struct SimpleIterator {
    keys: Vec<Vec<u8>>,
    vals: Vec<Vec<u8>>,
    idx: isize,
    reversed: bool,
    drops: Arc<AtomicUsize>,
}

impl SimpleIterator {
    fn new(keys: &[&str], vals: &[&str], reversed: bool, drops: &Arc<AtomicUsize>) -> Box<Self> {
        Box::new(SimpleIterator {
            keys: keys.iter().map(|k| key_with_ts(k.as_bytes(), 0)).collect(),
            vals: vals.iter().map(|v| v.as_bytes().to_vec()).collect(),
            idx: -1,
            reversed,
            drops: Arc::clone(drops),
        })
    }
}

impl Drop for SimpleIterator {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

impl KvIterator for SimpleIterator {
    fn next(&mut self) {
        self.idx += if self.reversed { -1 } else { 1 };
    }
    fn rewind(&mut self) {
        self.idx = if self.reversed {
            self.keys.len() as isize - 1
        } else {
            0
        };
    }
    fn seek(&mut self, key: &[u8]) {
        let key = key_with_ts(key, 0);
        self.idx = if !self.reversed {
            self.keys.partition_point(|k| compare_keys(k, &key).is_lt()) as isize
        } else {
            let from_end = self
                .keys
                .iter()
                .rev()
                .take_while(|k| compare_keys(k, &key).is_gt())
                .count();
            self.keys.len() as isize - 1 - from_end as isize
        };
    }
    fn key(&self) -> &[u8] {
        &self.keys[self.idx as usize]
    }
    fn value(&self) -> ValueStruct<'_> {
        ValueStruct {
            value: &self.vals[self.idx as usize],
            user_meta: 55,
            ..Default::default()
        }
    }
    fn valid(&self) -> bool {
        self.idx >= 0 && (self.idx as usize) < self.keys.len()
    }
}

type Iters = Vec<Box<dyn KvIterator>>;

fn strs(v: &[(String, String)]) -> (Vec<&str>, Vec<&str>) {
    v.iter().map(|(k, v)| (k.as_str(), v.as_str())).unzip()
}

/// Merges, rewinds (or seeks), collects; checks every iterator was dropped.
fn run_merge(
    iters: Iters,
    reverse: bool,
    seek: Option<&str>,
    drops: &Arc<AtomicUsize>,
) -> Vec<(String, String)> {
    let n = iters.len();
    drops.store(0, Ordering::SeqCst);
    let mut it = MergeIterator::merge(iters, reverse).unwrap();
    match seek {
        None => it.rewind(),
        Some(k) => it.seek(k.as_bytes()),
    }
    let out = collect(it.as_mut());
    drop(it);
    assert_eq!(drops.load(Ordering::SeqCst), n, "every iterator is dropped");
    out
}

#[test]
fn merge_single_and_nested() {
    let d = Arc::new(AtomicUsize::new(0));
    let keys = ["1", "2", "3"];
    let vals = ["v1", "v2", "v3"];
    let out = run_merge(
        vec![SimpleIterator::new(&keys, &vals, false, &d)],
        false,
        None,
        &d,
    );
    assert_eq!(strs(&out), (keys.to_vec(), vals.to_vec()));

    let out = run_merge(
        vec![SimpleIterator::new(&keys, &vals, true, &d)],
        true,
        None,
        &d,
    );
    assert_eq!(strs(&out), (vec!["3", "2", "1"], vec!["v3", "v2", "v1"]));

    let inner =
        MergeIterator::merge(vec![SimpleIterator::new(&keys, &vals, false, &d)], false).unwrap();
    let out = run_merge(vec![inner], false, None, &d);
    assert_eq!(strs(&out), (keys.to_vec(), vals.to_vec()));
}

fn four(reversed: bool, d: &Arc<AtomicUsize>, first: (&[&str], &[&str])) -> Iters {
    vec![
        SimpleIterator::new(first.0, first.1, reversed, d),
        SimpleIterator::new(&["2", "3", "5"], &["b2", "b3", "b5"], reversed, d),
        SimpleIterator::new(&["1"], &["c1"], reversed, d),
        SimpleIterator::new(&["1", "7", "9"], &["d1", "d7", "d9"], reversed, d),
    ]
}

#[test]
fn merge_more() {
    let d = Arc::new(AtomicUsize::new(0));
    let plain: (&[&str], &[&str]) = (&["1", "3", "7"], &["a1", "a3", "a7"]);
    let dups: (&[&str], &[&str]) = (&["1", "1", "3", "7"], &["a1", "a1-1", "a3", "a7"]);

    let fwd = (
        vec!["1", "2", "3", "5", "7", "9"],
        vec!["a1", "b2", "a3", "b5", "a7", "d9"],
    );
    assert_eq!(
        strs(&run_merge(four(false, &d, plain), false, None, &d)),
        fwd
    );
    assert_eq!(
        strs(&run_merge(four(false, &d, dups), false, None, &d)),
        fwd
    );

    let rev = (
        vec!["9", "7", "5", "3", "2", "1"],
        vec!["d9", "a7", "b5", "a3", "b2", "a1"],
    );
    assert_eq!(strs(&run_merge(four(true, &d, plain), true, None, &d)), rev);
    let rev_dups = (
        vec!["9", "7", "5", "3", "2", "1"],
        vec!["d9", "a7", "b5", "a3", "b2", "a1-1"],
    );
    assert_eq!(
        strs(&run_merge(four(true, &d, dups), true, None, &d)),
        rev_dups
    );
}

#[test]
fn merge_seek() {
    let d = Arc::new(AtomicUsize::new(0));
    let plain: (&[&str], &[&str]) = (&["1", "3", "7"], &["a1", "a3", "a7"]);
    let out = run_merge(four(false, &d, plain), false, Some("4"), &d);
    assert_eq!(strs(&out), (vec!["5", "7", "9"], vec!["b5", "a7", "d9"]));
    let out = run_merge(four(true, &d, plain), true, Some("5"), &d);
    assert_eq!(
        strs(&out),
        (vec!["5", "3", "2", "1"], vec!["b5", "a3", "b2", "a1"])
    );
    assert!(run_merge(four(false, &d, plain), false, Some("f"), &d).is_empty());
    assert!(run_merge(four(true, &d, plain), true, Some("0"), &d).is_empty());
}

#[test]
fn merge_iterator_duplicate() {
    let d = Arc::new(AtomicUsize::new(0));
    let it1 = |r| SimpleIterator::new(&["0", "1", "2"], &["a0", "a1", "a2"], r, &d);
    let it2 = |r| SimpleIterator::new(&["1", "3"], &["b1", "b3"], r, &d);
    let it3 = |r| SimpleIterator::new(&["0", "1", "2"], &["c0", "c1", "c2"], r, &d);

    let out = run_merge(vec![it1(false), it3(false)], false, None, &d);
    assert_eq!(strs(&out), (vec!["0", "1", "2"], vec!["a0", "a1", "a2"]));
    let out = run_merge(vec![it3(false), it2(false), it1(false)], false, None, &d);
    assert_eq!(
        strs(&out),
        (vec!["0", "1", "2", "3"], vec!["c0", "c1", "c2", "b3"])
    );
    let out = run_merge(vec![it3(true), it2(true), it1(true)], true, None, &d);
    assert_eq!(
        strs(&out),
        (vec!["3", "2", "1", "0"], vec!["b3", "c2", "c1", "c0"])
    );

    let out = run_merge(
        vec![
            SimpleIterator::new(&["2"], &["2"], false, &d),
            SimpleIterator::new(&["1"], &["1"], false, &d),
            SimpleIterator::new(&["0", "1", "2"], &["0", "1", "2"], false, &d),
        ],
        false,
        None,
        &d,
    );
    assert_eq!(strs(&out).0, vec!["0", "1", "2"]);
}

#[test]
fn merge_duplicates() {
    let d = Arc::new(AtomicUsize::new(0));
    let iters = |r| -> Iters {
        vec![
            SimpleIterator::new(&["1", "1", "1"], &["a1", "a3", "a7"], r, &d),
            SimpleIterator::new(&["1", "1", "1"], &["b2", "b3", "b5"], r, &d),
            SimpleIterator::new(&["1"], &["c1"], r, &d),
            SimpleIterator::new(&["1", "1", "2"], &["d1", "d7", "d9"], r, &d),
        ]
    };
    assert_eq!(
        strs(&run_merge(iters(false), false, None, &d)),
        (vec!["1", "2"], vec!["a1", "d9"])
    );
    assert_eq!(
        strs(&run_merge(iters(true), true, None, &d)),
        (vec!["2", "1"], vec!["d9", "a7"])
    );
}
