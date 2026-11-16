//! Iterating over a transaction's view of the DB, and [`Item`]s. Port of Go's
//! `iterator.go`.
//!
//! An [`Iterator`] merges the memtables and every level into one stream of
//! internal keys (newest version of each key first), then hides versions
//! above the read timestamp, internal `!badger!` keys, banned namespaces and
//! (unless `all_versions`) older versions and deleted or expired keys.
//!
//! Differences from Go:
//! - No value prefetching yet (`prefetch_values` is ignored; values are read
//!   on demand by [`Item::value`]). It comes with Phase 7.
//! - `Close` is `Drop`, and the borrow checker replaces Go's "unclosed
//!   iterator" panic in `Txn.Discard`.

use std::time::{SystemTime, UNIX_EPOCH};

use crate::db::DbInner;
use crate::error::Result;
use crate::levels::BoxedIter;
use crate::structs::{BIT_DELETE, BIT_DISCARD_EARLIER_VERSIONS, BIT_VALUE_POINTER, ValuePointer};
use crate::table::{MergeIterator, Table};
use crate::txn::Txn;
use crate::util::bloom;
use crate::util::keys::{key_with_ts, parse_key, parse_ts, same_key};
use crate::util::value::ValueStruct;

/// Prefix of Badger's internal keys.
pub(crate) const BADGER_PREFIX: &[u8] = b"!badger!";

/// Whether an entry with this meta and expiry is invisible to reads.
pub(crate) fn is_deleted_or_expired(meta: u8, expires_at: u64) -> bool {
    if meta & BIT_DELETE != 0 {
        return true;
    }
    if expires_at == 0 {
        return false;
    }
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    expires_at <= now
}

/// A key-value pair returned by [`Txn::get`] or an [`Iterator`].
///
/// The value is read lazily by [`Item::value`] (from the value log if the
/// value was stored there).
#[derive(Clone)]
pub struct Item<'a> {
    pub(crate) key: Vec<u8>,
    /// The value, or an encoded [`ValuePointer`] if `meta` says so.
    pub(crate) vptr: Vec<u8>,
    pub(crate) version: u64,
    pub(crate) expires_at: u64,
    pub(crate) meta: u8,
    pub(crate) user_meta: u8,
    pub(crate) db: &'a DbInner,
}

impl std::fmt::Debug for Item<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "key={:?}, version={}, meta={:x}",
            String::from_utf8_lossy(&self.key),
            self.version,
            self.meta
        )
    }
}

impl<'a> Item<'a> {
    pub(crate) fn new(db: &'a DbInner, key: Vec<u8>, version: u64, vs: ValueStruct<'_>) -> Self {
        Item {
            key,
            vptr: vs.value.to_vec(),
            version,
            expires_at: vs.expires_at,
            meta: vs.meta,
            user_meta: vs.user_meta,
            db,
        }
    }

    /// The key, without its version.
    pub fn key(&self) -> &[u8] {
        &self.key
    }

    pub fn version(&self) -> u64 {
        self.version
    }

    /// Calls `f` with the value and returns its result. The slice may point
    /// into a memory-mapped file and is only valid during the call; use
    /// [`Item::value_copy`] to keep it.
    pub fn value<R>(&self, f: impl FnOnce(&[u8]) -> R) -> Result<R> {
        if self.meta & BIT_VALUE_POINTER == 0 {
            return Ok(f(&self.vptr));
        }
        let vp = ValuePointer::decode(&self.vptr)?;
        self.db.vlog.read(vp, f).inspect_err(|e| {
            log::error!(
                "Unable to read: Key: {:?}, Version : {}, meta: {}, userMeta: {} Error: {e}",
                self.key,
                self.version,
                self.meta,
                self.user_meta
            )
        })
    }

    /// A copy of the value.
    pub fn value_copy(&self) -> Result<Vec<u8>> {
        self.value(<[u8]>::to_vec)
    }

    pub fn user_meta(&self) -> u8 {
        self.user_meta
    }

    /// Unix time in seconds when the entry expires; 0 = never.
    pub fn expires_at(&self) -> u64 {
        self.expires_at
    }

    /// Whether the entry is a delete marker or has expired. Only seen when
    /// iterating with `all_versions`.
    pub fn is_deleted_or_expired(&self) -> bool {
        is_deleted_or_expired(self.meta, self.expires_at)
    }

    /// Whether earlier versions of this key may be discarded.
    pub fn discard_earlier_versions(&self) -> bool {
        self.meta & BIT_DISCARD_EARLIER_VERSIONS != 0
    }

    fn has_value(&self) -> bool {
        self.meta != 0 || !self.vptr.is_empty()
    }

    /// Approximate size of the key and value on disk.
    pub fn estimated_size(&self) -> u64 {
        if !self.has_value() {
            return 0;
        }
        if self.meta & BIT_VALUE_POINTER == 0 {
            return (self.key.len() + self.vptr.len()) as u64;
        }
        // Includes the key length.
        ValuePointer::decode(&self.vptr).map_or(0, |vp| vp.len as u64)
    }

    pub fn key_size(&self) -> u64 {
        self.key.len() as u64
    }

    /// Size of the value, without reading it from the value log.
    pub fn value_size(&self) -> u64 {
        if !self.has_value() {
            return 0;
        }
        if self.meta & BIT_VALUE_POINTER == 0 {
            return self.vptr.len() as u64;
        }
        let Ok(vp) = ValuePointer::decode(&self.vptr) else {
            return 0;
        };
        // Go's estimate: key + 8-byte version, ~6 bytes of header, 4 of CRC.
        (vp.len as u64).saturating_sub(self.key.len() as u64 + 8 + 6 + 4)
    }
}

/// How to iterate (Go's `IteratorOptions`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IteratorOptions {
    /// How many items to prefetch (ignored until prefetching is ported).
    pub prefetch_size: usize,
    /// Whether to prefetch values (ignored until prefetching is ported).
    pub prefetch_values: bool,
    /// Iterate from the biggest key down.
    pub reverse: bool,
    /// Return every version of each key, including deletes and expired ones.
    pub all_versions: bool,
    /// Also return Badger's internal `!badger!` keys.
    pub internal_access: bool,
    /// Only iterate over keys with this prefix.
    pub prefix: Vec<u8>,
    /// Only return versions above this timestamp.
    pub since_ts: u64,
}

impl Default for IteratorOptions {
    /// Go's `DefaultIteratorOptions`.
    fn default() -> Self {
        IteratorOptions {
            prefetch_size: 100,
            prefetch_values: true,
            reverse: false,
            all_versions: false,
            internal_access: false,
            prefix: Vec::new(),
            since_ts: 0,
        }
    }
}

impl IteratorOptions {
    /// Compares the user key of `key` (truncated to the prefix length) with
    /// the prefix.
    fn compare_to_prefix(&self, key: &[u8]) -> std::cmp::Ordering {
        let key = parse_key(key);
        let key = &key[..key.len().min(self.prefix.len())];
        key.cmp(&self.prefix)
    }

    /// Whether an L0 table can hold keys for this iteration. With
    /// `prefix_is_key` (key iterators) the bloom filter is checked too.
    pub(crate) fn pick_table(&self, t: &Table, prefix_is_key: bool) -> bool {
        if t.max_version() < self.since_ts {
            return false;
        }
        if self.prefix.is_empty() {
            return true;
        }
        if self.compare_to_prefix(t.smallest()).is_gt() {
            return false;
        }
        if self.compare_to_prefix(t.biggest()).is_lt() {
            return false;
        }
        if prefix_is_key && t.does_not_have(bloom::hash(&self.prefix)) {
            return false;
        }
        true
    }

    /// The tables of a sorted level that can hold keys for this iteration.
    pub(crate) fn pick_tables(
        &self,
        all: &[std::sync::Arc<Table>],
        prefix_is_key: bool,
    ) -> Vec<std::sync::Arc<Table>> {
        let since =
            |t: &&std::sync::Arc<Table>| self.since_ts == 0 || t.max_version() >= self.since_ts;
        if self.prefix.is_empty() {
            return all.iter().filter(since).cloned().collect();
        }
        let start = all.partition_point(|t| self.compare_to_prefix(t.biggest()).is_lt());
        let rest = &all[start..];
        if !prefix_is_key {
            let end = rest.partition_point(|t| !self.compare_to_prefix(t.smallest()).is_gt());
            return rest[..end].iter().filter(since).cloned().collect();
        }
        let hash = bloom::hash(&self.prefix);
        rest.iter()
            .take_while(|t| !self.compare_to_prefix(t.smallest()).is_gt())
            .filter(|t| !t.does_not_have(hash))
            .filter(since)
            .cloned()
            .collect()
    }
}

/// Iterates over a transaction's view of the DB. See the module docs.
///
/// ```ignore
/// let mut it = txn.iter(IteratorOptions::default());
/// it.rewind();
/// while it.valid() {
///     let item = it.item();
///     item.value(|v| println!("{:?} = {:?}", item.key(), v))?;
///     it.next();
/// }
/// ```
pub struct Iterator<'t> {
    txn: &'t Txn<'t>,
    iitr: Option<BoxedIter>,
    read_ts: u64,
    opt: IteratorOptions,
    /// The prefix is a whole key (key iterators): match it exactly and use
    /// the bloom filters.
    prefix_is_key: bool,
    item: Option<Item<'t>>,
    /// The previous key, to skip its older versions (forward only).
    last_key: Vec<u8>,
}

impl<'t> Iterator<'t> {
    pub(crate) fn new(txn: &'t Txn<'t>, opt: IteratorOptions, prefix_is_key: bool) -> Self {
        let db = txn.db();
        let mut iters: Vec<BoxedIter> = Vec::new();
        // Pending writes of an update transaction come first (Phase 5).
        for mt in db.mem_tables() {
            iters.push(Box::new(mt.skiplist().uni_iter(opt.reverse)));
        }
        db.lc.append_iterators(&mut iters, &opt, prefix_is_key);
        Iterator {
            txn,
            iitr: MergeIterator::merge(iters, opt.reverse),
            read_ts: txn.read_ts(),
            opt,
            prefix_is_key,
            item: None,
            last_key: Vec::new(),
        }
    }

    /// The current item. Valid until the next move.
    ///
    /// # Panics
    /// If the iterator is not [`valid`](Iterator::valid).
    pub fn item(&self) -> &Item<'t> {
        self.item.as_ref().expect("iterator is not valid")
    }

    /// Whether the iterator points at an item (within the prefix, if any).
    pub fn valid(&self) -> bool {
        let Some(item) = &self.item else { return false };
        if self.prefix_is_key {
            item.key == self.opt.prefix
        } else {
            item.key.starts_with(&self.opt.prefix)
        }
    }

    /// Whether the iterator is valid and the current key starts with `prefix`.
    pub fn valid_for_prefix(&self, prefix: &[u8]) -> bool {
        self.valid() && self.item().key.starts_with(prefix)
    }

    /// Moves to the next item.
    pub fn next(&mut self) {
        self.item = None;
        self.advance();
    }

    /// Parses entries until one becomes the current item, or the underlying
    /// iterator ends (Go's `prefetch` / the loop in `Next`).
    fn advance(&mut self) {
        while self.iitr.as_ref().is_some_and(|i| i.valid()) && self.has_prefix() {
            if self.parse_item() {
                break;
            }
        }
    }

    /// In forward mode with a prefix, whether the underlying iterator is
    /// still within it (so iteration can stop early).
    fn has_prefix(&self) -> bool {
        if !self.opt.reverse && !self.opt.prefix.is_empty() {
            let key = self.iitr.as_ref().unwrap().key();
            return parse_key(key).starts_with(&self.opt.prefix);
        }
        true
    }

    /// Looks at the current entry of the underlying iterator and advances it.
    /// Returns true if an item was produced (Go's `parseItem`).
    fn parse_item(&mut self) -> bool {
        let db = self.txn.db();
        let mi = self.iitr.as_mut().unwrap();
        let key = mi.key();

        // Skip internal keys, unless asked for them.
        let is_internal = key.starts_with(BADGER_PREFIX);
        if !self.opt.internal_access && is_internal {
            mi.next();
            return false;
        }
        // Skip versions newer than the read, or not newer than since_ts.
        let version = parse_ts(key);
        if version > self.read_ts || (self.opt.since_ts > 0 && version <= self.opt.since_ts) {
            mi.next();
            return false;
        }
        if !is_internal && db.is_banned(key).is_err() {
            mi.next();
            return false;
        }

        if self.opt.all_versions {
            self.item = Some(Item::new(db, parse_key(key).to_vec(), version, mi.value()));
            mi.next();
            return true;
        }

        // Forward: only the first (newest) version of each key counts.
        if !self.opt.reverse {
            if same_key(&self.last_key, key) {
                mi.next();
                return false;
            }
            self.last_key.clear();
            self.last_key.extend_from_slice(key);
        }

        // Reverse iteration sees a key's versions oldest first, so keep
        // taking newer visible versions of the same key; the newest decides.
        loop {
            let key = mi.key();
            let vs = mi.value();
            if is_deleted_or_expired(vs.meta, vs.expires_at) {
                mi.next();
                return false;
            }
            let item = Item::new(db, parse_key(key).to_vec(), parse_ts(key), vs);
            mi.next();
            if !self.opt.reverse || !mi.valid() {
                self.item = Some(item);
                return true;
            }
            let next_ts = parse_ts(mi.key());
            if !(next_ts <= self.read_ts && parse_key(mi.key()) == item.key.as_slice()) {
                self.item = Some(item);
                return true;
            }
        }
    }

    /// Moves to the first key at or after `key` (at or before it, if
    /// reversed). An empty key means the prefix, or the start.
    pub fn seek(&mut self, key: &[u8]) {
        self.item = None;
        self.last_key.clear();
        let key = if key.is_empty() {
            self.opt.prefix.clone()
        } else {
            key.to_vec()
        };
        let Some(iitr) = self.iitr.as_mut() else {
            return;
        };
        if key.is_empty() {
            iitr.rewind();
        } else {
            let ts = if self.opt.reverse { 0 } else { self.read_ts };
            iitr.seek(&key_with_ts(&key, ts));
        }
        self.advance();
    }

    /// Moves to the first item (the last one, if reversed).
    pub fn rewind(&mut self) {
        self.seek(&[]);
    }
}

#[cfg(test)]
mod tests {
    // Ports of Go's TestPickTables and TestPickSortTables.
    use std::sync::Arc;

    use super::*;
    use crate::table::{TableBuilder, TableOptions};

    fn table(id: u64, keys: &[&str]) -> Arc<Table> {
        let opts = TableOptions {
            block_size: 4096,
            bloom_false_positive: 0.01,
            ..Default::default()
        };
        let mut b = TableBuilder::new(opts.clone());
        for k in keys {
            let vs = ValueStruct {
                value: b"some value",
                ..Default::default()
            };
            b.add(&key_with_ts(k.as_bytes(), 1), vs, 0);
        }
        Table::open_in_memory(b.finish().unwrap(), id, opts).unwrap()
    }

    #[test]
    fn pick_table() {
        let mut opt = IteratorOptions::default();
        let mut check = |prefix: &[u8], left: &[u8], right: &[u8], want: bool| {
            opt.prefix = prefix.to_vec();
            let t = table(
                1,
                &[
                    std::str::from_utf8(left).unwrap(),
                    std::str::from_utf8(right).unwrap(),
                ],
            );
            assert_eq!(
                opt.pick_table(&t, false),
                want,
                "{prefix:?} {left:?} {right:?}"
            );
        };
        check(b"abc", b"ab", b"ad", true);
        check(b"abc", b"abc", b"ad", true);
        check(b"abc", b"abb123", b"ad", true);
        check(b"abc", b"abc123", b"abd234", true);
        check(b"abc", b"abc123", b"abc456", true);
        // Regression test for Go issue #992.
        check(&[0, 0, 1], &[0], &[0, 0, 1], true);

        check(b"abd", b"abe", b"ad", false);
        check(b"abd", b"ac", b"ad", false);
        check(b"abd", b"b", b"e", false);
        check(b"abd", b"a", b"ab", false);
        check(b"abd", b"ab", b"abc", false);
        check(b"abd", b"ab", b"abc123", false);
    }

    #[test]
    fn pick_sort_tables() {
        let gen_tables = |ranges: &[(&str, &str)]| -> Vec<Arc<Table>> {
            ranges
                .iter()
                .enumerate()
                .map(|(i, (s, l))| table(i as u64 + 1, &[s, l]))
                .collect()
        };
        let small = |t: &Arc<Table>| parse_key(t.smallest()).to_vec();
        let mut opt = IteratorOptions {
            prefix: b"c".to_vec(),
            ..Default::default()
        };

        let tables = gen_tables(&[
            ("a", "abc"),
            ("abcd", "cde"),
            ("cge", "chf"),
            ("glr", "gyup"),
        ]);
        let f = opt.pick_tables(&tables, false);
        assert_eq!(
            f.iter().map(small).collect::<Vec<_>>(),
            [b"abcd".to_vec(), b"cge".to_vec()]
        );

        let tables = gen_tables(&[
            ("a", "abc"),
            ("abcd", "ade"),
            ("cge", "chf"),
            ("glr", "gyup"),
        ]);
        let f = opt.pick_tables(&tables, false);
        assert_eq!(f.iter().map(small).collect::<Vec<_>>(), [b"cge".to_vec()]);

        let tables = gen_tables(&[
            ("a", "abc"),
            ("abcd", "ade"),
            ("cge", "chf"),
            ("ckr", "cyup"),
            ("csfr", "gyup"),
        ]);
        let f = opt.pick_tables(&tables, false);
        assert_eq!(
            f.iter().map(small).collect::<Vec<_>>(),
            [b"cge".to_vec(), b"ckr".to_vec(), b"csfr".to_vec()]
        );

        opt.prefix = b"aa".to_vec();
        let f = opt.pick_tables(&tables, false);
        assert_eq!(small(&f[0]), b"a");
        assert_eq!(parse_key(f[0].biggest()), b"abc");

        // since_ts drops tables with only older versions (all are at 1).
        opt.prefix.clear();
        opt.since_ts = 2;
        assert!(opt.pick_tables(&tables, false).is_empty());
        assert!(!opt.pick_table(&tables[0], false));
    }

    #[test]
    fn deleted_or_expired() {
        assert!(is_deleted_or_expired(BIT_DELETE, 0));
        assert!(!is_deleted_or_expired(0, 0));
        assert!(is_deleted_or_expired(0, 1));
        assert!(!is_deleted_or_expired(0, u64::MAX));
    }
}
