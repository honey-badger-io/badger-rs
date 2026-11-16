//! Lock-free arena skiplist: the in-memory sorted structure behind a memtable.
//! Port of Go's `skl` package (itself adapted from RocksDB's inline skiplist).
//!
//! - Keys are internal keys (user key + version) ordered by [`compare_keys`].
//! - Nodes, keys and values live in one preallocated [`arena::Arena`]; nodes
//!   point at each other by `u32` offset. Nothing is freed until the whole
//!   skiplist is dropped, which makes lock-free reads safe without epochs or
//!   hazard pointers.
//! - Any number of threads may [`Skiplist::put`] and read concurrently. A put
//!   links the new node bottom-up with one CAS per level; a put of an existing
//!   key swaps the node's value word instead (values are never updated in
//!   place, a new copy is appended).
//!
//! Differences from Go:
//! - Reference counting (`IncrRef`/`DecrRef`, `OnClose`) is `Arc<Skiplist>`;
//!   iterators hold an `Arc`. The memtable deletes its WAL in its own `Drop`.
//! - [`Skiplist::get`] returns `Option` instead of a zero `ValueStruct`.
//! - Atomics use acquire/release ordering (Go's are sequentially consistent):
//!   a node's bytes are published by the release CAS that links it, and a
//!   value by the release store of the node's value word.
//! - Keys longer than `u16::MAX` panic (Go silently truncates the size).

mod arena;
mod sync;

#[cfg(test)]
mod tests;

#[cfg(all(test, loom))]
mod loom_tests;

use std::cmp::Ordering as CmpOrdering;
use std::sync::Arc;

pub use arena::MAX_NODE_SIZE;
use arena::{Arena, MAX_HEIGHT, decode_value, encode_value};
use sync::{AtomicI32, Ordering};

use crate::util::KvIterator;
use crate::util::keys::{compare_keys, parse_ts, same_key};
use crate::util::value::ValueStruct;

/// A tower grows one more level with probability 1/3 (Go's `heightIncrease`).
const HEIGHT_INCREASE: u32 = u32::MAX / 3;

/// Offset 0 is the nil node.
const NIL: u32 = 0;

/// See the module docs.
pub struct Skiplist {
    /// Current height, `1..=MAX_HEIGHT`. Only grows.
    height: AtomicI32,
    head: u32,
    arena: Arena,
}

impl Skiplist {
    /// An empty skiplist whose arena holds `arena_size` bytes. Inserting more
    /// than fits panics, so size it for the largest memtable plus one batch
    /// (see `memtable::arena_size`).
    pub fn new(arena_size: usize) -> Skiplist {
        let arena = Arena::new(arena_size);
        let head = new_node(&arena, &[], &ValueStruct::default(), MAX_HEIGHT);
        Skiplist {
            height: AtomicI32::new(1),
            head,
            arena,
        }
    }

    fn height(&self) -> usize {
        // Relaxed: a stale height only means starting the search lower; the
        // head's links at any level are valid (nil or a published node).
        self.height.load(Ordering::Relaxed) as usize
    }

    /// The node after `node` at level `h`, or [`NIL`].
    fn next(&self, node: u32, h: usize) -> u32 {
        // Acquire pairs with the release CAS that linked the next node, so its
        // bytes are visible.
        self.arena.tower(node, h).load(Ordering::Acquire)
    }

    fn key(&self, node: u32) -> &[u8] {
        self.arena.node_key(node)
    }

    fn value(&self, node: u32) -> ValueStruct<'_> {
        // Acquire pairs with the release store in `set_value` / the link CAS.
        let (off, size) = decode_value(self.arena.value_word(node).load(Ordering::Acquire));
        self.arena.val(off, size)
    }

    fn value_word(&self, node: u32) -> u64 {
        self.arena.value_word(node).load(Ordering::Acquire)
    }

    fn set_value(&self, node: u32, v: &ValueStruct<'_>) {
        let word = encode_value(self.arena.put_val(v), v.encoded_size());
        // Release publishes the value bytes just written.
        self.arena.value_word(node).store(word, Ordering::Release);
    }

    fn random_height(&self, _key: &[u8]) -> usize {
        // Under loom the height must not depend on state that differs between
        // explored executions, so derive it from the key.
        #[cfg(loom)]
        let mut rand = {
            let mut x = xxhash_rust::xxh64::xxh64(_key, 0);
            move || {
                x = x.wrapping_mul(0x9e37_79b9_7f4a_7c15).rotate_left(17);
                (x >> 32) as u32
            }
        };
        #[cfg(not(loom))]
        let rand = fast_rand;
        let mut h = 1;
        while h < MAX_HEIGHT && rand() <= HEIGHT_INCREASE {
            h += 1;
        }
        h
    }

    /// Finds the node near `key` (Go's `findNear`).
    ///
    /// With `less`, the rightmost node with key `<` (or `<=` if `allow_equal`)
    /// `key`; otherwise the leftmost node with key `>` (or `>=`). Never returns
    /// the head. The bool is true if the node's key equals `key`.
    fn find_near(&self, key: &[u8], less: bool, allow_equal: bool) -> (Option<u32>, bool) {
        let not_head = |x: u32| (x != self.head).then_some(x);
        let mut x = self.head;
        let mut level = self.height() - 1;
        loop {
            // Invariant: x.key < key.
            let next = self.next(x, level);
            if next == NIL {
                // x.key < key < end of list.
                if level > 0 {
                    level -= 1;
                    continue;
                }
                return if less {
                    (not_head(x), false)
                } else {
                    (None, false)
                };
            }
            match compare_keys(key, self.key(next)) {
                CmpOrdering::Greater => x = next,
                CmpOrdering::Equal => {
                    if allow_equal {
                        return (Some(next), true);
                    }
                    if !less {
                        // Want >: the next node on the base level.
                        let after = self.next(next, 0);
                        return ((after != NIL).then_some(after), false);
                    }
                    // Want <: get closer on a lower level.
                    if level > 0 {
                        level -= 1;
                        continue;
                    }
                    return (not_head(x), false);
                }
                CmpOrdering::Less => {
                    // x.key < key < next.key.
                    if level > 0 {
                        level -= 1;
                        continue;
                    }
                    return if less {
                        (not_head(x), false)
                    } else {
                        (Some(next), false)
                    };
                }
            }
        }
    }

    /// Returns `(before, after)` at `level` with `before.key < key < after.key`,
    /// starting the search at `before` (whose key must be `< key`). If a node
    /// with `key` exists, returns it as both.
    fn find_splice_for_level(&self, key: &[u8], mut before: u32, level: usize) -> (u32, u32) {
        loop {
            let next = self.next(before, level);
            if next == NIL {
                return (before, next);
            }
            match compare_keys(key, self.key(next)) {
                CmpOrdering::Equal => return (next, next),
                CmpOrdering::Less => return (before, next),
                CmpOrdering::Greater => before = next,
            }
        }
    }

    /// Inserts `key` with value `v`, or replaces the value if `key` exists.
    /// Both are copied into the arena.
    ///
    /// # Panics
    /// If the arena is full, or the key is longer than `u16::MAX`.
    pub fn put(&self, key: &[u8], v: ValueStruct<'_>) {
        assert!(key.len() <= u16::MAX as usize, "skiplist key too long");
        // Find the splice at each level. An existing key only needs a new value.
        let list_height = self.height();
        let mut prev = [NIL; MAX_HEIGHT + 1];
        let mut next = [NIL; MAX_HEIGHT + 1];
        prev[list_height] = self.head;
        for i in (0..list_height).rev() {
            // Start from the level above to skip ahead.
            (prev[i], next[i]) = self.find_splice_for_level(key, prev[i + 1], i);
            if prev[i] == next[i] {
                self.set_value(prev[i], &v);
                return;
            }
        }

        let height = self.random_height(key);
        let x = new_node(&self.arena, key, &v, height);

        // Raise the list height if needed.
        let mut list_height = self.height.load(Ordering::Relaxed);
        while height as i32 > list_height {
            match self.height.compare_exchange(
                list_height,
                height as i32,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(h) => list_height = h,
            }
        }

        // Link from the base level up. Once linked on level 0, x is visible, so
        // no other put can insert the same key on a higher level.
        for i in 0..height {
            loop {
                if prev[i] == NIL {
                    // Above the old list height: the splice was not computed.
                    // These levels are sparse, so search from the head.
                    assert!(i > 1, "cannot happen on the base level");
                    (prev[i], next[i]) = self.find_splice_for_level(key, self.head, i);
                    // The same key can only appear on the base level first.
                    assert_ne!(prev[i], next[i]);
                }
                // Relaxed: published by the release CAS below. (For i > 0, x is
                // already reachable on lower levels, but nobody reads x's link at
                // level i until x is linked at level i.)
                self.arena.tower(x, i).store(next[i], Ordering::Relaxed);
                if self
                    .arena
                    .tower(prev[i], i)
                    .compare_exchange(next[i], x, Ordering::Release, Ordering::Relaxed)
                    .is_ok()
                {
                    break;
                }
                // Someone inserted between prev and next: recompute the splice.
                (prev[i], next[i]) = self.find_splice_for_level(key, prev[i], i);
                if prev[i] == next[i] {
                    assert_eq!(i, 0, "equality can happen only on the base level");
                    // Another put inserted the same key first; x is wasted space.
                    self.set_value(prev[i], &v);
                    return;
                }
            }
        }
    }

    /// Whether the skiplist has no entries.
    pub fn is_empty(&self) -> bool {
        self.find_last().is_none()
    }

    /// The last node, never the head.
    fn find_last(&self) -> Option<u32> {
        let mut n = self.head;
        let mut level = self.height() - 1;
        loop {
            let next = self.next(n, level);
            if next != NIL {
                n = next;
            } else if level == 0 {
                return (n != self.head).then_some(n);
            } else {
                level -= 1;
            }
        }
    }

    /// The value of the first entry at or after `key` with the same user key,
    /// i.e. the newest version at or below `key`'s version. `version` is set
    /// from the found key.
    pub fn get(&self, key: &[u8]) -> Option<ValueStruct<'_>> {
        let (n, _) = self.find_near(key, false, true);
        let n = n?;
        let found = self.key(n);
        if !same_key(key, found) {
            return None;
        }
        let mut vs = self.value(n);
        vs.version = parse_ts(found);
        Some(vs)
    }

    /// Bytes used in the arena (Go's `MemSize`).
    pub fn mem_size(&self) -> u64 {
        self.arena.size()
    }

    /// A bidirectional iterator. It keeps the skiplist alive.
    pub fn iter(self: &Arc<Self>) -> Iter {
        Iter {
            list: Arc::clone(self),
            n: NIL,
        }
    }

    /// A one-direction iterator implementing [`KvIterator`].
    pub fn uni_iter(self: &Arc<Self>, reversed: bool) -> UniIter {
        UniIter {
            iter: self.iter(),
            reversed,
        }
    }
}

/// Allocates and initialises an unlinked node.
fn new_node(arena: &Arena, key: &[u8], v: &ValueStruct<'_>, height: usize) -> u32 {
    let node = arena.put_node(height);
    let key_offset = arena.put_key(key);
    let value = encode_value(arena.put_val(v), v.encoded_size());
    arena.init_node(node, key_offset, key.len() as u16, height as u16, value);
    node
}

/// A fast, non-cryptographic per-thread random number (Go's `z.FastRand`).
#[cfg(not(loom))]
fn fast_rand() -> u32 {
    use std::cell::Cell;
    thread_local! {
        static STATE: Cell<u64> = Cell::new({
            // Seed from a per-thread address and the clock; quality is irrelevant.
            let local = 0u8;
            let addr = &local as *const u8 as u64;
            let t = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos() as u64)
                .unwrap_or(0);
            (addr ^ t.rotate_left(32)) | 1
        });
    }
    STATE.with(|s| {
        // xorshift64*
        let mut x = s.get();
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        s.set(x);
        (x.wrapping_mul(0x2545_f491_4f6c_dd1d) >> 32) as u32
    })
}

/// Bidirectional iterator over a [`Skiplist`]. Holds an `Arc` to it.
/// A new iterator is not valid until positioned.
pub struct Iter {
    list: Arc<Skiplist>,
    n: u32,
}

impl Iter {
    pub fn valid(&self) -> bool {
        self.n != NIL
    }

    /// The current key. Panics if not valid.
    pub fn key(&self) -> &[u8] {
        assert!(self.valid());
        self.list.key(self.n)
    }

    /// The current value (`version` is not set). Panics if not valid.
    pub fn value(&self) -> ValueStruct<'_> {
        assert!(self.valid());
        self.list.value(self.n)
    }

    /// The raw value word (offset and size in the arena), Go's `ValueUint64`.
    pub fn value_u64(&self) -> u64 {
        assert!(self.valid());
        self.list.value_word(self.n)
    }

    pub fn next(&mut self) {
        assert!(self.valid());
        self.n = self.list.next(self.n, 0);
    }

    pub fn prev(&mut self) {
        assert!(self.valid());
        self.n = self
            .list
            .find_near(self.key(), true, false)
            .0
            .unwrap_or(NIL);
    }

    /// Moves to the first entry with key `>= target`.
    pub fn seek(&mut self, target: &[u8]) {
        self.n = self.list.find_near(target, false, true).0.unwrap_or(NIL);
    }

    /// Moves to the last entry with key `<= target`.
    pub fn seek_for_prev(&mut self, target: &[u8]) {
        self.n = self.list.find_near(target, true, true).0.unwrap_or(NIL);
    }

    pub fn seek_to_first(&mut self) {
        self.n = self.list.next(self.list.head, 0);
    }

    pub fn seek_to_last(&mut self) {
        self.n = self.list.find_last().unwrap_or(NIL);
    }
}

/// One-direction wrapper around [`Iter`] (Go's `UniIterator`).
pub struct UniIter {
    iter: Iter,
    reversed: bool,
}

impl KvIterator for UniIter {
    fn next(&mut self) {
        if self.reversed {
            self.iter.prev()
        } else {
            self.iter.next()
        }
    }

    fn rewind(&mut self) {
        if self.reversed {
            self.iter.seek_to_last()
        } else {
            self.iter.seek_to_first()
        }
    }

    fn seek(&mut self, key: &[u8]) {
        if self.reversed {
            self.iter.seek_for_prev(key)
        } else {
            self.iter.seek(key)
        }
    }

    fn key(&self) -> &[u8] {
        self.iter.key()
    }

    fn value(&self) -> ValueStruct<'_> {
        self.iter.value()
    }

    fn valid(&self) -> bool {
        self.iter.valid()
    }
}
