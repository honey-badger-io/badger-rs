//! The arena behind a skiplist: one fixed-size, zeroed allocation that nodes,
//! keys and values are bump-allocated from. Port of Go's `skl/arena.go`.
//!
//! Everything is addressed by a `u32` offset; offset 0 is reserved as "nil"
//! (allocation starts at 1). Nothing is ever freed: the whole arena goes away
//! when the skiplist is dropped.
//!
//! # Safety model
//!
//! All `unsafe` code of the skiplist lives in this file. The arena hands out
//! three kinds of access:
//!
//! - **Fresh writes** ([`Arena::put_key`], [`Arena::put_val`], [`Arena::init_node`]):
//!   the bytes were just reserved by an atomic bump of `n`, so no other thread
//!   knows their offset yet and the writer has exclusive access.
//! - **Published reads** ([`Arena::key`], [`Arena::val`], [`Arena::node_key`]):
//!   bytes written before their offset was published with a release store or
//!   CAS (a tower link or a node's value word), and read after an acquire
//!   load of that offset. They are never written again.
//! - **Atomics** ([`Arena::tower`], [`Arena::value_word`]): aligned 4- and
//!   8-byte words inside nodes, only ever accessed atomically.
//!
//! Offsets passed in come from the skiplist, which only uses offsets it got
//! from this arena. The bounds are still checked (cheaply) in debug builds.
//!
//! Under `cfg(loom)` the atomic words live in side tables of loom atomics
//! (loom cannot model atomics placed in raw memory); the plain bytes stay in
//! the arena.

use std::alloc::{Layout, alloc_zeroed, dealloc, handle_alloc_error};
use std::ptr::NonNull;

use super::sync::{AtomicU32, AtomicU64, Ordering};
use crate::util::value::ValueStruct;

/// Maximum height of a node's tower (Go's `maxHeight`).
pub const MAX_HEIGHT: usize = 20;

/// Size of one tower link.
const OFFSET_SIZE: usize = 4;

/// Nodes are 8-byte aligned so the value word can be an `AtomicU64`.
const NODE_ALIGN: u32 = 7;

/// Node layout, identical to Go's `node` struct so arena sizes (and so
/// memtable flush points) match:
///
/// ```text
/// offset  size  field
///      0     8  value      AtomicU64: value offset (low 32 bits) | value size (high 32)
///      8     4  key_offset u32, immutable
///     12     2  key_size   u16, immutable
///     14     2  height     u16, immutable
///     16  4*20  tower      AtomicU32 links to the next node at each level
/// ```
/// Only the first `height` tower links are allocated.
const VALUE_AT: usize = 0;
const KEY_OFFSET_AT: usize = 8;
const KEY_SIZE_AT: usize = 12;
const HEIGHT_AT: usize = 14;
const TOWER_AT: usize = 16;

/// Size of a node with a full tower (Go's `MaxNodeSize`).
pub const MAX_NODE_SIZE: usize = TOWER_AT + MAX_HEIGHT * OFFSET_SIZE;

pub(crate) struct Arena {
    /// Next free offset.
    n: AtomicU32,
    buf: NonNull<u8>,
    len: usize,
    #[cfg(loom)]
    loom_u32: Box<[AtomicU32]>,
    #[cfg(loom)]
    loom_u64: Box<[AtomicU64]>,
}

// SAFETY: the arena owns its allocation. Concurrent access follows the protocol
// in the module docs: fresh regions are written by one thread only, published
// regions are read-only, and shared words are accessed atomically.
unsafe impl Send for Arena {}
// SAFETY: as above.
unsafe impl Sync for Arena {}

impl Arena {
    /// Allocates a zeroed arena of `size` bytes.
    ///
    /// # Panics
    /// If `size` does not fit in a `u32` offset, or is smaller than one node.
    pub fn new(size: usize) -> Arena {
        assert!(
            size <= u32::MAX as usize,
            "arena size {size} does not fit u32 offsets"
        );
        assert!(size > MAX_NODE_SIZE, "arena size {size} too small");
        let layout = Self::layout(size);
        // SAFETY: layout has a non-zero size.
        let ptr = unsafe { alloc_zeroed(layout) };
        let buf = NonNull::new(ptr).unwrap_or_else(|| handle_alloc_error(layout));
        Arena {
            // Offset 0 means nil, so nothing is ever stored there.
            n: AtomicU32::new(1),
            buf,
            len: size,
            #[cfg(loom)]
            loom_u32: (0..size / 4).map(|_| AtomicU32::new(0)).collect(),
            #[cfg(loom)]
            loom_u64: (0..size / 8).map(|_| AtomicU64::new(0)).collect(),
        }
    }

    fn layout(size: usize) -> Layout {
        Layout::from_size_align(size, 8).expect("arena layout")
    }

    /// Bytes allocated so far.
    pub fn size(&self) -> u64 {
        self.n.load(Ordering::Relaxed) as u64
    }

    /// Reserves `l` bytes and returns the offset of the first one.
    ///
    /// # Panics
    /// When the arena is full, like Go (the memtable is meant to be flushed
    /// well before that).
    fn alloc(&self, l: u32) -> u32 {
        // Relaxed: the reserved range is private to this thread; publishing its
        // contents is done later by release stores on the skiplist links.
        let start = self.n.fetch_add(l, Ordering::Relaxed);
        let end = start as u64 + l as u64;
        assert!(
            end <= self.len as u64,
            "Arena too small, toWrite:{l} newTotal:{end} limit:{}",
            self.len
        );
        start
    }

    /// Raw pointer to `offset`, which must be within the arena.
    fn ptr(&self, offset: usize, len: usize) -> *mut u8 {
        debug_assert!(offset + len <= self.len, "arena access out of bounds");
        // SAFETY: offset + len <= self.len (asserted in debug builds; all
        // offsets come from `alloc`, which checks the bound).
        unsafe { self.buf.as_ptr().add(offset) }
    }

    /// Allocates a node with a tower of `height` links and returns its offset.
    /// The node's fields are zero until [`Arena::init_node`].
    pub fn put_node(&self, height: usize) -> u32 {
        // Unused tower links are not allocated.
        let unused = (MAX_HEIGHT - height) * OFFSET_SIZE;
        // Pad so the node can be aligned.
        let l = (MAX_NODE_SIZE - unused) as u32 + NODE_ALIGN;
        let start = self.alloc(l);
        (start + NODE_ALIGN) & !NODE_ALIGN
    }

    /// Fills in a node returned by [`Arena::put_node`] that is not yet linked
    /// into the skiplist.
    pub fn init_node(&self, node: u32, key_offset: u32, key_size: u16, height: u16, value: u64) {
        let p = self.ptr(node as usize, TOWER_AT);
        // SAFETY: `node` was just allocated by this thread (fresh write), is
        // 8-aligned, and TOWER_AT bytes fit in the allocation.
        unsafe {
            p.add(KEY_OFFSET_AT).cast::<u32>().write(key_offset);
            p.add(KEY_SIZE_AT).cast::<u16>().write(key_size);
            p.add(HEIGHT_AT).cast::<u16>().write(height);
        }
        // Not yet visible to other threads; the release CAS that links the
        // node publishes this store.
        self.value_word(node).store(value, Ordering::Relaxed);
    }

    /// Copies `key` into the arena and returns its offset.
    pub fn put_key(&self, key: &[u8]) -> u32 {
        let start = self.alloc(key.len() as u32);
        let p = self.ptr(start as usize, key.len());
        // SAFETY: fresh write of key.len() reserved bytes; `key` cannot overlap
        // the arena's private range.
        unsafe { std::ptr::copy_nonoverlapping(key.as_ptr(), p, key.len()) };
        start
    }

    /// Encodes `v` into the arena and returns its offset. The caller keeps the
    /// size (`v.encoded_size()`).
    pub fn put_val(&self, v: &ValueStruct<'_>) -> u32 {
        let l = v.encoded_size();
        let start = self.alloc(l);
        let p = self.ptr(start as usize, l as usize);
        // SAFETY: fresh write: the l reserved bytes belong to this thread only.
        let out = unsafe { std::slice::from_raw_parts_mut(p, l as usize) };
        v.encode(out);
        start
    }

    /// The bytes at `offset..offset + size`, which must be published.
    pub fn bytes(&self, offset: u32, size: u32) -> &[u8] {
        let p = self.ptr(offset as usize, size as usize);
        // SAFETY: published read: the bytes were written before their offset
        // was released, are never written again, and live as long as `self`.
        unsafe { std::slice::from_raw_parts(p, size as usize) }
    }

    /// Decodes the value at `offset` (Go's `getVal`).
    pub fn val(&self, offset: u32, size: u32) -> ValueStruct<'_> {
        ValueStruct::decode(self.bytes(offset, size))
            .expect("skiplist arena holds a valid ValueStruct")
    }

    /// A node's key.
    pub fn node_key(&self, node: u32) -> &[u8] {
        let p = self.ptr(node as usize, TOWER_AT);
        // SAFETY: published read of the node's immutable header fields.
        let (off, size) = unsafe {
            (
                p.add(KEY_OFFSET_AT).cast::<u32>().read(),
                p.add(KEY_SIZE_AT).cast::<u16>().read(),
            )
        };
        self.bytes(off, size as u32)
    }

    /// A node's tower height.
    #[cfg(test)]
    pub fn node_height(&self, node: u32) -> u16 {
        let p = self.ptr(node as usize, TOWER_AT);
        // SAFETY: published read of an immutable header field.
        unsafe { p.add(HEIGHT_AT).cast::<u16>().read() }
    }

    /// The node's packed value word (see [`encode_value`]).
    pub fn value_word(&self, node: u32) -> &AtomicU64 {
        #[cfg(not(loom))]
        {
            let p = self.ptr(node as usize + VALUE_AT, 8);
            // SAFETY: nodes are 8-aligned, the word is inside the allocation,
            // and it is only ever accessed atomically.
            unsafe { AtomicU64::from_ptr(p.cast()) }
        }
        #[cfg(loom)]
        {
            &self.loom_u64[(node as usize + VALUE_AT) / 8]
        }
    }

    /// The node's link at level `h`. `h` must be below the node's height.
    pub fn tower(&self, node: u32, h: usize) -> &AtomicU32 {
        debug_assert!(h < MAX_HEIGHT);
        let at = node as usize + TOWER_AT + h * OFFSET_SIZE;
        #[cfg(not(loom))]
        {
            let p = self.ptr(at, OFFSET_SIZE);
            // SAFETY: 4-aligned (node is 8-aligned, TOWER_AT is 16), inside the
            // node's allocated tower, and only ever accessed atomically.
            unsafe { AtomicU32::from_ptr(p.cast()) }
        }
        #[cfg(loom)]
        {
            &self.loom_u32[at / 4]
        }
    }
}

impl Drop for Arena {
    fn drop(&mut self) {
        // SAFETY: allocated in `new` with the same layout.
        unsafe { dealloc(self.buf.as_ptr(), Self::layout(self.len)) };
    }
}

/// Packs a value's offset and size into one word so both change atomically.
pub fn encode_value(offset: u32, size: u32) -> u64 {
    (size as u64) << 32 | offset as u64
}

/// Inverse of [`encode_value`]: `(offset, size)`.
pub fn decode_value(word: u64) -> (u32, u32) {
    (word as u32, (word >> 32) as u32)
}
