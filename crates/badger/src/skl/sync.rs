//! Atomics used by the skiplist: `std`'s normally, loom's under `cfg(loom)` so
//! `loom` can explore thread interleavings.

#[cfg(loom)]
pub(crate) use loom::sync::atomic::{AtomicI32, AtomicU32, AtomicU64, Ordering};
#[cfg(not(loom))]
pub(crate) use std::sync::atomic::{AtomicI32, AtomicU32, AtomicU64, Ordering};
