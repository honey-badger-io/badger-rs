//! A Rust port of [Badger](https://github.com/dgraph-io/badger), an embeddable,
//! persistent key-value store built on an LSM tree with key/value separation
//! (WiscKey).
//!
//! The on-disk format matches Go Badger [`GO_BADGER_VERSION`]: databases written by
//! either implementation can be opened by the other. See `PLAN.md` at the
//! repository root for the porting roadmap.

/// The Go Badger release whose on-disk format this crate reads and writes.
pub const GO_BADGER_VERSION: &str = "v4.9.6";
