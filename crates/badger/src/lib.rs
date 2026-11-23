//! A Rust port of [Badger](https://github.com/dgraph-io/badger), an embeddable,
//! persistent key-value store built on an LSM tree with key/value separation
//! (WiscKey).
//!
//! The on-disk format matches Go Badger [`GO_BADGER_VERSION`]: databases written by
//! either implementation can be opened by the other. See `PLAN.md` at the
//! repository root for the porting roadmap.
//!
//! Module map (Go package → Rust module):
//!
//! | Go | Rust |
//! |---|---|
//! | `y` | [`util`] |
//! | `trie` | [`trie`] |
//! | `pb` | [`pb`] |
//! | `fb` | [`fb`] |
//! | `options.go`, `options/` | [`options`] |
//! | `errors.go` | [`error`] |
//! | `structs.go` | [`structs`] |
//! | `table` | [`table`] |
//! | `skl` | [`skl`] |
//! | `memtable.go` | [`memtable`], [`log_file`] |
//! | `key_registry.go` | [`key_registry`] |
//! | `manifest.go` | [`manifest`] |
//! | `levels.go`, `level_handler.go` | `levels` (internal) |
//! | `compaction.go`, compaction in `levels.go` | `compaction` (internal) |
//! | `value.go` (value log and GC) | [`vlog`] |
//! | `discard.go` | `discard` (internal) |
//! | `db.go`, `managed_db.go` | [`db`], `write` (internal: writer and flush threads) |
//! | `txn.go` | [`txn`], `oracle` (internal) |
//! | `iterator.go` | [`iterator`] |
//! | `dir_unix.go` | [`dir_lock`] |

pub mod batch;
mod compaction;
pub mod db;
pub mod dir_lock;
mod discard;
pub mod error;
pub mod fb;
pub mod iterator;
pub mod key_registry;
mod levels;
pub mod log_file;
pub mod manifest;
pub mod memtable;
pub mod merge;
pub mod options;
mod oracle;
pub mod pb;
pub mod skl;
pub mod structs;
pub mod table;
pub mod trie;
pub mod txn;
pub mod util;
pub mod vlog;
mod write;

pub use batch::WriteBatch;
pub use db::DB;
pub use error::{Error, Result};
pub use iterator::{Item, Iterator, IteratorOptions};
pub use levels::{LevelInfo, TableInfo};
pub use merge::MergeOperator;
pub use options::Options;
pub use structs::Entry;
pub use txn::{PendingCommit, Txn};

/// The Go Badger release whose on-disk format this crate reads and writes.
pub const GO_BADGER_VERSION: &str = "v4.9.6";
