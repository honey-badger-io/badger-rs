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

pub mod error;
pub mod fb;
pub mod options;
pub mod pb;
pub mod structs;
pub mod trie;
pub mod util;

pub use error::{Error, Result};
pub use options::Options;
pub use structs::Entry;

/// The Go Badger release whose on-disk format this crate reads and writes.
pub const GO_BADGER_VERSION: &str = "v4.9.6";
