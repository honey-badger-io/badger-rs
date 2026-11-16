//! Building blocks shared by the rest of the crate. Ports Go Badger's `y`
//! package. Helpers that Go needs but Rust's standard library covers (`Copy`,
//! `SafeCopy`, `Slice`, the U32/U64 byte conversions) are not ported.

pub mod bloom;
pub mod checksum;
pub mod compression;
pub mod encrypt;
pub mod file;
pub mod iterator;
pub mod keys;
pub mod value;
pub mod varint;
pub mod watermark;

pub use file::{MmapFile, sync_dir};
pub use iterator::KvIterator;
pub use keys::{compare_keys, key_with_ts, parse_key, parse_ts, same_key};
pub use value::ValueStruct;
pub use watermark::WaterMark;
