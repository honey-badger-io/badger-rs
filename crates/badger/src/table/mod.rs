//! SSTables: immutable sorted files of key-value entries. Port of Go Badger's
//! `table` package.
//!
//! Table layout (see [`builder`] for how it is written):
//! ```text
//! +---------+---------+-----+---------+-------+------------------+----------+--------------------+
//! | block 0 | block 1 | ... | block N | index | index len (u32BE)| checksum | checksum len(u32BE)|
//! +---------+---------+-----+---------+-------+------------------+----------+--------------------+
//! ```
//! The index is a flatbuffer ([`crate::fb::TableIndex`]) with each block's
//! first key, offset and length, plus a bloom filter and table metadata. The
//! checksum (CRC32C, protobuf-encoded) covers the stored index. With
//! encryption, every block and the index are AES-CTR encrypted with a fresh
//! 16-byte IV appended. Blocks are compressed before encryption.
//!
//! Note that a table does not record its compression or encryption key:
//! callers pass them in [`TableOptions`] (the DB gets them from the manifest
//! and key registry).
//!
//! Differences from Go:
//! - Tables live in an `Arc`. Instead of deleting the file when a reference
//!   count drops to zero, call [`Table::delete_on_drop`] and the file is removed
//!   when the last `Arc` is dropped.
//! - Table bytes are held as [`Bytes`] (backed by the mmap), so blocks and the
//!   index are zero-copy slices.
//! - Without an index cache, an encrypted table keeps its decrypted index in
//!   memory (Go requires an index cache for encrypted tables).

mod block;
pub mod builder;
mod cache;
mod iterator;
mod merge_iterator;
// These use files, mmap and zstd, which Miri cannot run; see `miri_tests`.
#[cfg(all(test, not(miri)))]
mod tests;

use std::fmt;
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::SystemTime;

use bytes::Bytes;
use memmap2::Mmap;
use prost::Message;

use crate::error::{Error, Result, ResultExt};
use crate::fb;
use crate::options::{ChecksumVerificationMode, CompressionType};
use crate::pb;
use crate::util::bloom;
use crate::util::checksum::verify_checksum;
use crate::util::compression::decompress;
use crate::util::encrypt::{BLOCK_SIZE as IV_SIZE, xor_block_alloc};
use crate::util::keys::compare_keys;

pub use block::Block;
use block::BlockIterator;
pub use builder::{BuildData, TableBuilder};
pub use cache::{BlockCache, IndexCache, new_block_cache, new_index_cache};
pub use iterator::{ConcatIterator, TableIterator};
pub use merge_iterator::MergeIterator;

/// Suffix of table file names.
pub const FILE_SUFFIX: &str = ".sst";

/// Options for building and opening tables (Go's `table.Options`).
#[derive(Clone, Default)]
pub struct TableOptions {
    /// Target size of a table, for [`TableBuilder::reached_capacity`].
    pub table_size: u64,
    pub checksum_mode: ChecksumVerificationMode,
    /// Bloom filter false positive rate; 0 builds no bloom filter.
    pub bloom_false_positive: f64,
    /// Target size of a block before compression.
    pub block_size: usize,
    /// Key to encrypt blocks and the index with; `None` means unencrypted.
    pub data_key: Option<Arc<pb::DataKey>>,
    pub compression: CompressionType,
    pub zstd_compression_level: i32,
    /// Cache of decoded blocks, shared between tables.
    pub block_cache: Option<BlockCache>,
    /// Cache of decrypted indexes of encrypted tables.
    pub index_cache: Option<IndexCache>,
}

impl fmt::Debug for TableOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TableOptions")
            .field("table_size", &self.table_size)
            .field("checksum_mode", &self.checksum_mode)
            .field("bloom_false_positive", &self.bloom_false_positive)
            .field("block_size", &self.block_size)
            .field("data_key_id", &self.data_key.as_ref().map(|k| k.key_id))
            .field("compression", &self.compression)
            .field("zstd_compression_level", &self.zstd_compression_level)
            .field("block_cache", &self.block_cache.is_some())
            .field("index_cache", &self.index_cache.is_some())
            .finish()
    }
}

/// Index fields read once at open (Go's `cheapIndex`).
#[derive(Debug, Default, Clone, Copy)]
struct CheapIndex {
    max_version: u64,
    key_count: u32,
    uncompressed_size: u32,
    on_disk_size: u32,
    bloom_filter_len: usize,
    offsets_len: usize,
}

/// An open SSTable.
pub struct Table {
    id: u64,
    path: Option<PathBuf>,
    data: Bytes,
    opts: TableOptions,
    index_start: usize,
    index_len: usize,
    /// The decoded index, unless it lives in the index cache.
    index: Option<Bytes>,
    cheap: CheapIndex,
    smallest: Vec<u8>,
    biggest: Vec<u8>,
    created_at: SystemTime,
    delete_on_drop: AtomicBool,
}

impl fmt::Debug for Table {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Table")
            .field("id", &self.id)
            .field("path", &self.path)
            .field("size", &self.data.len())
            .field("cheap", &self.cheap)
            .finish_non_exhaustive()
    }
}

/// Table file name for `id`: six or more digits plus `.sst`.
pub fn id_to_filename(id: u64) -> String {
    format!("{id:06}{FILE_SUFFIX}")
}

/// Path of table `id` in `dir`.
pub fn new_filename(id: u64, dir: &Path) -> PathBuf {
    dir.join(id_to_filename(id))
}

/// Parses the table id from a file name like `000042.sst`.
pub fn parse_file_id(path: &Path) -> Option<u64> {
    let name = path.file_name()?.to_str()?;
    name.strip_suffix(FILE_SUFFIX)?.parse().ok()
}

impl Table {
    /// Opens the table file at `path`. The file name must be `<id>.sst`.
    pub fn open(path: impl AsRef<Path>, opts: TableOptions) -> Result<Arc<Table>> {
        let path = path.as_ref();
        let id = parse_file_id(path)
            .ok_or_else(|| Error::corruption(format!("Invalid filename: {}", path.display())))?;
        let file = File::open(path).with_context(|| format!("opening table {}", path.display()))?;
        let created_at = file
            .metadata()?
            .modified()
            .unwrap_or_else(|_| SystemTime::now());
        // SAFETY: table files are written once and never modified while open.
        // Deleting the file (on Unix) does not invalidate the mapping.
        let mmap = unsafe { Mmap::map(&file) }?;
        Self::from_bytes(
            id,
            Some(path.to_path_buf()),
            Bytes::from_owner(mmap),
            opts,
            created_at,
        )
    }

    /// Opens a table held in memory (Go's `OpenInMemoryTable`, used for L0
    /// tables of in-memory DBs). `id` must be unique.
    pub fn open_in_memory(
        data: impl Into<Bytes>,
        id: u64,
        opts: TableOptions,
    ) -> Result<Arc<Table>> {
        Self::from_bytes(id, None, data.into(), opts, SystemTime::now())
    }

    /// Writes the table built by `builder` to `path` (which must not exist)
    /// and opens it (Go's `CreateTable`).
    pub fn create(path: impl AsRef<Path>, builder: TableBuilder) -> Result<Arc<Table>> {
        let path = path.as_ref();
        let opts = builder.opts().clone();
        let bd = builder.done()?;
        if bd.size() == 0 {
            return Err(Error::InvalidOptions(
                "cannot create a table with no entries".into(),
            ));
        }
        let file = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)
            .with_context(|| format!("while creating table: {}", path.display()))?;
        let mut w = std::io::BufWriter::new(&file);
        bd.write_to(&mut w)?;
        std::io::Write::flush(&mut w)?;
        drop(w);
        file.sync_all()?;
        Self::open(path, opts)
    }

    fn from_bytes(
        id: u64,
        path: Option<PathBuf>,
        data: Bytes,
        opts: TableOptions,
        created_at: SystemTime,
    ) -> Result<Arc<Table>> {
        let mut t = Table {
            id,
            path,
            data,
            opts,
            index_start: 0,
            index_len: 0,
            index: None,
            cheap: CheapIndex::default(),
            smallest: Vec::new(),
            biggest: Vec::new(),
            created_at,
            delete_on_drop: AtomicBool::new(false),
        };
        t.init_index()
            .with_context(|| format!("failed to read index of {}", t.filename()))?;
        t.smallest = t.block_key(0)?;
        t.biggest = {
            let mut bi = BlockIterator::default();
            bi.set_block(t.block(t.cheap.offsets_len - 1, false)?);
            bi.seek_to_last();
            if let Some(block::IterError::Err(e)) = &bi.err {
                return Err(Error::corruption(format!(
                    "failed to initialize biggest for table {}: {e}",
                    t.filename()
                )));
            }
            bi.key
        };
        if matches!(
            t.opts.checksum_mode,
            ChecksumVerificationMode::OnTableRead | ChecksumVerificationMode::OnTableAndBlockRead
        ) {
            t.verify_checksum().context("failed to verify checksum")?;
        }
        Ok(Arc::new(t))
    }

    /// Reads the footer, checks the index checksum and decodes the index.
    fn init_index(&mut self) -> Result<()> {
        let data = &self.data;
        let bad = |msg: &str| Error::corruption(msg.to_string());
        if data.len() < 8 {
            return Err(bad("table too short"));
        }
        let be = |pos: usize| u32::from_be_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;

        let mut pos = data.len() - 4;
        let checksum_len = be(pos);
        if checksum_len > pos {
            return Err(bad("checksum length out of range"));
        }
        pos -= checksum_len;
        let expected = pb::Checksum::decode(&data[pos..pos + checksum_len])?;
        if pos < 4 {
            return Err(bad("missing index length"));
        }
        pos -= 4;
        let index_len = be(pos);
        if index_len > pos {
            return Err(bad("index length out of range"));
        }
        pos -= index_len;
        self.index_start = pos;
        self.index_len = index_len;
        verify_checksum(&data[pos..pos + index_len], &expected)
            .with_context(|| format!("failed to verify checksum for table: {}", self.filename()))?;

        let index = self.read_index()?;
        // SAFETY: read_index verified these bytes.
        let ti = unsafe { fb::root_as_table_index_unchecked(&index) };
        self.cheap = CheapIndex {
            max_version: ti.max_version(),
            key_count: ti.key_count(),
            uncompressed_size: ti.uncompressed_size(),
            on_disk_size: ti.on_disk_size(),
            bloom_filter_len: ti.bloom_filter().map_or(0, |b| b.len()),
            offsets_len: ti.offsets().map_or(0, |o| o.len()),
        };
        if self.cheap.offsets_len == 0 {
            return Err(bad("table has no blocks"));
        }
        match (&self.opts.data_key, &self.opts.index_cache) {
            (Some(_), Some(cache)) => cache.insert(self.id, index),
            _ => self.index = Some(index),
        }
        Ok(())
    }

    /// Reads, decrypts and verifies the stored index.
    fn read_index(&self) -> Result<Bytes> {
        let stored = self
            .data
            .slice(self.index_start..self.index_start + self.index_len);
        let index = if self.opts.data_key.is_some() {
            Bytes::from(self.decrypt(&stored).with_context(|| {
                format!(
                    "Error while decrypting table index for the table {}",
                    self.id
                )
            })?)
        } else {
            stored
        };
        fb::root_as_table_index(&index)?;
        Ok(index)
    }

    /// The decoded index bytes (verified when they were read).
    fn index_bytes(&self) -> Result<Bytes> {
        if let Some(index) = &self.index {
            return Ok(index.clone());
        }
        let cache = self
            .opts
            .index_cache
            .as_ref()
            .expect("index is cached when not held");
        cache.get_or_insert_with(&self.id, || self.read_index())
    }

    /// Calls `f` with the table's index.
    fn with_index<R>(&self, f: impl FnOnce(fb::TableIndex<'_>) -> R) -> Result<R> {
        let bytes = self.index_bytes()?;
        // SAFETY: index_bytes only returns bytes that passed root_as_table_index.
        Ok(f(unsafe { fb::root_as_table_index_unchecked(&bytes) }))
    }

    /// Offset and length of block `idx`.
    fn block_offset(&self, idx: usize) -> Result<(usize, usize)> {
        self.with_index(|ti| {
            let bo = ti.offsets().expect("checked at open").get(idx);
            (bo.offset() as usize, bo.len() as usize)
        })
    }

    /// First key of block `idx`.
    fn block_key(&self, idx: usize) -> Result<Vec<u8>> {
        self.with_index(|ti| {
            let bo = ti.offsets().expect("checked at open").get(idx);
            bo.key().map(|k| k.bytes().to_vec()).unwrap_or_default()
        })
    }

    /// Index of the first block whose first key is greater than `key`.
    fn search_blocks(&self, key: &[u8]) -> Result<usize> {
        self.with_index(|ti| {
            let offsets = ti.offsets().expect("checked at open");
            let (mut lo, mut hi) = (0, offsets.len());
            while lo < hi {
                let mid = lo + (hi - lo) / 2;
                let block_key = offsets
                    .get(mid)
                    .key()
                    .map(|k| k.bytes())
                    .unwrap_or_default();
                if compare_keys(block_key, key).is_gt() {
                    hi = mid;
                } else {
                    lo = mid + 1;
                }
            }
            lo
        })
    }

    fn block_cache_key(&self, idx: usize) -> u64 {
        assert!(self.id < u32::MAX as u64 && idx < u32::MAX as usize);
        (self.id << 32) | idx as u64
    }

    /// Reads and decodes block `idx`, through the block cache if `use_cache`.
    pub(crate) fn block(&self, idx: usize, use_cache: bool) -> Result<Arc<Block>> {
        if idx >= self.cheap.offsets_len {
            return Err(Error::corruption("block out of index"));
        }
        let cache_key = self.block_cache_key(idx);
        if let Some(cache) = &self.opts.block_cache
            && let Some(block) = cache.get(&cache_key)
        {
            return Ok(block);
        }

        let (offset, len) = self.block_offset(idx)?;
        if offset + len > self.index_start {
            return Err(Error::corruption(format!(
                "block {idx} at offset {offset}, len {len} overlaps the index of {}",
                self.filename()
            )));
        }
        let mut data = self.data.slice(offset..offset + len);
        if self.opts.data_key.is_some() {
            data = Bytes::from(self.decrypt(&data)?);
        }
        if self.opts.compression != CompressionType::None {
            data = Bytes::from(decompress(self.opts.compression, &data).with_context(|| {
                format!(
                    "failed to decode compressed data in file: {} at offset: {offset}, len: {len}",
                    self.filename()
                )
            })?);
        }
        let block = Block::parse(offset, data)?;
        if matches!(
            self.opts.checksum_mode,
            ChecksumVerificationMode::OnBlockRead | ChecksumVerificationMode::OnTableAndBlockRead
        ) {
            block.verify_checksum()?;
        }
        let block = Arc::new(block);
        if use_cache && let Some(cache) = &self.opts.block_cache {
            cache.insert(cache_key, Arc::clone(&block));
        }
        Ok(block)
    }

    /// Decrypts `data`, whose last 16 bytes are the IV.
    fn decrypt(&self, data: &[u8]) -> Result<Vec<u8>> {
        let key = self
            .opts
            .data_key
            .as_ref()
            .expect("only called for encrypted tables");
        if data.len() < IV_SIZE {
            return Err(Error::corruption("encrypted data shorter than its IV"));
        }
        let (data, iv) = data.split_at(data.len() - IV_SIZE);
        xor_block_alloc(data, &key.data, iv).context("while decrypt")
    }

    /// Checks the checksum of every block.
    pub fn verify_checksum(&self) -> Result<()> {
        for i in 0..self.cheap.offsets_len {
            self.block(i, true)
                .and_then(|b| b.verify_checksum())
                .with_context(|| {
                    format!(
                        "checksum validation failed for table: {}, block: {i}",
                        self.filename()
                    )
                })?;
        }
        Ok(())
    }

    /// Iterates over the table, newest-first per key; `reversed` goes from the
    /// biggest key down. `use_cache` controls whether blocks are put into the
    /// block cache.
    pub fn iter(self: &Arc<Self>, reversed: bool, use_cache: bool) -> TableIterator {
        TableIterator::new(Arc::clone(self), reversed, use_cache)
    }

    /// Whether the bloom filter rules out a key with this hash
    /// ([`bloom::hash`] of the user key).
    pub fn does_not_have(&self, hash: u32) -> bool {
        if self.cheap.bloom_filter_len == 0 {
            return false;
        }
        self.with_index(|ti| {
            let filter = ti.bloom_filter().map(|b| b.bytes()).unwrap_or_default();
            !bloom::may_contain(filter, hash)
        })
        .unwrap_or(false)
    }

    /// Splits the table into about `n` ranges, returning the first key of
    /// every block range whose key starts with `prefix`.
    pub fn key_splits(&self, n: usize, prefix: &[u8]) -> Result<Vec<Vec<u8>>> {
        if n == 0 {
            return Ok(Vec::new());
        }
        let jump = (self.cheap.offsets_len / n).max(1);
        let mut out = Vec::new();
        for i in (0..self.cheap.offsets_len).step_by(jump) {
            let key = self.block_key(i)?;
            if key.starts_with(prefix) {
                out.push(key);
            }
        }
        Ok(out)
    }

    /// Removes the file (and its cache entries) once the last reference drops.
    /// Go's equivalent is the reference count reaching zero.
    pub fn delete_on_drop(&self) {
        self.delete_on_drop.store(true, Ordering::SeqCst);
    }

    pub fn id(&self) -> u64 {
        self.id
    }
    /// Smallest internal key in the table.
    pub fn smallest(&self) -> &[u8] {
        &self.smallest
    }
    /// Biggest internal key in the table.
    pub fn biggest(&self) -> &[u8] {
        &self.biggest
    }
    pub fn max_version(&self) -> u64 {
        self.cheap.max_version
    }
    pub fn key_count(&self) -> u32 {
        self.cheap.key_count
    }
    pub fn uncompressed_size(&self) -> u32 {
        self.cheap.uncompressed_size
    }
    /// Size including the vlog data its value pointers refer to.
    pub fn on_disk_size(&self) -> u32 {
        self.cheap.on_disk_size
    }
    pub fn stale_data_size(&self) -> Result<u32> {
        self.with_index(|ti| ti.stale_data_size())
    }
    /// File size in bytes.
    pub fn size(&self) -> u64 {
        self.data.len() as u64
    }
    pub fn index_size(&self) -> usize {
        self.index_len
    }
    pub fn bloom_filter_size(&self) -> usize {
        self.cheap.bloom_filter_len
    }
    pub fn has_bloom_filter(&self) -> bool {
        self.cheap.bloom_filter_len > 0
    }
    pub fn num_blocks(&self) -> usize {
        self.cheap.offsets_len
    }
    pub fn compression_type(&self) -> CompressionType {
        self.opts.compression
    }
    /// Id of the data key the table is encrypted with; 0 when unencrypted.
    pub fn key_id(&self) -> u64 {
        self.opts.data_key.as_ref().map_or(0, |k| k.key_id)
    }
    pub fn created_at(&self) -> SystemTime {
        self.created_at
    }
    pub fn is_in_memory(&self) -> bool {
        self.path.is_none()
    }
    /// The file path, or the would-be file name for in-memory tables.
    pub fn filename(&self) -> String {
        match &self.path {
            Some(p) => p.display().to_string(),
            None => id_to_filename(self.id),
        }
    }
}

impl Drop for Table {
    fn drop(&mut self) {
        if !self.delete_on_drop.load(Ordering::SeqCst) {
            return;
        }
        if let Some(cache) = &self.opts.block_cache {
            for i in 0..self.cheap.offsets_len {
                cache.remove(&self.block_cache_key(i));
            }
        }
        if let Some(cache) = &self.opts.index_cache {
            cache.remove(&self.id);
        }
        if let Some(path) = &self.path {
            // Nothing useful to do with an error in drop; compaction will
            // notice leftover files when the DB is reopened.
            let _ = fs::remove_file(path);
        }
    }
}

/// Small in-memory tests that also run under Miri, to check the unsafe
/// flatbuffer access and block parsing.
#[cfg(test)]
mod miri_tests {
    use super::*;
    use crate::util::iterator::KvIterator;
    use crate::util::keys::key_with_ts;
    use crate::util::value::ValueStruct;

    #[test]
    fn build_open_iterate_seek_in_memory() {
        let opts = TableOptions {
            block_size: 64,
            bloom_false_positive: 0.01,
            ..Default::default()
        };
        let mut b = TableBuilder::new(opts.clone());
        for i in 0..40u64 {
            let v = format!("value-{i}");
            let vs = ValueStruct {
                value: v.as_bytes(),
                meta: 1,
                ..Default::default()
            };
            b.add(
                &key_with_ts(format!("key{i:03}").as_bytes(), 100 - i),
                vs,
                0,
            );
        }
        let t = Table::open_in_memory(b.finish().unwrap(), 1, opts).unwrap();
        assert!(t.num_blocks() > 3);
        assert_eq!(t.max_version(), 100);
        assert!(!t.does_not_have(bloom::hash(b"key007")));

        let mut it = t.iter(false, false);
        it.rewind();
        let mut n = 0;
        while it.valid() {
            assert_eq!(it.value().value, format!("value-{n}").as_bytes());
            n += 1;
            it.next();
        }
        assert_eq!(n, 40);

        let mut rev = t.iter(true, false);
        rev.seek(&key_with_ts(b"key0205", 0));
        assert_eq!(rev.value().value, b"value-20");
    }
}
