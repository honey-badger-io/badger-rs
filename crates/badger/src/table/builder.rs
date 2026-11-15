//! Builds SSTables. Port of Go's `table/builder.go`.
//!
//! Entries must be added in sorted order (see [`compare_keys`]). They are
//! packed into blocks of about `block_size` bytes (see [`Block`] for the
//! block layout). Each finished block is compressed, then
//! encrypted. [`TableBuilder::done`] adds the bloom filter and the flatbuffer
//! index (also encrypted) plus its checksum.
//!
//! With no compression or encryption the output is byte-identical to Go's.
//!
//! Difference from Go: blocks are compressed and encrypted synchronously,
//! instead of by a pool of goroutines. Go's `ReachedCapacity` reads a
//! compressed size those goroutines update asynchronously, so it can lag
//! slightly; here the size is always exact.
//!
//! [`compare_keys`]: crate::util::keys::compare_keys
//! [`Block`]: super::Block

use std::io::Write;

use flatbuffers::FlatBufferBuilder;
use prost::Message;

use super::TableOptions;
use super::block::encode_entry_header;
use crate::error::{Error, Result};
use crate::fb::{self, BlockOffsetBuilder, TableIndexBuilder};
use crate::options::CompressionType;
use crate::pb::checksum::Algorithm;
use crate::util::bloom;
use crate::util::checksum::checksum_of;
use crate::util::compression::compress;
use crate::util::encrypt::{BLOCK_SIZE as IV_SIZE, generate_iv, xor_block_alloc};
use crate::util::keys::{parse_key, parse_ts};
use crate::util::value::ValueStruct;

/// Extra room allocated per block for growth from encryption (Go's `padding`).
const PADDING: usize = 256;

#[derive(Debug, Default)]
struct CurrentBlock {
    data: Vec<u8>,
    base_key: Vec<u8>,
    entry_offsets: Vec<u32>,
}

#[derive(Debug)]
struct FinishedBlock {
    /// Compressed and encrypted block bytes, as written to the file.
    data: Vec<u8>,
    base_key: Vec<u8>,
}

/// Builds one table. See the module docs.
#[derive(Debug)]
pub struct TableBuilder {
    opts: TableOptions,
    table_capacity: u64,
    cur: CurrentBlock,
    blocks: Vec<FinishedBlock>,
    compressed_size: u32,
    uncompressed_size: u32,
    /// Estimated size of the index entries so far.
    len_offsets: u32,
    /// Bloom filter hashes of all user keys.
    key_hashes: Vec<u32>,
    max_version: u64,
    on_disk_size: u32,
    stale_data_size: usize,
    /// First error from compressing or encrypting a block, reported by `done`.
    error: Option<Error>,
}

impl TableBuilder {
    pub fn new(opts: TableOptions) -> Self {
        let table_capacity = (opts.table_size as f64 * 0.95) as u64;
        let cur = CurrentBlock {
            data: Vec::with_capacity(opts.block_size + PADDING),
            ..Default::default()
        };
        TableBuilder {
            opts,
            table_capacity,
            cur,
            blocks: Vec::new(),
            compressed_size: 0,
            uncompressed_size: 0,
            len_offsets: 0,
            key_hashes: Vec::new(),
            max_version: 0,
            on_disk_size: 0,
            stale_data_size: 0,
            error: None,
        }
    }

    pub fn opts(&self) -> &TableOptions {
        &self.opts
    }

    /// Whether no entries have been added.
    pub fn is_empty(&self) -> bool {
        self.key_hashes.is_empty()
    }

    /// Adds an entry. `key` is an internal key (with version). `value_len` is
    /// the length of the value in the value log when `value` is a value
    /// pointer, else 0; it only counts towards `on_disk_size`.
    pub fn add(&mut self, key: &[u8], value: ValueStruct<'_>, value_len: u32) {
        self.add_internal(key, value, value_len, false);
    }

    /// Like [`TableBuilder::add`], but also counts the entry as stale data,
    /// which makes compaction pick this table sooner.
    pub fn add_stale_key(&mut self, key: &[u8], value: ValueStruct<'_>, value_len: u32) {
        // Rough estimate of the space the entry takes in the table.
        self.stale_data_size += key.len() + value.value.len() + 4 /* offset */ + 4 /* header */;
        self.add_internal(key, value, value_len, true);
    }

    fn add_internal(&mut self, key: &[u8], value: ValueStruct<'_>, value_len: u32, stale: bool) {
        if self.should_finish_block(key, &value) {
            if stale {
                // This key starts a block, so it also lands in the index.
                self.stale_data_size += key.len() + 4 /* len */ + 4 /* offset */;
            }
            self.finish_block();
        }
        self.add_helper(key, value, value_len);
    }

    fn add_helper(&mut self, key: &[u8], value: ValueStruct<'_>, value_len: u32) {
        self.key_hashes.push(bloom::hash(parse_key(key)));
        self.max_version = self.max_version.max(parse_ts(key));

        let cur = &mut self.cur;
        let diff_key = if cur.base_key.is_empty() {
            cur.base_key.extend_from_slice(key);
            key
        } else {
            let common = key
                .iter()
                .zip(&cur.base_key)
                .take_while(|(a, b)| a == b)
                .count();
            &key[common..]
        };
        let overlap = key.len() - diff_key.len();
        assert!(
            overlap <= u16::MAX as usize && diff_key.len() <= u16::MAX as usize,
            "key too long"
        );

        cur.entry_offsets.push(cur.data.len() as u32);
        cur.data
            .extend_from_slice(&encode_entry_header(overlap as u16, diff_key.len() as u16));
        cur.data.extend_from_slice(diff_key);
        value.encode_to(&mut cur.data);

        self.on_disk_size = self.on_disk_size.wrapping_add(value_len); // uint32 in Go
    }

    /// Whether adding this entry would make the current block too big.
    pub(crate) fn should_finish_block(&self, key: &[u8], value: &ValueStruct<'_>) -> bool {
        if self.cur.entry_offsets.is_empty() {
            return false;
        }
        // Include the new entry's offset (+1).
        let entry_offsets_size = (self.cur.entry_offsets.len() as u64 + 1) * 4
            + 4 // number of entries
            + 8 // checksum (Sum64 in the proto)
            + 4; // checksum length
        let mut estimated = self.cur.data.len() as u64
            + 6 // entry header
            + key.len() as u64
            + value.encoded_size() as u64
            + entry_offsets_size;
        if self.opts.data_key.is_some() {
            estimated += IV_SIZE as u64; // the IV appended to encrypted blocks
        }
        estimated > self.opts.block_size as u64
    }

    /// Appends the block trailer, then compresses and encrypts the block.
    fn finish_block(&mut self) {
        if self.cur.entry_offsets.is_empty() {
            return;
        }
        let mut cur = std::mem::take(&mut self.cur);
        for off in &cur.entry_offsets {
            cur.data.extend_from_slice(&off.to_le_bytes());
        }
        cur.data
            .extend_from_slice(&(cur.entry_offsets.len() as u32).to_be_bytes());
        let checksum = checksum_of(&cur.data, Algorithm::Crc32c).encode_to_vec();
        cur.data.extend_from_slice(&checksum);
        cur.data
            .extend_from_slice(&(checksum.len() as u32).to_be_bytes());

        self.uncompressed_size += cur.data.len() as u32;
        // Index entry estimate: the base key rounded up to 4 bytes, plus 40
        // bytes of flatbuffer overhead, offset and length.
        self.len_offsets += (cur.base_key.len().div_ceil(4) * 4) as u32 + 40;

        let data = match self.process_block(cur.data) {
            Ok(data) => data,
            Err(e) => {
                self.error.get_or_insert(e);
                Vec::new()
            }
        };
        self.compressed_size += data.len() as u32;
        self.blocks.push(FinishedBlock {
            data,
            base_key: cur.base_key,
        });
        self.cur.data.reserve(self.opts.block_size + PADDING);
    }

    fn process_block(&self, data: Vec<u8>) -> Result<Vec<u8>> {
        let data = match self.opts.compression {
            CompressionType::None => data,
            c => compress(c, &data, self.opts.zstd_compression_level)?,
        };
        if self.opts.data_key.is_some() {
            self.encrypt(&data)
        } else {
            Ok(data)
        }
    }

    /// Encrypts `data` and appends the IV.
    fn encrypt(&self, data: &[u8]) -> Result<Vec<u8>> {
        let key = self
            .opts
            .data_key
            .as_ref()
            .expect("only called with a data key");
        let iv = generate_iv()?;
        let mut out = xor_block_alloc(data, &key.data, &iv)?;
        out.extend_from_slice(&iv);
        Ok(out)
    }

    /// Whether the table has reached about `table_size`.
    pub fn reached_capacity(&self) -> bool {
        let sum_block_sizes =
            if self.opts.compression == CompressionType::None && self.opts.data_key.is_none() {
                self.uncompressed_size
            } else {
                self.compressed_size
            };
        let blocks_size = sum_block_sizes as u64
            + self.cur.entry_offsets.len() as u64 * 4
            + 4 // count of entry offsets
            + 8 // checksum
            + 4; // checksum length
        let estimate = blocks_size + 4 /* index length */ + self.len_offsets as u64;
        estimate > self.table_capacity
    }

    /// Finishes the table: the last block, bloom filter, index and checksum.
    pub fn done(mut self) -> Result<BuildData> {
        self.finish_block();
        if let Some(e) = self.error.take() {
            return Err(e);
        }
        if self.blocks.is_empty() {
            return Ok(BuildData::default());
        }
        let bloom = if self.opts.bloom_false_positive > 0.0 {
            let bits =
                bloom::bloom_bits_per_key(self.key_hashes.len(), self.opts.bloom_false_positive);
            bloom::new_filter(&self.key_hashes, bits)
        } else {
            Vec::new()
        };
        let (mut index, data_size) = self.build_index(&bloom);
        if self.opts.data_key.is_some() {
            index = self.encrypt(&index)?;
        }
        let checksum = checksum_of(&index, Algorithm::Crc32c).encode_to_vec();
        let size = data_size + index.len() + checksum.len() + 4 + 4;
        Ok(BuildData {
            blocks: self.blocks.into_iter().map(|b| b.data).collect(),
            index,
            checksum,
            size,
        })
    }

    /// Finishes and returns the whole table as bytes (Go's `Finish`).
    pub fn finish(self) -> Result<Vec<u8>> {
        let bd = self.done()?;
        let mut out = Vec::with_capacity(bd.size);
        bd.write_to(&mut out)?;
        Ok(out)
    }

    /// Builds the flatbuffer index in exactly the order Go does, so the bytes
    /// match. Returns the index and the total size of the blocks.
    fn build_index(&mut self, bloom: &[u8]) -> (Vec<u8>, usize) {
        let mut fbb = FlatBufferBuilder::with_capacity(1024 + self.blocks.len() * 64);
        let mut offsets = Vec::with_capacity(self.blocks.len());
        let mut start = 0u32;
        for block in &self.blocks {
            let key = fbb.create_vector(&block.base_key);
            let mut bo = BlockOffsetBuilder::new(&mut fbb);
            bo.add_key(key);
            bo.add_offset(start);
            bo.add_len(block.data.len() as u32);
            offsets.push(bo.finish());
            start += block.data.len() as u32;
        }
        let data_size = start;
        let offsets = fbb.create_vector(&offsets);
        let bloom = (!bloom.is_empty()).then(|| fbb.create_vector(bloom));

        self.on_disk_size += data_size;
        let mut ti = TableIndexBuilder::new(&mut fbb);
        ti.add_offsets(offsets);
        if let Some(bloom) = bloom {
            ti.add_bloom_filter(bloom);
        }
        ti.add_max_version(self.max_version);
        ti.add_uncompressed_size(self.uncompressed_size);
        ti.add_key_count(self.key_hashes.len() as u32);
        ti.add_on_disk_size(self.on_disk_size);
        ti.add_stale_data_size(self.stale_data_size as u32);
        let root = ti.finish();
        fbb.finish(root, None);

        let mut index = fbb.finished_data().to_vec();
        // The on-disk size includes the index itself.
        let with_index = self.on_disk_size + index.len() as u32;
        assert!(
            fb::mutate_on_disk_size(&mut index, with_index),
            "on_disk_size is set"
        );
        (index, data_size as usize)
    }
}

/// A finished table, ready to be written out (Go's `buildData`).
#[derive(Debug, Default)]
pub struct BuildData {
    blocks: Vec<Vec<u8>>,
    index: Vec<u8>,
    checksum: Vec<u8>,
    size: usize,
}

impl BuildData {
    /// Total size of the table in bytes; 0 for an empty builder.
    pub fn size(&self) -> usize {
        self.size
    }

    /// Writes the table: blocks, index, index length, checksum, checksum length.
    pub fn write_to(&self, w: &mut impl Write) -> Result<()> {
        if self.size == 0 {
            return Ok(());
        }
        for block in &self.blocks {
            w.write_all(block)?;
        }
        w.write_all(&self.index)?;
        w.write_all(&(self.index.len() as u32).to_be_bytes())?;
        w.write_all(&self.checksum)?;
        w.write_all(&(self.checksum.len() as u32).to_be_bytes())?;
        Ok(())
    }
}
