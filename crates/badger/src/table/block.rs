//! Data blocks and the block iterator. Port of Go's `table.Block` and
//! `table.blockIterator`.
//!
//! See [`Block`] for the block layout.

use std::sync::Arc;

use bytes::Bytes;
use prost::Message;

use crate::error::{Error, Result, ResultExt};
use crate::pb;
use crate::util::checksum::verify_checksum;
use crate::util::keys::compare_keys;
use crate::util::value::ValueStruct;
use crate::util::varint::uvarint;

/// Size of an entry header (overlap + diff length).
pub(crate) const ENTRY_HEADER_SIZE: usize = 4;

/// Encodes an entry header: overlap with the base key, and diff length.
pub(crate) fn encode_entry_header(overlap: u16, diff: u16) -> [u8; ENTRY_HEADER_SIZE] {
    let mut h = [0u8; ENTRY_HEADER_SIZE];
    h[..2].copy_from_slice(&overlap.to_le_bytes());
    h[2..].copy_from_slice(&diff.to_le_bytes());
    h
}

fn decode_entry_header(b: &[u8]) -> (usize, usize) {
    let overlap = u16::from_le_bytes([b[0], b[1]]) as usize;
    let diff = u16::from_le_bytes([b[2], b[3]]) as usize;
    (overlap, diff)
}

fn be_u32(b: &[u8], pos: usize) -> usize {
    u32::from_be_bytes(b[pos..pos + 4].try_into().unwrap()) as usize
}

/// A decoded (decrypted, decompressed) block of an SSTable.
///
/// A block, after decryption and decompression:
/// ```text
/// +---------+---------+-----+---------+------------------+-----------+----------+--------------+
/// | entry 0 | entry 1 | ... | entry N | entry offsets    | N (u32 BE)| checksum | checksum len |
/// |         |         |     |         | (N x u32, LE)    |           | (pb)     | (u32 BE)     |
/// +---------+---------+-----+---------+------------------+-----------+----------+--------------+
/// ```
/// The checksum covers everything before it. Each entry is
/// ```text
/// | overlap (u16 LE) | diff len (u16 LE) | diff key bytes | ValueStruct |
/// ```
/// where the key is the first `overlap` bytes of the block's base key (the
/// first entry's key) followed by the diff bytes. Go writes the header and the
/// entry offsets by copying memory, hence little-endian, while the counts and
/// lengths use `binary.BigEndian`.
#[derive(Debug)]
pub struct Block {
    /// Offset of the block in the table file.
    offset: usize,
    /// Entries, entry offsets and entry count: the bytes the checksum covers.
    data: Bytes,
    /// Where the entries end and the entry offsets start.
    entries_end: usize,
    num_entries: usize,
    checksum: Bytes,
}

impl Block {
    /// Parses a block's trailer. `raw` is the decrypted, decompressed block.
    pub(crate) fn parse(offset: usize, raw: Bytes) -> Result<Block> {
        let bad = |msg: &str| Error::corruption(format!("block at offset {offset}: {msg}"));
        if raw.len() < 4 {
            return Err(bad("too short"));
        }
        let mut pos = raw.len() - 4;
        let chk_len = be_u32(&raw, pos);
        if chk_len > pos {
            return Err(bad(
                "invalid checksum length. Either the data is corrupted or the \
                            table options are incorrectly set",
            ));
        }
        pos -= chk_len;
        let checksum = raw.slice(pos..pos + chk_len);
        if pos < 4 {
            return Err(bad("missing entry count"));
        }
        pos -= 4;
        let num_entries = be_u32(&raw, pos);
        if num_entries > pos / 4 {
            return Err(bad("invalid entry count"));
        }
        let entries_end = pos - num_entries * 4;
        Ok(Block {
            offset,
            data: raw.slice(..pos + 4),
            entries_end,
            num_entries,
            checksum,
        })
    }

    /// Offset of the block in its table file.
    pub fn offset(&self) -> usize {
        self.offset
    }

    pub fn num_entries(&self) -> usize {
        self.num_entries
    }

    /// Approximate memory used, for cache accounting.
    pub fn size(&self) -> usize {
        3 * std::mem::size_of::<usize>() + self.data.len() + self.checksum.len()
    }

    pub fn verify_checksum(&self) -> Result<()> {
        let cs = pb::Checksum::decode(&self.checksum[..])
            .context("unable to unmarshal checksum for block")?;
        verify_checksum(&self.data, &cs)
    }

    fn entries(&self) -> &[u8] {
        &self.data[..self.entries_end]
    }

    fn entry_offset(&self, i: usize) -> usize {
        let p = self.entries_end + 4 * i;
        u32::from_le_bytes(self.data[p..p + 4].try_into().unwrap()) as usize
    }
}

/// Why an iterator is not valid.
#[derive(Debug, Clone)]
pub(crate) enum IterError {
    /// Moved past either end.
    Eof,
    /// A real error (I/O, corruption, ...).
    Err(Arc<Error>),
}

impl From<Error> for IterError {
    fn from(e: Error) -> Self {
        IterError::Err(Arc::new(e))
    }
}

/// Iterates over the entries of one block.
#[derive(Debug)]
pub(crate) struct BlockIterator {
    pub(crate) block: Option<Arc<Block>>,
    idx: isize,
    /// Range of the base key in the block's entries.
    base_key: Option<(usize, usize)>,
    pub(crate) key: Vec<u8>,
    // Decoded current value: meta, user meta, expires_at, value range.
    meta: u8,
    user_meta: u8,
    expires_at: u64,
    val: (usize, usize),
    pub(crate) err: Option<IterError>,
}

impl Default for BlockIterator {
    fn default() -> Self {
        BlockIterator {
            block: None,
            idx: 0,
            base_key: None,
            key: Vec::new(),
            meta: 0,
            user_meta: 0,
            expires_at: 0,
            val: (0, 0),
            err: Some(IterError::Eof),
        }
    }
}

impl BlockIterator {
    pub(crate) fn set_block(&mut self, block: Arc<Block>) {
        self.block = Some(block);
        self.err = None;
        self.idx = 0;
        self.base_key = None;
        self.key.clear();
        self.val = (0, 0);
    }

    pub(crate) fn value(&self) -> ValueStruct<'_> {
        let value = match &self.block {
            Some(b) => &b.entries()[self.val.0..self.val.1],
            None => &[],
        };
        ValueStruct {
            meta: self.meta,
            user_meta: self.user_meta,
            expires_at: self.expires_at,
            value,
            version: 0,
        }
    }

    /// Positions the iterator at entry `i`; out of range means EOF.
    fn set_idx(&mut self, i: isize) {
        self.idx = i;
        let Some(block) = self.block.as_ref() else {
            self.err = Some(IterError::Eof);
            return;
        };
        if i < 0 || i as usize >= block.num_entries {
            self.err = Some(IterError::Eof);
            return;
        }
        match self.decode_entry(i as usize) {
            Ok(()) => self.err = None,
            Err(e) => self.err = Some(e.into()),
        }
    }

    fn decode_entry(&mut self, i: usize) -> Result<()> {
        let block = self.block.as_ref().expect("checked by set_idx");
        let data = block.entries();
        let bad = |msg: &str| {
            Error::corruption(format!(
                "block at offset {}, entry {i}: {msg}",
                block.offset
            ))
        };

        let (base_start, base_end) = match self.base_key {
            Some(range) => range,
            None => {
                if data.len() < ENTRY_HEADER_SIZE {
                    return Err(bad("truncated base key header"));
                }
                let (_, diff) = decode_entry_header(data);
                let range = (ENTRY_HEADER_SIZE, ENTRY_HEADER_SIZE + diff);
                if range.1 > data.len() {
                    return Err(bad("truncated base key"));
                }
                self.base_key = Some(range);
                range
            }
        };

        let start = block.entry_offset(i);
        let end = if i + 1 == block.num_entries {
            data.len()
        } else {
            block.entry_offset(i + 1)
        };
        if start + ENTRY_HEADER_SIZE > end || end > data.len() {
            return Err(bad("invalid entry offsets"));
        }
        let (overlap, diff) = decode_entry_header(&data[start..]);
        let value_off = start + ENTRY_HEADER_SIZE + diff;
        if overlap > base_end - base_start || value_off > end {
            return Err(bad("invalid entry header"));
        }
        self.key.clear();
        self.key
            .extend_from_slice(&data[base_start..base_start + overlap]);
        self.key
            .extend_from_slice(&data[start + ENTRY_HEADER_SIZE..value_off]);

        // Decode the ValueStruct once here, so value() cannot fail.
        let v = &data[value_off..end];
        if v.len() < 3 {
            return Err(bad("truncated value"));
        }
        let (expires_at, n) = uvarint(&v[2..]).ok_or_else(|| bad("invalid expiresAt"))?;
        self.meta = v[0];
        self.user_meta = v[1];
        self.expires_at = expires_at;
        self.val = (value_off + 2 + n, end);
        Ok(())
    }

    /// Moves to the first entry with key >= `key`, searching from the start
    /// (`from_current == false`) or from the current entry.
    pub(crate) fn seek(&mut self, key: &[u8], from_current: bool) {
        self.err = None;
        let n = self.block.as_ref().map_or(0, |b| b.num_entries) as isize;
        let (mut lo, mut hi) = (if from_current { self.idx.max(0) } else { 0 }, n);
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            self.set_idx(mid);
            if matches!(self.err, Some(IterError::Err(_))) {
                return;
            }
            if compare_keys(&self.key, key).is_ge() {
                hi = mid;
            } else {
                lo = mid + 1;
            }
        }
        self.set_idx(lo);
    }

    pub(crate) fn seek_to_first(&mut self) {
        self.set_idx(0);
    }

    pub(crate) fn seek_to_last(&mut self) {
        let n = self.block.as_ref().map_or(0, |b| b.num_entries) as isize;
        self.set_idx(n - 1);
    }

    pub(crate) fn next(&mut self) {
        self.set_idx(self.idx + 1);
    }

    pub(crate) fn prev(&mut self) {
        self.set_idx(self.idx - 1);
    }
}
