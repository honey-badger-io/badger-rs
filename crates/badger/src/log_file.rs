//! [`LogFile`]: the append-only file format shared by the memtable WAL
//! (`*.mem`) and the value log (`*.vlog`). Port of Go's `logFile` from
//! `memtable.go`, plus `safeRead` from `value.go`.
//!
//! ```text
//! +----------------+------------------+------------------+-----
//! | keyID (8, BE)  | baseIV (12)      | entry            | entry ...
//! +----------------+------------------+------------------+-----
//! entry = header | key | value | crc32c (4, BE)
//! ```
//!
//! The header is [`Header`] (meta, user meta, varint lengths and expiry). The
//! CRC covers the header and the key and value as stored. With encryption
//! (`keyID != 0`), key and value are encrypted with AES-CTR using the data
//! key and an IV of `baseIV || entry offset (u32 BE)`.
//!
//! Files are preallocated and mapped; after each write the next header's
//! worth of bytes is zeroed, so replay stops at the first empty or corrupt
//! entry. Transactions are written as entries with `BIT_TXN` followed by a
//! `BIT_FIN_TXN` entry whose value is the commit timestamp in decimal; replay
//! drops a transaction without its finish marker.
//!
//! Differences from Go: no internal `RWMutex` (callers that share a file
//! between threads wrap it); writing past the end of the mapping is an error
//! instead of a panic; `iterate` parses the mapping directly instead of
//! through a buffered reader.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::error::{Error, Result, ResultExt};
use crate::key_registry::KeyRegistry;
use crate::pb;
use crate::structs::{BIT_FIN_TXN, BIT_TXN, Entry, Header, ValuePointer};
use crate::util::MmapFile;
use crate::util::encrypt::{BLOCK_SIZE, xor_block_alloc, xor_block_stream};
use crate::util::keys::parse_ts;
use crate::util::varint::MAX_VARINT_LEN64;

/// Size of the file header: key id (8) and base IV (12). Go's `vlogHeaderSize`.
pub const VLOG_HEADER_SIZE: u32 = 20;

/// Size of the CRC after each entry.
const CRC_SIZE: usize = 4;

/// Keys must fit in a `u16` (replay treats longer ones as corruption).
const MAX_KEY_LEN: u32 = 1 << 16;

/// See the module docs.
pub struct LogFile {
    mmap: MmapFile,
    pub fid: u32,
    /// Logical size: the mapping's length, or less after truncation.
    size: u32,
    data_key: Option<Arc<pb::DataKey>>,
    base_iv: [u8; 12],
    /// Where the next entry goes.
    pub write_at: u32,
}

impl LogFile {
    /// Opens (or, with `create`, creates) a log file. A new file is extended
    /// to `max_size` and gets a header with the registry's latest data key and
    /// a random base IV. Returns whether the file is new.
    pub fn open(
        path: &Path,
        fid: u32,
        create: bool,
        read_only: bool,
        max_size: u64,
        registry: &KeyRegistry,
    ) -> Result<(LogFile, bool)> {
        let (mmap, new) = MmapFile::open(path, create, read_only, max_size)
            .with_context(|| format!("while opening file: {}", path.display()))?;
        let mut lf = LogFile {
            fid,
            size: mmap.data().len() as u32,
            mmap,
            data_key: None,
            base_iv: [0; 12],
            write_at: VLOG_HEADER_SIZE,
        };
        if new {
            if let Err(e) = lf.bootstrap(registry) {
                let _ = std::fs::remove_file(path);
                return Err(e);
            }
            return Ok((lf, true));
        }
        if lf.size < VLOG_HEADER_SIZE {
            // Corrupt or empty; replay truncates and the caller rebuilds it.
            return Ok((lf, false));
        }
        let data = lf.mmap.data();
        let key_id = u64::from_be_bytes(data[..8].try_into().unwrap());
        lf.base_iv.copy_from_slice(&data[8..20]);
        lf.data_key = registry
            .data_key(key_id)
            .with_context(|| format!("While opening vlog file {fid}"))?;
        Ok((lf, false))
    }

    /// Writes the file header (Go's `bootstrap`).
    fn bootstrap(&mut self, registry: &KeyRegistry) -> Result<()> {
        self.data_key = registry
            .latest_data_key()
            .context("Error while retrieving datakey in logFile.bootstrap")?;
        getrandom::fill(&mut self.base_iv).map_err(|e| Error::Io(std::io::Error::other(e)))?;
        let key_id = self.key_id();
        let data = self.mmap.data_mut();
        data[..8].copy_from_slice(&key_id.to_be_bytes());
        data[8..20].copy_from_slice(&self.base_iv);
        self.zero_next_entry();
        Ok(())
    }

    pub fn path(&self) -> &Path {
        self.mmap.path()
    }

    pub fn path_buf(&self) -> PathBuf {
        self.mmap.path().to_path_buf()
    }

    /// The mapped bytes.
    pub fn data(&self) -> &[u8] {
        self.mmap.data()
    }

    /// Logical size of the file.
    pub fn size(&self) -> u32 {
        self.size
    }

    /// The data key id, 0 when not encrypted.
    pub fn key_id(&self) -> u64 {
        self.data_key.as_ref().map_or(0, |k| k.key_id)
    }

    pub fn encryption_enabled(&self) -> bool {
        self.data_key.is_some()
    }

    /// IV for the entry at `offset`: the base IV followed by the offset.
    fn iv(&self, offset: u32) -> [u8; BLOCK_SIZE] {
        let mut iv = [0u8; BLOCK_SIZE];
        iv[..12].copy_from_slice(&self.base_iv);
        iv[12..].copy_from_slice(&offset.to_be_bytes());
        iv
    }

    pub(crate) fn decrypt_kv(&self, buf: &[u8], offset: u32) -> Result<Vec<u8>> {
        let dk = self.data_key.as_ref().expect("encryption enabled");
        xor_block_alloc(buf, &dk.data, &self.iv(offset))
    }

    /// Appends the encoding of `e`, as written at `offset`, to `buf`.
    /// Returns its length.
    pub fn encode_entry(&self, buf: &mut Vec<u8>, e: &Entry, offset: u32) -> Result<usize> {
        let start = buf.len();
        let h = Header {
            klen: e.key.len() as u32,
            vlen: e.value.len() as u32,
            expires_at: e.expires_at,
            meta: e.meta,
            user_meta: e.user_meta,
        };
        let mut enc = [0u8; Header::MAX_SIZE];
        let n = h.encode(&mut enc);
        buf.extend_from_slice(&enc[..n]);
        match &self.data_key {
            Some(dk) => {
                // Key and value are encrypted as one CTR stream.
                let mut kv = Vec::with_capacity(e.key.len() + e.value.len());
                kv.extend_from_slice(&e.key);
                kv.extend_from_slice(&e.value);
                xor_block_stream(buf, &kv, &dk.data, &self.iv(offset))
                    .context("Error while encoding entry for vlog.")?;
            }
            None => {
                buf.extend_from_slice(&e.key);
                buf.extend_from_slice(&e.value);
            }
        }
        let crc = crc32c::crc32c(&buf[start..]);
        buf.extend_from_slice(&crc.to_be_bytes());
        Ok(buf.len() - start)
    }

    /// Writes `e` at [`LogFile::write_at`] and advances it. `buf` is scratch space.
    pub fn write_entry(&mut self, buf: &mut Vec<u8>, e: &Entry) -> Result<()> {
        buf.clear();
        let n = self.encode_entry(buf, e, self.write_at)?;
        let at = self.write_at as usize;
        let data = self.mmap.data_mut();
        let Some(dst) = data.get_mut(at..at + n) else {
            return Err(Error::corruption(format!(
                "log file {} full: entry of {n} bytes at {at}, size {}",
                self.fid,
                data.len()
            )));
        };
        dst.copy_from_slice(buf);
        self.write_at += n as u32;
        self.zero_next_entry();
        Ok(())
    }

    /// Decodes the entry at the start of `buf`, which was read at `offset`.
    /// The CRC is not checked.
    pub fn decode_entry(&self, buf: &[u8], offset: u32) -> Result<Entry> {
        let (h, hlen) = Header::decode(buf)?;
        let kv_len = h.klen as usize + h.vlen as usize;
        let kv = buf
            .get(hlen..hlen + kv_len)
            .ok_or_else(|| Error::corruption("entry shorter than its header says"))?;
        let kv = if self.encryption_enabled() {
            self.decrypt_kv(kv, offset)?
        } else {
            kv.to_vec()
        };
        Ok(entry_from(h, hlen, offset, kv))
    }

    /// The bytes `p` points at, which must lie within the file.
    pub fn read(&self, p: ValuePointer) -> Result<&[u8]> {
        let data = self.mmap.data();
        let (start, end) = (p.offset as u64, p.offset as u64 + p.len as u64);
        // The logical size can be below the mapping after truncation.
        if start >= data.len() as u64 || end > data.len() as u64 || end > self.size as u64 {
            return Err(Error::Eof);
        }
        Ok(&data[start as usize..end as usize])
    }

    /// Zeroes the header-sized region after the last entry, so a replay after
    /// a crash stops there.
    fn zero_next_entry(&mut self) {
        let at = self.write_at as usize;
        let data = self.mmap.data_mut();
        if at < data.len() {
            let end = (at + Header::MAX_SIZE).min(data.len());
            data[at..end].fill(0);
        }
    }

    /// Flushes the mapping to disk.
    pub fn sync(&self) -> Result<()> {
        self.mmap.sync()
    }

    /// Resizes the file to `end` bytes, if it is not that size already.
    pub fn truncate(&mut self, end: u64) -> Result<()> {
        let actual = self
            .mmap
            .file()
            .metadata()
            .with_context(|| format!("while file.stat on file: {}", self.path().display()))?
            .len();
        if actual == end {
            return Ok(());
        }
        if self.mmap.is_read_only() {
            return Err(Error::TruncateNeeded)
                .with_context(|| format!("{} is read-only", self.path().display()));
        }
        self.size = end as u32;
        self.mmap.truncate(end)
    }

    /// Syncs (if `sync_writes`) and truncates the file to `offset` once it is
    /// fully written (Go's `doneWriting`).
    pub fn done_writing(&mut self, offset: u32, sync_writes: bool) -> Result<()> {
        if sync_writes {
            self.sync()
                .with_context(|| format!("Unable to sync value log: {}", self.path().display()))?;
        }
        self.truncate(offset as u64)
            .with_context(|| format!("Unable to truncate file: {}", self.path().display()))
    }

    /// Unmaps and removes the file.
    pub fn delete(self) -> Result<()> {
        self.mmap.delete()
    }

    /// Syncs, unmaps and closes the file, optionally resizing it first.
    pub fn close(self, max_size: Option<u64>) -> Result<()> {
        self.mmap.close(max_size)
    }

    /// Replays the entries from `offset` (0 = just after the header), calling
    /// `f` for each complete entry: every plain entry, and the entries of each
    /// transaction once its finish marker is seen (the marker itself is not
    /// passed). Stops at the end of the file, at a zeroed, truncated or
    /// corrupt entry, or at an incomplete transaction.
    ///
    /// Returns the offset just past the last entry that was passed to `f`
    /// (or the last finish marker): everything after it can be truncated.
    pub fn iterate(
        &self,
        offset: u32,
        mut f: impl FnMut(&Entry, ValuePointer) -> Result<()>,
    ) -> Result<u32> {
        let offset = if offset == 0 {
            VLOG_HEADER_SIZE
        } else {
            offset
        };
        let data = self.mmap.data();
        let mut record_offset = offset;
        let mut last_commit = 0u64;
        let mut valid_end = offset;
        let mut txn: Vec<(Entry, ValuePointer)> = Vec::new();

        loop {
            let e = match self.read_entry(data, record_offset)? {
                Some(e) if !e.key.is_empty() => e,
                // End of file, a truncated or corrupt entry, or a zeroed one.
                _ => break,
            };
            let vp = ValuePointer {
                fid: self.fid,
                len: (e.hlen + e.key.len() + e.value.len() + CRC_SIZE) as u32,
                offset: e.offset,
            };
            record_offset += vp.len;

            if e.meta & BIT_TXN != 0 {
                let txn_ts = parse_ts(&e.key);
                if last_commit == 0 {
                    last_commit = txn_ts;
                }
                if last_commit != txn_ts {
                    break;
                }
                txn.push((e, vp));
            } else if e.meta & BIT_FIN_TXN != 0 {
                let ts = std::str::from_utf8(&e.value)
                    .ok()
                    .and_then(|s| s.parse::<u64>().ok());
                if ts != Some(last_commit) {
                    break;
                }
                // The transaction is complete: hand over its entries.
                last_commit = 0;
                valid_end = record_offset;
                for (e, vp) in txn.drain(..) {
                    f(&e, vp).with_context(|| {
                        format!("{}: Iteration function", self.path().display())
                    })?;
                }
            } else {
                if last_commit != 0 {
                    // A plain entry inside a transaction (most likely moved by
                    // GC): the transaction is incomplete.
                    break;
                }
                valid_end = record_offset;
                f(&e, vp)
                    .with_context(|| format!("{}: Iteration function", self.path().display()))?;
            }
        }
        Ok(valid_end)
    }

    /// Reads and checks the entry at `offset` (Go's `safeRead.Entry`).
    /// `Ok(None)` means the end of the valid log: end of file, a truncated
    /// entry, an oversized key or a CRC mismatch. A malformed varint is an
    /// error, as in Go.
    fn read_entry(&self, data: &[u8], offset: u32) -> Result<Option<Entry>> {
        let Some(buf) = data.get(offset as usize..) else {
            return Ok(None);
        };
        let Some((h, hlen)) = read_header(buf)? else {
            return Ok(None);
        };
        if h.klen > MAX_KEY_LEN {
            return Ok(None);
        }
        let kv_end = hlen + h.klen as usize + h.vlen as usize;
        let Some(crc_bytes) = buf.get(kv_end..kv_end + CRC_SIZE) else {
            return Ok(None);
        };
        let crc = u32::from_be_bytes(crc_bytes.try_into().unwrap());
        if crc != crc32c::crc32c(&buf[..kv_end]) {
            return Ok(None);
        }
        let kv = &buf[hlen..kv_end];
        let kv = if self.encryption_enabled() {
            self.decrypt_kv(kv, offset)?
        } else {
            kv.to_vec()
        };
        Ok(Some(entry_from(h, hlen, offset, kv)))
    }
}

fn entry_from(h: Header, hlen: usize, offset: u32, mut kv: Vec<u8>) -> Entry {
    let value = kv.split_off(h.klen as usize);
    Entry {
        key: kv,
        value,
        expires_at: h.expires_at,
        user_meta: h.user_meta,
        meta: h.meta,
        offset,
        hlen,
        ..Default::default()
    }
}

/// Decodes a header like Go's `header.DecodeFrom` over a reader: `Ok(None)` if
/// the input ends first, an error if a varint overflows.
fn read_header(buf: &[u8]) -> Result<Option<(Header, usize)>> {
    if buf.len() < 2 {
        return Ok(None);
    }
    let mut i = 2;
    let mut next = || -> Result<Option<u64>> {
        let mut x = 0u64;
        for (j, &b) in buf[i..].iter().enumerate() {
            if j == MAX_VARINT_LEN64 || (j == MAX_VARINT_LEN64 - 1 && b > 1) {
                return Err(Error::corruption(
                    "binary: varint overflows a 64-bit integer",
                ));
            }
            if b < 0x80 {
                i += j + 1;
                return Ok(Some(x | (b as u64) << (7 * j)));
            }
            x |= ((b & 0x7f) as u64) << (7 * j);
        }
        Ok(None)
    };
    let (Some(klen), Some(vlen), Some(expires_at)) = (next()?, next()?, next()?) else {
        return Ok(None);
    };
    Ok(Some((
        Header {
            // Truncated to u32 like Go.
            klen: klen as u32,
            vlen: vlen as u32,
            expires_at,
            meta: buf[0],
            user_meta: buf[1],
        },
        i,
    )))
}
