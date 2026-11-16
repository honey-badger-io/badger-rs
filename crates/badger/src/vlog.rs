//! The value log: values too big for the LSM tree, stored in `<fid>.vlog`
//! files (same format as the WAL, see [`crate::log_file`]) and referenced by
//! [`ValuePointer`]s. Port of Go's `value.go` without garbage collection
//! (Phase 6).
//!
//! Only the newest file is written. When it passes `value_log_file_size` or
//! `value_log_max_entries`, it is truncated to its data and a new file is
//! started. A request's entries always land in one file, so a transaction is
//! never split across files.
//!
//! Differences from Go: each file sits behind an `RwLock` that the writer
//! takes for every append (Go appends without it, guarded by the write
//! offset); readers hold the read side while they use the value. The value
//! threshold is fixed (`vlog_percentile`, the dynamic threshold, is not
//! ported yet).

use std::borrow::Cow;
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, RwLock};

use crate::error::{Error, Result, ResultExt};
use crate::key_registry::KeyRegistry;
use crate::log_file::{LogFile, VLOG_HEADER_SIZE};
use crate::options::Options;
use crate::structs::{BIT_FIN_TXN, BIT_TXN, Entry, Header, ValuePointer};

/// Extension of value log files.
pub const VLOG_FILE_EXT: &str = ".vlog";

/// Offsets are `u32`, so no file can grow past this.
const MAX_VLOG_FILE_SIZE: u64 = u32::MAX as u64;

/// Path of value log file `fid` (Go's `vlogFilePath`).
pub fn vlog_file_path(dir: &Path, fid: u32) -> PathBuf {
    dir.join(format!("{fid:06}{VLOG_FILE_EXT}"))
}

type SharedFile = Arc<RwLock<LogFile>>;

pub(crate) struct ValueLog {
    dir: PathBuf,
    /// Open files by id. Each file has its own lock (Go's `logFile.lock`).
    files: RwLock<BTreeMap<u32, SharedFile>>,
    /// The file being written: the highest id.
    max_fid: AtomicU32,
    /// End of the data in the file being written (Go's `writableLogOffset`).
    writable_offset: AtomicU32,
    /// Entries written to the current file.
    num_entries_written: AtomicU32,
    registry: Arc<KeyRegistry>,
    read_only: bool,
    in_memory: bool,
    sync_writes: bool,
    verify_value_checksum: bool,
    file_size: u64,
    max_entries: u32,
}

impl ValueLog {
    /// Opens the value log files in `opt.value_dir` (Go's `valueLog.open`).
    /// Unless read-only, the newest file is truncated after its last valid
    /// entry and a new file is created for writes.
    pub fn open(opt: &Options, registry: Arc<KeyRegistry>) -> Result<ValueLog> {
        let vlog = ValueLog {
            dir: opt.value_dir.clone(),
            files: RwLock::new(BTreeMap::new()),
            max_fid: AtomicU32::new(0),
            writable_offset: AtomicU32::new(0),
            num_entries_written: AtomicU32::new(0),
            registry,
            read_only: opt.read_only,
            in_memory: opt.in_memory,
            sync_writes: opt.sync_writes,
            verify_value_checksum: opt.verify_value_checksum,
            file_size: opt.value_log_file_size,
            max_entries: opt.value_log_max_entries,
        };
        if opt.in_memory {
            return Ok(vlog);
        }
        let fids = vlog.list_fids()?;
        let Some(&max_fid) = fids.last() else {
            if !vlog.read_only {
                vlog.create_vlog_file()
                    .context("Error while creating log file in valueLog.open")?;
            }
            return Ok(vlog);
        };
        vlog.max_fid.store(max_fid, Ordering::Relaxed);
        let mut files = BTreeMap::new();
        for fid in fids {
            let path = vlog_file_path(&vlog.dir, fid);
            let (lf, _) = LogFile::open(
                &path,
                fid,
                false,
                vlog.read_only,
                2 * vlog.file_size,
                &vlog.registry,
            )
            .with_context(|| format!("Open existing file: {}", path.display()))?;
            // Empty files (only a header) are removed, except the newest.
            if lf.size() == VLOG_HEADER_SIZE && fid != max_fid && !vlog.read_only {
                log::info!("Deleting empty file: {}", path.display());
                lf.delete().with_context(|| {
                    format!("while trying to delete empty file: {}", path.display())
                })?;
                continue;
            }
            files.insert(fid, Arc::new(RwLock::new(lf)));
        }
        if vlog.read_only {
            *vlog.files.write().unwrap() = files;
            return Ok(vlog);
        }

        // Cut the newest file after its last complete entry (a crash may have
        // left a partial one), then always start a new file.
        {
            let mut last = files[&max_fid].write().unwrap();
            let end = last
                .iterate(VLOG_HEADER_SIZE, |_, _| Ok(()))
                .with_context(|| format!("while iterating over: {}", last.path().display()))?;
            last.truncate(end as u64).with_context(|| {
                format!(
                    "while truncating last value log file: {}",
                    last.path().display()
                )
            })?;
            last.write_at = end;
        }
        *vlog.files.write().unwrap() = files;
        vlog.create_vlog_file()
            .context("Error while creating log file in valueLog.open")?;
        Ok(vlog)
    }

    /// Ids of the `*.vlog` files in the directory, ascending.
    fn list_fids(&self) -> Result<Vec<u32>> {
        let mut fids = Vec::new();
        for entry in fs::read_dir(&self.dir)
            .with_context(|| format!("Unable to open log dir: {}", self.dir.display()))?
        {
            let name = entry?.file_name();
            let name = name.to_string_lossy();
            let Some(stem) = name.strip_suffix(VLOG_FILE_EXT) else {
                continue;
            };
            let fid: u32 = stem
                .parse()
                .map_err(|e| Error::corruption(format!("{e}")))
                .with_context(|| format!("{name}: Unable to parse log id."))?;
            fids.push(fid);
        }
        fids.sort_unstable();
        if fids.windows(2).any(|w| w[0] == w[1]) {
            return Err(Error::corruption(
                "Duplicate vlog file found. Please delete one.",
            ));
        }
        Ok(fids)
    }

    /// Starts the next file (Go's `createVlogFile`).
    fn create_vlog_file(&self) -> Result<SharedFile> {
        let fid = self.max_fid.load(Ordering::Relaxed) + 1;
        let path = vlog_file_path(&self.dir, fid);
        let (lf, new) = LogFile::open(&path, fid, true, false, 2 * self.file_size, &self.registry)?;
        if !new {
            return Err(Error::corruption(format!(
                "{} already exists",
                path.display()
            )));
        }
        let lf = Arc::new(RwLock::new(lf));
        let mut files = self.files.write().unwrap();
        files.insert(fid, Arc::clone(&lf));
        self.max_fid.store(fid, Ordering::Release);
        self.writable_offset
            .store(VLOG_HEADER_SIZE, Ordering::Release);
        self.num_entries_written.store(0, Ordering::Relaxed);
        Ok(lf)
    }

    fn file(&self, fid: u32) -> Result<SharedFile> {
        self.files
            .read()
            .unwrap()
            .get(&fid)
            .cloned()
            .ok_or_else(|| Error::corruption(format!("file with ID: {fid} not found")))
    }

    /// Reads the value `vp` points at and passes it to `f` (Go's `Read`).
    /// Unencrypted values are passed straight from the mapped file.
    pub fn read<R>(&self, vp: ValuePointer, f: impl FnOnce(&[u8]) -> R) -> Result<R> {
        let lf = self.file(vp.fid)?;
        if !self.read_only && vp.fid == self.max_fid.load(Ordering::Acquire) {
            let woffset = self.writable_offset.load(Ordering::Acquire);
            if vp.offset >= woffset {
                return Err(Error::corruption(format!(
                    "Invalid value pointer offset: {} greater than current offset: {woffset}",
                    vp.offset
                )));
            }
        }
        let lf = lf.read().unwrap();
        let buf = lf.read(vp)?;
        if buf.len() < 4 {
            return Err(Error::corruption(format!("Invalid read: vp: {vp:?}")));
        }
        if self.verify_value_checksum {
            let (data, crc) = buf.split_at(buf.len() - 4);
            let expected = u32::from_be_bytes(crc.try_into().unwrap());
            let actual = crc32c::crc32c(data);
            if actual != expected {
                return Err(Error::ChecksumMismatch {
                    actual: actual as u64,
                    expected: expected as u64,
                })
                .with_context(|| format!("value corrupted for vp: {vp:?}"));
            }
        }
        let (h, hlen) = Header::decode(buf)?;
        let kv = &buf[hlen..];
        let kv: Cow<'_, [u8]> = if lf.encryption_enabled() {
            Cow::Owned(lf.decrypt_kv(kv, vp.offset)?)
        } else {
            Cow::Borrowed(kv)
        };
        let (klen, vlen) = (h.klen as usize, h.vlen as usize);
        let Some(value) = kv.get(klen..klen + vlen) else {
            return Err(Error::corruption(format!(
                "Invalid read: Len: {} read at:[{klen}:{}]",
                kv.len(),
                klen + vlen
            )));
        };
        Ok(f(value))
    }

    /// Checks that no request would push a file past the 4 GiB offset limit,
    /// before anything is written.
    fn validate_writes(&self, reqs: &[&mut Vec<Entry>]) -> Result<()> {
        let mut offset = self.writable_offset.load(Ordering::Acquire) as u64;
        for entries in reqs {
            let size: u64 = entries
                .iter()
                .map(|e| (Header::MAX_SIZE + e.key.len() + e.value.len() + 4) as u64)
                .sum();
            let estimated = offset + size;
            if estimated > MAX_VLOG_FILE_SIZE {
                return Err(Error::InvalidRequest).with_context(|| {
                    format!(
                        "Request size offset {estimated} is bigger than maximum offset {MAX_VLOG_FILE_SIZE}"
                    )
                });
            }
            // A new file is started once the estimate passes the file size.
            offset = if estimated >= self.file_size {
                0
            } else {
                estimated
            };
        }
        Ok(())
    }

    /// Writes the values of `reqs` that are at least `threshold` bytes and
    /// returns a pointer per entry (a zero pointer for values that stay in
    /// the LSM tree), Go's `valueLog.write`.
    pub fn write(
        &self,
        reqs: &mut [&mut Vec<Entry>],
        threshold: i64,
    ) -> Result<Vec<Vec<ValuePointer>>> {
        if self.in_memory {
            return Ok(reqs
                .iter()
                .map(|r| vec![ValuePointer::default(); r.len()])
                .collect());
        }
        self.validate_writes(reqs)
            .context("while validating writes")?;
        let mut cur = self.file(self.max_fid.load(Ordering::Acquire))?;
        let mut buf = Vec::new();
        let mut all_ptrs = Vec::with_capacity(reqs.len());
        for entries in reqs.iter_mut() {
            let mut ptrs = Vec::with_capacity(entries.len());
            let mut written = 0;
            for e in entries.iter_mut() {
                if e.skip_vlog_and_set_threshold(threshold) {
                    ptrs.push(ValuePointer::default());
                    continue;
                }
                let mut lf = cur.write().unwrap();
                let offset = lf.write_at;
                // Transaction marks stay out of the value log (GC must be able
                // to read a file on its own); the WAL keeps them.
                let meta = e.meta;
                e.meta &= !(BIT_TXN | BIT_FIN_TXN);
                buf.clear();
                let len = lf.encode_entry(&mut buf, e, offset);
                e.meta = meta;
                let len = len?;
                lf.append(&buf)?;
                self.writable_offset.store(lf.write_at, Ordering::Release);
                ptrs.push(ValuePointer {
                    fid: lf.fid,
                    len: len as u32,
                    offset,
                });
                written += 1;
            }
            self.num_entries_written
                .fetch_add(written, Ordering::Relaxed);
            all_ptrs.push(ptrs);
            // Rotate between requests, so a request stays in one file.
            cur = self.maybe_rotate(cur)?;
        }
        if self.sync_writes {
            cur.read().unwrap().sync()?;
        }
        Ok(all_ptrs)
    }

    /// Finishes the current file and starts a new one if it is full (Go's
    /// `toDisk`).
    fn maybe_rotate(&self, cur: SharedFile) -> Result<SharedFile> {
        let woffset = self.writable_offset.load(Ordering::Acquire);
        if (woffset as u64) <= self.file_size
            && self.num_entries_written.load(Ordering::Relaxed) <= self.max_entries
        {
            return Ok(cur);
        }
        cur.write()
            .unwrap()
            .done_writing(woffset, self.sync_writes)?;
        self.create_vlog_file()
    }

    /// Flushes the file being written to disk (unless every write is synced
    /// anyway).
    pub fn sync(&self) -> Result<()> {
        if self.sync_writes || self.in_memory || self.read_only {
            return Ok(());
        }
        match self.file(self.max_fid.load(Ordering::Acquire)) {
            Ok(lf) => lf.read().unwrap().sync(),
            Err(_) => Ok(()),
        }
    }

    /// Syncs and closes every file, truncating the one being written to its
    /// data (Go's `valueLog.Close`).
    pub fn close(&self) -> Result<()> {
        let files = std::mem::take(&mut *self.files.write().unwrap());
        let max_fid = self.max_fid.load(Ordering::Acquire);
        let mut first_err = None;
        for (fid, lf) in files {
            let mut lf = lf.write().unwrap();
            let res = if !self.read_only && fid == max_fid {
                let end = self.writable_offset.load(Ordering::Acquire);
                lf.sync().and_then(|_| lf.truncate(end as u64))
            } else if !self.read_only {
                lf.sync()
            } else {
                Ok(())
            };
            if let Err(e) = res {
                first_err.get_or_insert(e);
            }
        }
        first_err.map_or(Ok(()), Err)
    }

    /// Ids of the open files, ascending.
    #[allow(dead_code)] // used by GC (Phase 6)
    pub fn sorted_fids(&self) -> Vec<u32> {
        self.files.read().unwrap().keys().copied().collect()
    }
}
