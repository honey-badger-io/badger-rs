//! The value log: values too big for the LSM tree, stored in `<fid>.vlog`
//! files (same format as the WAL, see [`crate::log_file`]) and referenced by
//! [`ValuePointer`]s. Port of Go's `value.go`.
//!
//! Only the newest file is written. When it passes `value_log_file_size` or
//! `value_log_max_entries`, it is truncated to its data and a new file is
//! started. A request's entries always land in one file, so a transaction is
//! never split across files.
//!
//! Garbage collection ([`run_gc`]): compaction counts the bytes of every
//! value pointer it drops in the discard stats ([`crate::discard`]). GC picks
//! the file with the most discardable bytes, writes its still-live entries
//! again (they land in the newest file and the LSM tree gets new pointers),
//! then deletes it, or defers that until no iterator is open.
//!
//! An old version written back also gets the key's newest version written
//! again after it, so a delete dropped by compaction further down cannot
//! uncover it (Go has that bug). Versions no read can see may still point
//! into a deleted file afterwards, as in Go: reading their values (only an
//! all-versions iteration can) fails.
//!
//! Differences from Go: each file sits behind an `RwLock` that the writer
//! takes for every append (Go appends without it, guarded by the write
//! offset); readers hold the read side while they use the value. The value
//! threshold is fixed (`vlog_percentile`, the dynamic threshold, is not
//! ported).

use std::borrow::Cow;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use crate::db::DbInner;
use crate::discard::DiscardStats;
use crate::error::{Error, Result, ResultExt};
use crate::iterator::is_deleted_or_expired;
use crate::key_registry::KeyRegistry;
use crate::log_file::{LogFile, VLOG_HEADER_SIZE};
use crate::options::Options;
use crate::structs::{BIT_FIN_TXN, BIT_TXN, BIT_VALUE_POINTER, Entry, Header, ValuePointer};
use crate::util::OwnedValueStruct;
use crate::util::keys::{key_with_ts, parse_key, parse_ts};

/// Extension of value log files.
pub const VLOG_FILE_EXT: &str = ".vlog";

/// Offsets are `u32`, so no file can grow past this.
const MAX_VLOG_FILE_SIZE: u64 = u32::MAX as u64;

/// Path of value log file `fid` (Go's `vlogFilePath`).
pub fn vlog_file_path(dir: &Path, fid: u32) -> PathBuf {
    dir.join(format!("{fid:06}{VLOG_FILE_EXT}"))
}

type SharedFile = Arc<RwLock<LogFile>>;

/// The open files.
#[derive(Default)]
struct Files {
    /// By id. Each file has its own lock (Go's `logFile.lock`).
    map: BTreeMap<u32, SharedFile>,
    /// Rewritten by GC, deleted once no iterator is open.
    to_be_deleted: Vec<u32>,
}

pub(crate) struct ValueLog {
    dir: PathBuf,
    files: RwLock<Files>,
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
    /// `None` in read-only and in-memory mode.
    discard_stats: Option<Mutex<DiscardStats>>,
    /// Open iterators: GC defers deleting files while any is open.
    num_active_iterators: AtomicI64,
    /// Held while GC runs; only one GC at a time.
    gc_lock: Mutex<()>,
    /// While a GC rewrite runs, the DB's max version at its start
    /// (compaction must not drop newer versions; see `subcompact`).
    gc_active: AtomicBool,
    gc_discard_ts: AtomicU64,
}

impl ValueLog {
    /// Opens the value log files in `opt.value_dir` (Go's `valueLog.open`).
    /// Unless read-only, the newest file is truncated after its last valid
    /// entry and a new file is created for writes.
    pub fn open(opt: &Options, registry: Arc<KeyRegistry>) -> Result<ValueLog> {
        let vlog = ValueLog {
            dir: opt.value_dir.clone(),
            files: RwLock::new(Files::default()),
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
            discard_stats: None,
            num_active_iterators: AtomicI64::new(0),
            gc_lock: Mutex::new(()),
            gc_active: AtomicBool::new(false),
            gc_discard_ts: AtomicU64::new(0),
        };
        if opt.in_memory {
            return Ok(vlog);
        }
        let mut vlog = vlog;
        if !opt.read_only {
            vlog.discard_stats = Some(Mutex::new(DiscardStats::open(&opt.value_dir)?));
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
            vlog.files.write().unwrap().map = files;
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
        vlog.files.write().unwrap().map = files;
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
        files.map.insert(fid, Arc::clone(&lf));
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
            .map
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
        self.read_from(&lf, vp, f)
    }

    /// Reads the value `vp` points at in `lf`, a file the caller holds.
    fn read_from<R>(
        &self,
        lf: &LogFile,
        vp: ValuePointer,
        f: impl FnOnce(&[u8]) -> R,
    ) -> Result<R> {
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
        let files = std::mem::take(&mut self.files.write().unwrap().map);
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
        if let Some(ds) = &self.discard_stats
            && let Err(e) = ds.lock().unwrap().sync()
        {
            first_err.get_or_insert(e);
        }
        first_err.map_or(Ok(()), Err)
    }

    /// Ids of the open files not waiting for deletion, ascending.
    #[allow(dead_code)] // used by tests
    pub fn sorted_fids(&self) -> Vec<u32> {
        let files = self.files.read().unwrap();
        files
            .map
            .keys()
            .filter(|fid| !files.to_be_deleted.contains(fid))
            .copied()
            .collect()
    }

    /// Adds bytes that compaction dropped to the discard stats (Go's
    /// `updateDiscardStats`).
    pub fn update_discard_stats(&self, stats: &HashMap<u32, i64>) {
        let Some(ds) = &self.discard_stats else {
            return;
        };
        let mut ds = ds.lock().unwrap();
        for (&fid, &discard) in stats {
            if let Err(e) = ds.update(fid, discard) {
                log::error!("while updating discard stats: {e}");
            }
        }
    }

    /// The discardable bytes recorded for file `fid`.
    #[allow(dead_code)] // used by tests
    pub(crate) fn discard_of(&self, fid: u32) -> i64 {
        self.discard_stats
            .as_ref()
            .map_or(0, |ds| ds.lock().unwrap().update(fid, 0).unwrap_or(0))
    }

    /// While a GC rewrite runs, the max version at its start.
    pub fn gc_discard_ts(&self) -> Option<u64> {
        self.gc_active
            .load(Ordering::SeqCst)
            .then(|| self.gc_discard_ts.load(Ordering::SeqCst))
    }

    pub fn incr_iterator_count(&self) {
        self.num_active_iterators.fetch_add(1, Ordering::SeqCst);
    }

    /// The last open iterator deletes the files GC rewrote meanwhile.
    pub fn decr_iterator_count(&self) -> Result<()> {
        if self.num_active_iterators.fetch_sub(1, Ordering::SeqCst) != 1 {
            return Ok(());
        }
        let lfs: Vec<SharedFile> = {
            let mut files = self.files.write().unwrap();
            let ids = std::mem::take(&mut files.to_be_deleted);
            ids.iter().filter_map(|id| files.map.remove(id)).collect()
        };
        lfs.iter().try_for_each(|lf| self.delete_log_file(lf))
    }

    fn delete_log_file(&self, lf: &SharedFile) -> Result<()> {
        let mut lf = lf.write().unwrap();
        if let Some(ds) = &self.discard_stats {
            ds.lock().unwrap().update(lf.fid, -1)?;
        }
        lf.delete_in_place()
    }

    /// Deletes every file and starts again at file 1 (Go's `dropAll`).
    /// Writes must be blocked. Returns the number of files deleted.
    pub fn drop_all(&self) -> Result<usize> {
        if self.in_memory {
            return Ok(0);
        }
        let lfs: Vec<SharedFile> = {
            let mut files = self.files.write().unwrap();
            files.to_be_deleted.clear();
            std::mem::take(&mut files.map).into_values().collect()
        };
        for lf in &lfs {
            self.delete_log_file(lf)?;
        }
        self.max_fid.store(0, Ordering::Release);
        log::info!("Value logs deleted. Creating value log file: 1");
        self.create_vlog_file()?;
        Ok(lfs.len())
    }

    /// The file with the most discardable data, if that is at least
    /// `discard_ratio` of it and it is not the file being written (Go's
    /// `pickLog`).
    fn pick_log(&self, discard_ratio: f64) -> Option<SharedFile> {
        let ds = self.discard_stats.as_ref()?;
        let files = self.files.read().unwrap();
        loop {
            let (fid, discard) = ds.lock().unwrap().max_discard();
            // File ids start at 1.
            if fid == 0 {
                log::debug!("No file with discard stats");
                return None;
            }
            let Some(lf) = files.map.get(&fid) else {
                // Deleted, but compaction still found pointers into it.
                let _ = ds.lock().unwrap().update(fid, -1);
                continue;
            };
            let size = lf.read().unwrap().size() as f64;
            let threshold = discard_ratio * size;
            if (discard as f64) < threshold {
                log::debug!(
                    "Discard: {discard} less than threshold: {threshold:.0} for file: {fid}"
                );
                return None;
            }
            if fid < self.max_fid.load(Ordering::Acquire) {
                log::info!("Found value log max discard fid: {fid} discard: {discard}");
                return Some(Arc::clone(lf));
            }
            return None;
        }
    }
}

/// Runs value log GC once (Go's `runGC`): rewrites the file with the most
/// discardable data, if it has at least `discard_ratio` of it.
pub(crate) fn run_gc(db: &DbInner, discard_ratio: f64) -> Result<()> {
    let vlog = &db.vlog;
    let Ok(_gc) = vlog.gc_lock.try_lock() else {
        return Err(Error::Rejected);
    };
    let Some(lf) = vlog.pick_log(discard_ratio) else {
        return Err(Error::NoRewrite);
    };
    let fid = lf.read().unwrap().fid;
    rewrite(db, fid, &lf)?;
    if let Some(ds) = &vlog.discard_stats {
        ds.lock().unwrap().update(fid, -1)?;
    }
    Ok(())
}

/// Rewrites file `fid` directly (Go's tests call `vlog.rewrite`).
#[cfg(all(test, not(miri)))]
pub(crate) fn rewrite_for_test(db: &DbInner, fid: u32) -> Result<()> {
    let lf = db.vlog.file(fid)?;
    rewrite(db, fid, &lf)
}

/// Clears `gc_active` when the rewrite ends, however it ends.
struct GcActive<'a>(&'a ValueLog);

impl Drop for GcActive<'_> {
    fn drop(&mut self) {
        self.0.gc_active.store(false, Ordering::SeqCst);
    }
}

/// Whether a value log entry is garbage, given what the LSM tree holds for
/// its key and version (Go's `discardEntry`).
fn discard_entry(e: &Entry, vs: Option<&OwnedValueStruct>) -> bool {
    let Some(vs) = vs else { return true };
    // A different version: this one was dropped.
    vs.version != parse_ts(&e.key)
        // The LSM tree holds the value itself.
        || vs.meta & BIT_VALUE_POINTER == 0
        // A transaction's finish mark.
        || vs.meta & BIT_FIN_TXN != 0
}

/// Writes the live entries of file `fid` again and deletes it (Go's
/// `rewrite`).
fn rewrite(db: &DbInner, fid: u32, lf: &SharedFile) -> Result<()> {
    let vlog = &db.vlog;
    {
        let files = vlog.files.read().unwrap();
        if files.to_be_deleted.contains(&fid) {
            return Err(Error::corruption(format!(
                "value log file already marked for deletion fid: {fid}"
            )));
        }
        let max_fid = vlog.max_fid.load(Ordering::Acquire);
        assert!(
            fid < max_fid,
            "fid to move: {fid}. Current max fid: {max_fid}"
        );
    }
    log::info!("Rewriting fid: {fid}");
    // Until the rewrite ends, compaction keeps versions newer than now: a
    // delete committed after this point must shadow the values written back.
    vlog.gc_discard_ts.store(db.max_version(), Ordering::SeqCst);
    vlog.gc_active.store(true, Ordering::SeqCst);
    let _active = GcActive(vlog);

    let max_count = db.opt.max_batch_count() as usize;
    let max_size = db.opt.max_batch_size() as i64;
    let mut wb: Vec<Entry> = Vec::with_capacity(1000);
    let mut size = 0i64;
    let (mut count, mut moved) = (0usize, 0usize);
    {
        let file = lf.read().unwrap();
        file.iterate(0, |e, vp| {
            count += 1;
            if count.is_multiple_of(100_000) {
                log::debug!("Processing entry {count}");
            }
            if is_deleted_or_expired(e.meta, e.expires_at) {
                return Ok(());
            }
            let vs = db.get(&e.key)?;
            if discard_entry(e, vs.as_ref()) {
                return Ok(());
            }
            let vs = vs.unwrap();
            if vs.value.is_empty() {
                return Err(Error::corruption(format!("Empty value: {vs:?}")));
            }
            let lsm_vp = ValuePointer::decode(&vs.value)?;
            // The LSM tree points at a newer copy (or, after GC of an older
            // file, at an older one that compaction will drop).
            if lsm_vp.fid > fid || lsm_vp.offset > vp.offset {
                return Ok(());
            }
            if lsm_vp.fid == fid && lsm_vp.offset == vp.offset {
                moved += 1;
                let mut moves = vec![Entry {
                    key: e.key.clone(),
                    value: e.value.clone(),
                    expires_at: e.expires_at,
                    user_meta: e.user_meta,
                    // Keep the other bits.
                    meta: e.meta & !(BIT_VALUE_POINTER | BIT_TXN | BIT_FIN_TXN),
                    ..Default::default()
                }];
                // An older version written back lands above the newer ones.
                // Write the newest one again after it, or once compaction
                // drops that newest version (say, a delete) further down,
                // the old value would come back. (Go only guards against
                // deletes committed during the rewrite.)
                let user_key = parse_key(&e.key);
                if let Some(newest) = db.get(&key_with_ts(user_key, u64::MAX))?
                    && newest.version > parse_ts(&e.key)
                {
                    let value = if newest.meta & BIT_VALUE_POINTER != 0 {
                        let nvp = ValuePointer::decode(&newest.value)?;
                        if nvp.fid == fid {
                            vlog.read_from(&file, nvp, |v| v.to_vec())?
                        } else {
                            vlog.read(nvp, |v| v.to_vec())?
                        }
                    } else {
                        newest.value.clone()
                    };
                    moves.push(Entry {
                        key: key_with_ts(user_key, newest.version),
                        value,
                        expires_at: newest.expires_at,
                        user_meta: newest.user_meta,
                        meta: newest.meta & !(BIT_VALUE_POINTER | BIT_TXN | BIT_FIN_TXN),
                        ..Default::default()
                    });
                }
                // The value counts too (Go #1292).
                let es: i64 = moves
                    .iter_mut()
                    .map(|ne| {
                        ne.estimate_size_and_set_threshold(db.value_threshold())
                            + ne.value.len() as i64
                    })
                    .sum();
                if wb.len() + moves.len() >= max_count || size + es >= max_size {
                    db.batch_set(std::mem::take(&mut wb))?;
                    size = 0;
                }
                wb.extend(moves);
                size += es;
            }
            Ok(())
        })?;
    }

    // Write back in batches, halving them while they are too big.
    let mut batch_size = 1024;
    let mut loops = 0;
    let mut i = 0;
    while i < wb.len() {
        loops += 1;
        if batch_size == 0 {
            log::warn!("We shouldn't reach batch size of zero.");
            return Err(Error::NoRewrite);
        }
        let end = (i + batch_size).min(wb.len());
        match db.batch_set(wb[i..end].to_vec()) {
            Ok(()) => i = end,
            Err(e) if matches!(e.root(), Error::TxnTooBig) => batch_size /= 2,
            Err(e) => return Err(e),
        }
    }
    log::info!("Processed {} entries in {loops} loops", wb.len());
    log::info!("Total entries: {count}. Moved: {moved}");
    log::info!("Removing fid: {fid}");

    // The entries are in the LSM tree: the file can go, now or when the last
    // open iterator closes.
    let delete_now = {
        let mut files = vlog.files.write().unwrap();
        if !files.map.contains_key(&fid) {
            return Err(Error::corruption(format!("Unable to find fid: {fid}")));
        }
        if vlog.num_active_iterators.load(Ordering::SeqCst) == 0 {
            files.map.remove(&fid)
        } else {
            files.to_be_deleted.push(fid);
            None
        }
    };
    if let Some(lf) = delete_now {
        vlog.delete_log_file(&lf)?;
    }
    Ok(())
}
