//! Memtables: a [`Skiplist`] plus its write-ahead log. Port of Go's
//! `memtable.go` (the `logFile` part is in [`crate::log_file`]).
//!
//! Every write goes to the WAL (`<fid>.mem`, 5-digit zero-padded) and then to
//! the skiplist. When the DB opens, each WAL is replayed into a new skiplist
//! and truncated after its last complete entry or transaction. A WAL is
//! deleted once its memtable has been flushed to L0.
//!
//! Differences from Go:
//! - `put` takes `&self`: the WAL sits behind a mutex (only the single DB
//!   writer takes it) so a memtable can be shared as `Arc<MemTable>` with
//!   readers, which only touch the lock-free skiplist.
//! - Go deletes the WAL when the skiplist's refcount drops to zero. Here the
//!   memtable deletes it on drop only after [`MemTable::delete_wal_on_drop`],
//!   which the flush path calls once the L0 table is safely recorded. Without
//!   that, dropping a memtable just closes its WAL (e.g. on DB close, or in
//!   read-only mode).
//! - The L0 bytes-written metric is not recorded yet.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use crate::error::{Error, Result, ResultExt};
use crate::key_registry::KeyRegistry;
use crate::log_file::LogFile;
use crate::options::Options;
use crate::skl::{MAX_NODE_SIZE, Skiplist};
use crate::structs::{BIT_FIN_TXN, Entry};
use crate::util::keys::parse_ts;
use crate::util::value::ValueStruct;

/// Extension of memtable WAL files.
pub const MEM_FILE_EXT: &str = ".mem";

/// Arena size for a memtable: the memtable size plus room for one more batch
/// (Go's `arenaSize`), since a batch is applied even when it overshoots.
pub fn arena_size(opt: &Options) -> usize {
    (opt.mem_table_size + opt.max_batch_size() + opt.max_batch_count() * MAX_NODE_SIZE as u64)
        as usize
}

/// Path of the WAL with id `fid` (Go's `mtFilePath`).
pub fn mem_file_path(dir: &Path, fid: u32) -> PathBuf {
    dir.join(format!("{fid:05}{MEM_FILE_EXT}"))
}

/// See the module docs.
pub struct MemTable {
    sl: Arc<Skiplist>,
    /// `None` in in-memory mode.
    wal: Option<Mutex<Wal>>,
    max_version: AtomicU64,
    mem_table_size: u64,
    delete_wal: AtomicBool,
}

struct Wal {
    /// `None` only after the file was taken in `drop`.
    file: Option<LogFile>,
    /// Scratch buffer for encoding entries.
    buf: Vec<u8>,
}

impl Wal {
    fn file(&mut self) -> &mut LogFile {
        self.file.as_mut().expect("WAL is open")
    }
}

impl MemTable {
    /// Opens the memtable with WAL `fid`, replaying the WAL if it already
    /// existed. With `create`, a missing WAL is created; the bool is true for
    /// a new WAL.
    ///
    /// In read-only mode, a WAL that would need truncating is an error
    /// ([`Error::TruncateNeeded`]), as in Go.
    pub fn open(
        opt: &Options,
        registry: &KeyRegistry,
        fid: u32,
        create: bool,
    ) -> Result<(MemTable, bool)> {
        let mut mt = MemTable {
            sl: Arc::new(Skiplist::new(arena_size(opt))),
            wal: None,
            max_version: AtomicU64::new(0),
            mem_table_size: opt.mem_table_size,
            delete_wal: AtomicBool::new(false),
        };
        if opt.in_memory {
            return Ok((mt, true));
        }
        let path = mem_file_path(&opt.dir, fid);
        let (wal, new) = LogFile::open(
            &path,
            fid,
            create,
            opt.read_only,
            2 * opt.mem_table_size,
            registry,
        )
        .with_context(|| format!("While opening memtable: {}", path.display()))?;
        mt.wal = Some(Mutex::new(Wal {
            file: Some(wal),
            buf: Vec::new(),
        }));
        if !new {
            mt.replay(opt.read_only)
                .context("while updating skiplist")?;
        }
        Ok((mt, new))
    }

    /// Creates a new, empty memtable with WAL `fid` (Go's `newMemTable`).
    /// Fails if the WAL already exists.
    pub fn create(opt: &Options, registry: &KeyRegistry, fid: u32) -> Result<MemTable> {
        let (mt, new) = Self::open(opt, registry, fid, true).context("newMemTable")?;
        if !new {
            return Err(Error::InvalidRequest).with_context(|| {
                format!(
                    "File {} already exists",
                    mem_file_path(&opt.dir, fid).display()
                )
            });
        }
        Ok(mt)
    }

    /// Replays the WAL into the skiplist and truncates it after the last
    /// complete entry (Go's `UpdateSkipList`).
    fn replay(&self, read_only: bool) -> Result<()> {
        let Some(wal) = &self.wal else { return Ok(()) };
        let mut wal = wal.lock().unwrap();
        let lf = wal.file();
        let end = lf
            .iterate(0, |e, _vp| {
                self.max_version
                    .fetch_max(parse_ts(&e.key), Ordering::Relaxed);
                // The value is stored as-is: a value pointer or the value itself.
                self.sl.put(&e.key, value_struct(e));
                Ok(())
            })
            .with_context(|| format!("while iterating wal: {}", lf.path().display()))?;
        if end < lf.size() && read_only {
            return Err(Error::TruncateNeeded)
                .with_context(|| format!("end offset: {end} < size: {}", lf.size()));
        }
        lf.truncate(end as u64)?;
        lf.write_at = end;
        Ok(())
    }

    /// Writes an entry to the WAL, then (unless it is a transaction finish
    /// marker) to the skiplist.
    pub fn put(&self, key: &[u8], value: ValueStruct<'_>) -> Result<()> {
        if let Some(wal) = &self.wal {
            let e = Entry {
                key: key.to_vec(),
                value: value.value.to_vec(),
                expires_at: value.expires_at,
                user_meta: value.user_meta,
                meta: value.meta,
                ..Default::default()
            };
            let mut wal = wal.lock().unwrap();
            let Wal { file, buf } = &mut *wal;
            file.as_mut()
                .expect("WAL is open")
                .write_entry(buf, &e)
                .context("cannot write entry to WAL file")?;
        }
        // The finish marker only matters to WAL replay.
        if value.meta & BIT_FIN_TXN != 0 {
            return Ok(());
        }
        self.sl.put(key, value);
        self.max_version.fetch_max(parse_ts(key), Ordering::Relaxed);
        Ok(())
    }

    /// Whether the memtable should be flushed: its arena or its WAL has
    /// reached the memtable size.
    pub fn is_full(&self) -> bool {
        if self.sl.mem_size() >= self.mem_table_size {
            return true;
        }
        match &self.wal {
            Some(wal) => wal.lock().unwrap().file().write_at as u64 >= self.mem_table_size,
            None => false,
        }
    }

    /// Flushes the WAL to disk.
    pub fn sync_wal(&self) -> Result<()> {
        match &self.wal {
            Some(wal) => wal.lock().unwrap().file().sync(),
            None => Ok(()),
        }
    }

    /// The skiplist, to read from or iterate.
    pub fn skiplist(&self) -> &Arc<Skiplist> {
        &self.sl
    }

    /// Highest version written to this memtable.
    pub fn max_version(&self) -> u64 {
        self.max_version.load(Ordering::Relaxed)
    }

    /// Path of the WAL, if any.
    pub fn wal_path(&self) -> Option<PathBuf> {
        let wal = self.wal.as_ref()?.lock().unwrap();
        Some(wal.file.as_ref()?.path_buf())
    }

    /// Bytes written to the WAL so far (including its header).
    pub fn wal_write_at(&self) -> Option<u32> {
        let mut wal = self.wal.as_ref()?.lock().unwrap();
        Some(wal.file().write_at)
    }

    /// Delete the WAL when this memtable is dropped (after a flush to L0).
    pub fn delete_wal_on_drop(&self) {
        self.delete_wal.store(true, Ordering::Relaxed);
    }
}

impl Drop for MemTable {
    fn drop(&mut self) {
        let Some(wal) = &mut self.wal else { return };
        let Some(file) = wal.get_mut().unwrap_or_else(|e| e.into_inner()).file.take() else {
            return;
        };
        let path = file.path_buf();
        let res = if *self.delete_wal.get_mut() {
            file.delete()
        } else {
            file.close(None)
        };
        if let Err(e) = res {
            log_error(&format!("while closing WAL {}: {e}", path.display()));
        }
    }
}

// TODO(phase 5): switch to the `log` facade once the crate depends on it.
fn log_error(msg: &str) {
    eprintln!("badger: {msg}");
}

/// The value of a replayed WAL entry.
fn value_struct(e: &Entry) -> ValueStruct<'_> {
    ValueStruct {
        meta: e.meta,
        user_meta: e.user_meta,
        expires_at: e.expires_at,
        value: &e.value,
        version: 0,
    }
}

/// Opens every WAL in `opt.dir` in id order, replaying each into a memtable
/// (Go's `DB.openMemTables`). WALs with no entries are deleted (kept in
/// read-only mode). Returns the memtables, oldest first, and the next free id.
pub fn open_mem_tables(opt: &Options, registry: &KeyRegistry) -> Result<(Vec<MemTable>, u32)> {
    if opt.in_memory {
        return Ok((Vec::new(), 1));
    }
    let mut fids = Vec::new();
    for entry in fs::read_dir(&opt.dir)
        .with_context(|| format!("Unable to open mem dir: {}", opt.dir.display()))?
    {
        let name = entry?.file_name();
        let name = name.to_string_lossy();
        let Some(stem) = name.strip_suffix(MEM_FILE_EXT) else {
            continue;
        };
        let fid: u32 = stem
            .parse()
            .map_err(|e| Error::corruption(format!("{e}")))
            .with_context(|| format!("{name}: Unable to parse log id."))?;
        fids.push(fid);
    }
    fids.sort_unstable();

    let mut tables = Vec::new();
    for &fid in &fids {
        let (mt, _) = MemTable::open(opt, registry, fid, false)
            .with_context(|| format!("while opening fid: {fid}"))?;
        if mt.sl.is_empty() {
            // Fully truncated: nothing to recover.
            if !opt.read_only {
                mt.delete_wal_on_drop();
            }
            continue;
        }
        tables.push(mt);
    }
    let next = fids.last().map_or(0, |&f| f) + 1;
    Ok((tables, next))
}

// Miri cannot mmap files; the skiplist's own tests cover the unsafe code.
#[cfg(all(test, not(miri)))]
mod tests;
