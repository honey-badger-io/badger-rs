//! The database. Port of Go's `db.go` (and `managed_db.go`'s basics).
//!
//! Opening replays the manifest, the memtable WALs and the value log. Reads
//! go through memtables → L0 → Ln. Writes are queued to a writer thread
//! (see [`crate::write`]), which appends to the value log and the memtable;
//! full memtables are flushed to L0 tables by a flush thread.
//!
//! Differences from Go:
//! - Background work runs on `std` threads; [`DB::close`] (or dropping the
//!   DB) stops and joins them.
//! - No compaction, value log GC or L0 write stalls yet (Phase 6), no
//!   subscriptions (Phase 7).

use std::collections::HashSet;
use std::fs;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, RwLock};
use std::thread::JoinHandle;

use crossbeam_channel::{Receiver, Sender};

use crate::dir_lock::DirLockGuard;
use crate::error::{Error, Result, ResultExt};
use crate::iterator::IteratorOptions;
use crate::key_registry::{KeyRegistry, KeyRegistryOptions};
use crate::levels::{LevelsController, TableInfo, base_table_options};
use crate::manifest::ManifestFile;
use crate::memtable::{MemTable, open_mem_tables};
use crate::options::Options;
use crate::oracle::Oracle;
use crate::structs::Entry;
use crate::table::{TableOptions, new_block_cache, new_index_cache};
use crate::txn::Txn;
use crate::util::keys::parse_ts;
use crate::util::{OwnedValueStruct, sync_dir};
use crate::vlog::ValueLog;
use crate::write::{self, FlushMsg, Request};

/// Key prefix under which banned namespaces are stored.
pub(crate) const BANNED_NS_KEY: &[u8] = b"!badger!banned";

/// Capacity of the write channel (Go's `kvWriteChCapacity`).
pub(crate) const WRITE_CH_CAPACITY: usize = 1000;

/// An open database. Share it between threads by reference (`&DB` is
/// `Send`), or put it in an `Arc`.
pub struct DB {
    inner: Arc<DbInner>,
    writer: Option<JoinHandle<()>>,
    flusher: Option<JoinHandle<()>>,
}

/// Memtables: the one being written (none in read-only mode) and the
/// immutable ones waiting to be flushed, oldest first.
#[derive(Default)]
pub(crate) struct MemTables {
    pub mt: Option<Arc<MemTable>>,
    pub imm: Vec<Arc<MemTable>>,
    /// Id of the next memtable WAL.
    pub next_mem_fid: u32,
}

pub(crate) struct DbInner {
    pub opt: Options,
    pub mem: Mutex<MemTables>,
    /// Signalled when the flush thread removes a memtable from `imm`.
    pub flush_cond: Condvar,
    pub lc: LevelsController,
    pub vlog: ValueLog,
    pub orc: Oracle,
    pub manifest: ManifestFile,
    pub registry: Arc<KeyRegistry>,
    /// Options for new tables (without a data key).
    pub table_opts: TableOptions,
    /// Queue to the writer thread; `None` once closing (Go closes `writeCh`).
    pub write_tx: RwLock<Option<Sender<Request>>>,
    /// Queue to the flush thread.
    pub flush_tx: Sender<FlushMsg>,
    pub block_writes: AtomicBool,
    banned_namespaces: RwLock<HashSet<u64>>,
    closed: AtomicBool,
    // Released last (fields drop in declaration order).
    _dir_locks: Vec<DirLockGuard>,
}

impl DB {
    /// Opens (or creates) the database described by `opt`.
    pub fn open(opt: Options) -> Result<DB> {
        Self::open_inner(opt, false)
    }

    /// Opens a database whose transaction timestamps the application manages
    /// (Go's `OpenManaged`): read with [`DB::new_transaction_at`], commit with
    /// [`Txn::commit_at`].
    pub fn open_managed(opt: Options) -> Result<DB> {
        Self::open_inner(opt, true)
    }

    fn open_inner(mut opt: Options, managed: bool) -> Result<DB> {
        opt.managed_txns = managed;
        opt.check_and_set()?;

        let mut dir_locks = Vec::new();
        if !opt.in_memory {
            if !opt.read_only {
                create_dirs(&opt)?;
            }
            if !opt.bypass_lock_guard {
                dir_locks.push(DirLockGuard::acquire(&opt.dir, opt.read_only)?);
                let abs = |p| std::path::absolute(p).map_err(Error::from);
                if abs(&opt.value_dir)? != abs(&opt.dir)? {
                    dir_locks.push(DirLockGuard::acquire(&opt.value_dir, opt.read_only)?);
                }
            }
        }

        let (manifest_file, manifest) = ManifestFile::open_or_create(&opt)?;
        let block_cache = (opt.block_cache_size > 0).then(|| new_block_cache(opt.block_cache_size));
        let index_cache = (opt.index_cache_size > 0).then(|| new_index_cache(opt.index_cache_size));
        let registry = Arc::new(KeyRegistry::open(KeyRegistryOptions::from_options(&opt))?);

        let (imm, next_mem_fid) =
            open_mem_tables(&opt, &registry).context("while opening memtables")?;
        let mut mem = MemTables {
            mt: None,
            imm: imm.into_iter().map(Arc::new).collect(),
            next_mem_fid,
        };
        if !opt.read_only {
            let mt = MemTable::create(&opt, &registry, mem.next_mem_fid)
                .context("cannot create memtable")?;
            mem.next_mem_fid += 1;
            mem.mt = Some(Arc::new(mt));
        }

        let table_opts = base_table_options(&opt, block_cache, index_cache);
        let lc = LevelsController::open(&opt, &manifest, &registry, &table_opts)?;

        // Reads start at the newest version found (before the value log is
        // opened, as in Go).
        let max_version = mem
            .imm
            .iter()
            .map(|m| m.max_version())
            .chain(lc.table_info().iter().map(|t| t.max_version))
            .max()
            .unwrap_or(0);
        let vlog = ValueLog::open(&opt, Arc::clone(&registry)).context("During db.vlog.open")?;

        let (write_tx, write_rx) = crossbeam_channel::bounded(WRITE_CH_CAPACITY);
        let (flush_tx, flush_rx) = crossbeam_channel::bounded(opt.num_memtables.max(1));
        let imm_to_flush = if opt.read_only {
            Vec::new()
        } else {
            mem.imm.clone()
        };
        let inner = Arc::new(DbInner {
            orc: Oracle::new(managed, opt.detect_conflicts, max_version),
            mem: Mutex::new(mem),
            flush_cond: Condvar::new(),
            lc,
            vlog,
            manifest: manifest_file,
            registry,
            table_opts,
            write_tx: RwLock::new((!opt.read_only).then_some(write_tx)),
            flush_tx,
            block_writes: AtomicBool::new(opt.read_only),
            banned_namespaces: RwLock::new(HashSet::new()),
            closed: AtomicBool::new(false),
            _dir_locks: dir_locks,
            opt,
        });

        let mut db = DB {
            inner,
            writer: None,
            flusher: None,
        };
        if !db.inner.opt.read_only {
            db.start_threads(write_rx, flush_rx)?;
            // Recovered memtables are flushed right away.
            for mt in imm_to_flush {
                db.inner
                    .flush_tx
                    .send(Some(mt))
                    .map_err(|_| Error::DbClosed)?;
            }
        }
        db.init_banned_namespaces()
            .context("While setting banned keys")?;
        Ok(db)
    }

    fn start_threads(
        &mut self,
        write_rx: Receiver<Request>,
        flush_rx: Receiver<FlushMsg>,
    ) -> Result<()> {
        let spawn = |name: &str, f: Box<dyn FnOnce() + Send>| {
            std::thread::Builder::new()
                .name(name.to_string())
                .spawn(f)
                .map_err(Error::from)
        };
        let inner = Arc::clone(&self.inner);
        self.flusher = Some(spawn(
            "badger-flush",
            Box::new(move || write::flush_loop(&inner, flush_rx)),
        )?);
        let inner = Arc::clone(&self.inner);
        self.writer = Some(spawn(
            "badger-write",
            Box::new(move || write::write_loop(&inner, write_rx)),
        )?);
        Ok(())
    }

    /// Loads the banned namespaces stored under [`BANNED_NS_KEY`].
    fn init_banned_namespaces(&self) -> Result<()> {
        if self.inner.opt.namespace_offset.is_none() {
            return Ok(());
        }
        let mut banned = HashSet::new();
        self.view(|txn| {
            let mut it = txn.iter(IteratorOptions {
                prefix: BANNED_NS_KEY.to_vec(),
                prefetch_values: false,
                internal_access: true,
                ..Default::default()
            });
            it.rewind();
            while it.valid() {
                let key = it.item().key();
                if let Some(ns) = key.get(BANNED_NS_KEY.len()..BANNED_NS_KEY.len() + 8) {
                    banned.insert(u64::from_be_bytes(ns.try_into().unwrap()));
                }
                it.next();
            }
            Ok(())
        })?;
        *self.inner.banned_namespaces.write().unwrap() = banned;
        Ok(())
    }

    /// Runs `f` in a read-only transaction (Go's `View`). In managed mode it
    /// reads at the newest version.
    pub fn view<R>(&self, f: impl FnOnce(&Txn<'_>) -> Result<R>) -> Result<R> {
        self.check_open()?;
        let txn = if self.inner.opt.managed_txns {
            self.new_transaction_at(u64::MAX, false)
        } else {
            self.new_transaction(false)
        };
        f(&txn)
    }

    /// Runs `f` in an update transaction and commits it if `f` succeeds (Go's
    /// `Update`). Retry on [`Error::Conflict`] if needed.
    ///
    /// # Panics
    /// In managed mode.
    pub fn update<R>(&self, f: impl FnOnce(&mut Txn<'_>) -> Result<R>) -> Result<R> {
        self.check_open()?;
        assert!(
            !self.inner.opt.managed_txns,
            "Update can only be used with managedDB=false."
        );
        let mut txn = self.new_transaction(true);
        let r = f(&mut txn)?;
        txn.commit()?;
        Ok(r)
    }

    /// Starts a transaction (Go's `NewTransaction`). `update` is ignored in
    /// read-only mode.
    ///
    /// # Panics
    /// In managed mode; use [`DB::new_transaction_at`].
    pub fn new_transaction(&self, update: bool) -> Txn<'_> {
        assert!(
            !self.inner.opt.managed_txns,
            "Cannot use NewTransaction() with managedDB=true. Use NewTransactionAt() instead."
        );
        Txn::new(&self.inner, update, None)
    }

    /// Starts a transaction reading at `read_ts` (Go's `NewTransactionAt`).
    ///
    /// # Panics
    /// If the DB was not opened with [`DB::open_managed`].
    pub fn new_transaction_at(&self, read_ts: u64, update: bool) -> Txn<'_> {
        assert!(
            self.inner.opt.managed_txns,
            "Cannot use NewTransactionAt with managedDB=false. Use NewTransaction instead."
        );
        Txn::new(&self.inner, update, Some(read_ts))
    }

    /// Managed mode: versions at or below `ts` are no longer read (Go's
    /// `SetDiscardTs`).
    pub fn set_discard_ts(&self, ts: u64) {
        assert!(
            self.inner.opt.managed_txns,
            "SetDiscardTs needs a managed DB"
        );
        self.inner.orc.set_discard_ts(ts);
    }

    /// Writes the current memtable's WAL and value log file to disk (Go's
    /// `Sync`).
    pub fn sync(&self) -> Result<()> {
        if self.inner.opt.in_memory || self.inner.opt.read_only {
            return Ok(());
        }
        let mt = self.inner.mem.lock().unwrap().mt.clone();
        let wal = mt.map_or(Ok(()), |mt| mt.sync_wal());
        let vlog = self.inner.vlog.sync();
        wal.and(vlog)
    }

    /// The newest version in the DB.
    pub fn max_version(&self) -> u64 {
        self.inner.max_version()
    }

    /// Information about every table.
    pub fn tables(&self) -> Vec<TableInfo> {
        self.inner.lc.table_info()
    }

    /// The options the DB was opened with (after validation).
    pub fn opts(&self) -> &Options {
        &self.inner.opt
    }

    /// Largest number of entries in one transaction.
    pub fn max_batch_count(&self) -> u64 {
        self.inner.opt.max_batch_count()
    }

    /// Largest size of one transaction, in bytes.
    pub fn max_batch_size(&self) -> u64 {
        self.inner.opt.max_batch_size()
    }

    pub fn is_closed(&self) -> bool {
        self.inner.closed.load(Ordering::SeqCst)
    }

    fn check_open(&self) -> Result<()> {
        if self.is_closed() {
            return Err(Error::DbClosed);
        }
        Ok(())
    }

    /// Closes the DB: waits for queued writes, flushes the memtable to L0,
    /// truncates the value log and releases the files and the directory
    /// lock. Dropping the DB does the same but cannot report errors.
    pub fn close(mut self) -> Result<()> {
        self.close_impl()
    }

    fn close_impl(&mut self) -> Result<()> {
        let inner = &self.inner;
        {
            // Under the memtable lock, so a writer waiting for room sees it.
            let _mem = inner.mem.lock().unwrap();
            if inner.closed.swap(true, Ordering::SeqCst) {
                return Ok(());
            }
            inner.block_writes.store(true, Ordering::SeqCst);
        }
        inner.flush_cond.notify_all();
        if inner.opt.read_only {
            return Ok(());
        }

        // Stop the writer once it has drained the queue: dropping the sender
        // disconnects the channel after the queued requests.
        drop(inner.write_tx.write().unwrap().take());
        if let Some(t) = self.writer.take() {
            let _ = t.join();
        }

        // Flush the current memtable, or delete its WAL if it is empty.
        let mt = inner.mem.lock().unwrap().mt.take();
        if let Some(mt) = mt {
            if mt.skiplist().is_empty() {
                mt.delete_wal_on_drop();
            } else if self.flusher.is_some() {
                inner.mem.lock().unwrap().imm.push(Arc::clone(&mt));
                let _ = inner.flush_tx.send(Some(mt));
            }
            // Without a flush thread (it failed to start), the WAL stays and
            // is replayed on the next open.
        }
        if let Some(t) = self.flusher.take() {
            let _ = inner.flush_tx.send(None);
            let _ = t.join();
        }

        let mut result = inner.vlog.close().context("DB.Close");
        for dir in [&inner.opt.dir, &inner.opt.value_dir] {
            if !inner.opt.in_memory {
                let r = sync_dir(dir).context("DB.Close");
                result = result.and(r);
            }
        }
        result
    }
}

impl Drop for DB {
    fn drop(&mut self) {
        if let Err(e) = self.close_impl() {
            log::error!("while closing DB: {e}");
        }
    }
}

impl DbInner {
    /// Memtables to read, newest first.
    pub(crate) fn mem_tables(&self) -> Vec<Arc<MemTable>> {
        let mem = self.mem.lock().unwrap();
        mem.mt.iter().chain(mem.imm.iter().rev()).cloned().collect()
    }

    /// The newest value of the key in `key` (an internal key) at or below its
    /// version: memtables first, then the levels (Go's `DB.get`).
    pub(crate) fn get(&self, key: &[u8]) -> Result<Option<OwnedValueStruct>> {
        if self.closed.load(Ordering::SeqCst) {
            return Err(Error::DbClosed);
        }
        let version = parse_ts(key);
        let mut max_vs: Option<OwnedValueStruct> = None;
        for mt in self.mem_tables() {
            let Some(vs) = mt.skiplist().get(key) else {
                continue;
            };
            if vs.version == version {
                return Ok(Some(vs.into()));
            }
            if max_vs.as_ref().is_none_or(|m| m.version < vs.version) {
                max_vs = Some(vs.into());
            }
        }
        self.lc.get(key, max_vs, 0)
    }

    pub(crate) fn max_version(&self) -> u64 {
        let from_mem = self
            .mem_tables()
            .iter()
            .map(|m| m.max_version())
            .max()
            .unwrap_or(0);
        let from_tables = self
            .lc
            .table_info()
            .iter()
            .map(|t| t.max_version)
            .max()
            .unwrap_or(0);
        from_mem.max(from_tables)
    }

    /// Values of at least this many bytes go to the value log.
    pub(crate) fn value_threshold(&self) -> i64 {
        self.opt.value_threshold as i64
    }

    pub(crate) fn is_closed(&self) -> bool {
        self.closed.load(Ordering::SeqCst)
    }

    /// Fails with [`Error::BannedKey`] if `key`'s namespace is banned.
    pub(crate) fn is_banned(&self, key: &[u8]) -> Result<()> {
        let Some(off) = self.opt.namespace_offset else {
            return Ok(());
        };
        if key.len() <= off + 8 {
            return Ok(());
        }
        let ns = u64::from_be_bytes(key[off..off + 8].try_into().unwrap());
        if self.banned_namespaces.read().unwrap().contains(&ns) {
            return Err(Error::BannedKey);
        }
        Ok(())
    }

    /// Queues `entries` as one write request (Go's `sendToWriteCh`) and
    /// returns the channel that receives its result.
    pub(crate) fn send_to_write_ch(&self, mut entries: Vec<Entry>) -> Result<Receiver<Result<()>>> {
        if self.block_writes.load(Ordering::SeqCst) {
            return Err(Error::BlockedWrites);
        }
        let threshold = self.value_threshold();
        let size: i64 = entries
            .iter_mut()
            .map(|e| e.estimate_size_and_set_threshold(threshold))
            .sum();
        if entries.len() as u64 >= self.opt.max_batch_count()
            || size as u64 >= self.opt.max_batch_size()
        {
            return Err(Error::TxnTooBig);
        }
        let (done_tx, done_rx) = crossbeam_channel::bounded(1);
        let gate = self.write_tx.read().unwrap();
        let Some(tx) = gate.as_ref() else {
            return Err(Error::BlockedWrites);
        };
        tx.send(Request {
            entries,
            done: done_tx,
        })
        .map_err(|_| Error::BlockedWrites)?;
        Ok(done_rx)
    }
}

impl DbInner {
    /// Writes `entries` (keys with versions) as one batch and waits for it
    /// (Go's `batchSet`).
    #[allow(dead_code)] // used by WriteBatch (Phase 7) and tests
    pub(crate) fn batch_set(&self, entries: Vec<Entry>) -> Result<()> {
        let rx = self.send_to_write_ch(entries)?;
        rx.recv().unwrap_or(Err(Error::DbClosed))
    }
}

/// Creates the DB directories if missing (Go's `createDirs`).
pub(crate) fn create_dirs(opt: &Options) -> Result<()> {
    for dir in [&opt.dir, &opt.value_dir] {
        fs::create_dir_all(dir)
            .with_context(|| format!("Error Creating Dir: {}", dir.display()))?;
    }
    Ok(())
}

// Miri cannot mmap files.
#[cfg(all(test, not(miri)))]
mod tests;
