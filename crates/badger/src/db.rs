//! The database. Port of Go's `db.go` (and `managed_db.go`'s basics).
//!
//! Opening replays the manifest, the memtable WALs and the value log. Reads
//! go through memtables → L0 → Ln. Writes are queued to a writer thread
//! (see [`crate::write`]), which appends to the value log and the memtable;
//! full memtables are flushed to L0 tables by a flush thread, and compactor
//! threads ([`crate::compaction`]) push tables down the levels. Value log GC
//! runs when the application calls [`DB::run_value_log_gc`].
//!
//! Differences from Go:
//! - Background work runs on `std` threads; [`DB::close`] (or dropping the
//!   DB) stops and joins them.
//! - Blocking writes for a drop does not restart the writer and flush
//!   threads: the writer is drained with an empty request, and the drop
//!   waits for the flush queue to empty.
//! - `drop_all` does not restart table ids at 1, so a table still open in a
//!   reader can never share a file name with a new one.

use std::collections::HashSet;
use std::fs;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, RwLock};
use std::thread::JoinHandle;

use crossbeam_channel::{Receiver, Sender};

use crate::compaction::{self, CompactionPriority};
use crate::dir_lock::DirLockGuard;
use crate::error::{Error, Result, ResultExt};
use crate::iterator::IteratorOptions;
use crate::key_registry::{KeyRegistry, KeyRegistryOptions};
use crate::levels::{LevelInfo, LevelsController, TableInfo, base_table_options};
use crate::manifest::ManifestFile;
use crate::memtable::{MemTable, open_mem_tables};
use crate::options::Options;
use crate::oracle::{CommitMark, Oracle};
use crate::publisher::{self, Publisher};
use crate::structs::Entry;
use crate::table::{TableOptions, new_block_cache, new_index_cache};
use crate::txn::Txn;
use crate::util::keys::{key_with_ts, parse_ts};
use crate::util::{OwnedValueStruct, sync_dir};
use crate::vlog::{self, ValueLog};
use crate::write::{self, FlushMsg, Request};

/// Key prefix under which banned namespaces are stored.
pub(crate) const BANNED_NS_KEY: &[u8] = b"!badger!banned";

/// Capacity of the write channel (Go's `kvWriteChCapacity`).
pub(crate) const WRITE_CH_CAPACITY: usize = 1000;

/// An open database. Share it between threads by reference (`&DB` is
/// `Send`), or put it in an `Arc`.
pub struct DB {
    pub(crate) inner: Arc<DbInner>,
    writer: Option<JoinHandle<()>>,
    flusher: Option<JoinHandle<()>>,
    publisher: Option<JoinHandle<()>>,
    /// `None` while stopped (and always in read-only mode).
    compactors: Mutex<Option<Compactors>>,
}

/// The compactor threads; dropping `stop` tells them to exit.
struct Compactors {
    stop: Sender<()>,
    handles: Vec<JoinHandle<()>>,
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
    /// Delivers writes to subscribers.
    pub publisher: Publisher,
    /// Set while a drop runs: flushes to L0 do not wait for compaction.
    l0_stall_disabled: AtomicBool,
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
        let (publisher, publish_rx) = Publisher::new();
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
            publisher,
            l0_stall_disabled: AtomicBool::new(false),
            banned_namespaces: RwLock::new(HashSet::new()),
            closed: AtomicBool::new(false),
            _dir_locks: dir_locks,
            opt,
        });

        let mut db = DB {
            inner,
            writer: None,
            flusher: None,
            publisher: None,
            compactors: Mutex::new(None),
        };
        let registry = Arc::clone(&db.inner.publisher.registry);
        db.publisher = Some(
            std::thread::Builder::new()
                .name("badger-publish".into())
                .spawn(move || publisher::publish_loop(&registry, publish_rx))?,
        );
        if !db.inner.opt.read_only {
            db.start_threads(write_rx, flush_rx)?;
            db.start_compactions()?;
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

    /// Starts the compactor threads (Go's `startCompactions`).
    pub(crate) fn start_compactions(&self) -> Result<()> {
        let mut slot = self.compactors.lock().unwrap();
        if slot.is_some() || self.inner.opt.read_only {
            return Ok(());
        }
        let (stop, rx) = crossbeam_channel::bounded::<()>(0);
        let mut handles = Vec::new();
        for id in 0..self.inner.opt.num_compactors {
            let inner = Arc::clone(&self.inner);
            let rx = rx.clone();
            handles.push(
                std::thread::Builder::new()
                    .name(format!("badger-compact-{id}"))
                    .spawn(move || compaction::run_compactor(&inner, id as i32, rx))?,
            );
        }
        *slot = Some(Compactors { stop, handles });
        Ok(())
    }

    /// Stops the compactor threads, waiting for running compactions (Go's
    /// `stopCompactions`).
    pub(crate) fn stop_compactions(&self) {
        let Some(c) = self.compactors.lock().unwrap().take() else {
            return;
        };
        drop(c.stop);
        for h in c.handles {
            let _ = h.join();
        }
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

    /// Size, target size and compaction score of every level (Go's
    /// `Levels`).
    pub fn levels(&self) -> Vec<LevelInfo> {
        self.inner.lc.level_info(&self.inner.opt)
    }

    /// Verifies the checksum of every table (Go's `VerifyChecksum`), useful
    /// when the DB was opened without checksum verification.
    pub fn verify_checksum(&self) -> Result<()> {
        self.inner.lc.verify_checksum()
    }

    /// Runs value log garbage collection once (Go's `RunValueLogGC`): if a
    /// value log file has at least `discard_ratio` (in `(0, 1)`) of its data
    /// discardable, its live values are written again and the file is
    /// deleted. Fails with [`Error::NoRewrite`] when no file qualifies and
    /// [`Error::Rejected`] while another GC runs. Call it repeatedly (while
    /// it succeeds) to reclaim more.
    pub fn run_value_log_gc(&self, discard_ratio: f64) -> Result<()> {
        if self.inner.opt.in_memory {
            return Err(Error::GcInMemoryMode);
        }
        if self.inner.opt.read_only {
            return Err(Error::GcInReadOnlyMode);
        }
        if discard_ratio >= 1.0 || discard_ratio <= 0.0 {
            return Err(Error::InvalidRequest);
        }
        self.check_open()?;
        vlog::run_gc(&self.inner, discard_ratio)
    }

    /// Compacts until every table sits on one level, so all versions of a
    /// key are together (Go's `Flatten`), using `workers` threads. Background
    /// compactions pause meanwhile; avoid writing during it.
    ///
    /// # Panics
    /// In read-only mode.
    pub fn flatten(&self, workers: usize) -> Result<()> {
        assert!(
            !self.inner.opt.read_only,
            "Cannot flatten in read-only mode."
        );
        self.stop_compactions();
        let res = self.flatten_inner(workers.max(1));
        let started = self.start_compactions();
        res.and(started)
    }

    pub(crate) fn flatten_inner(&self, workers: usize) -> Result<()> {
        let db = &*self.inner;
        let compact_away = |cp: CompactionPriority| -> Result<()> {
            log::info!("Attempting to compact with {cp:?}");
            let results: Vec<Result<bool>> = std::thread::scope(|s| {
                let handles: Vec<_> = (0..workers)
                    .map(|_| {
                        let cp = cp.clone();
                        s.spawn(move || db.lc.do_compact(db, 175, cp))
                    })
                    .collect();
                handles.into_iter().map(|h| h.join().unwrap()).collect()
            });
            let mut success = 0;
            let mut last_err = None;
            for r in results {
                match r {
                    Ok(true) => success += 1,
                    Ok(false) => last_err = Some(fill_tables_error()),
                    Err(e) => {
                        log::warn!("While running doCompact with {cp:?}. Error: {e}");
                        last_err = Some(e);
                    }
                }
            }
            if success == 0 {
                return Err(last_err.unwrap_or_else(fill_tables_error));
            }
            log::info!(
                "{success} compactor(s) succeeded. One or more tables from level {} compacted.",
                cp.level
            );
            Ok(())
        };
        loop {
            let levels: Vec<usize> = db
                .lc
                .levels
                .iter()
                .filter(|l| l.total_size() > 0)
                .map(|l| l.level)
                .collect();
            if levels.len() <= 1 {
                let prios = db.lc.pick_compact_levels(&db.opt);
                match prios.into_iter().next() {
                    Some(p) if p.score > 1.0 => compact_away(p)?,
                    _ => {
                        log::info!("All tables consolidated into one level. Flattening done.");
                        return Ok(());
                    }
                }
                continue;
            }
            // A score above 1 forces the compaction; 1.71 marks it in logs.
            compact_away(CompactionPriority::new(levels[0], 1.71))?;
        }
    }

    /// Stops writes until the returned guard is dropped: queued writes are
    /// applied and every full memtable is flushed (Go's `prepareToDrop`).
    pub(crate) fn prepare_to_drop(&self) -> Result<DropGuard<'_>> {
        assert!(
            !self.inner.opt.read_only,
            "Attempting to drop data in read-only mode."
        );
        self.inner.block_write()?;
        let guard = DropGuard { db: &self.inner };
        self.inner.l0_stall_disabled.store(true, Ordering::SeqCst);
        self.inner.lc.signal_l0_drained();
        let mut mem = self.inner.mem.lock().unwrap();
        while !mem.imm.is_empty() {
            mem = self.inner.flush_cond.wait(mem).unwrap();
        }
        Ok(guard)
    }

    /// Deletes all data (Go's `DropAll`): every table, value log file and
    /// memtable. Writes and compactions pause meanwhile; do not read during
    /// it (a read may fail).
    ///
    /// # Panics
    /// In read-only mode.
    pub fn drop_all(&self) -> Result<()> {
        log::info!("DropAll called. Blocking writes...");
        let _guard = self.prepare_to_drop()?;
        self.stop_compactions();
        let res = self.drop_all_inner();
        let started = self.start_compactions();
        res.and(started)
    }

    pub(crate) fn drop_all_inner(&self) -> Result<()> {
        let db = &*self.inner;
        let mut mem = db.mem.lock().unwrap();
        if let Some(mt) = mem.mt.take() {
            mt.delete_wal_on_drop();
        }
        for mt in mem.imm.drain(..) {
            mt.delete_wal_on_drop();
        }
        let fid = mem.next_mem_fid;
        mem.mt = Some(Arc::new(
            MemTable::create(&db.opt, &db.registry, fid).context("cannot open new memtable")?,
        ));
        mem.next_mem_fid += 1;

        let num = db.lc.drop_tree(db)?;
        log::info!("Deleted {num} SSTables. Now deleting value logs...");
        let num = db.vlog.drop_all()?;
        log::info!("Deleted {num} value log files. DropAll done.");
        if let Some(c) = &db.table_opts.block_cache {
            c.clear();
        }
        if let Some(c) = &db.table_opts.index_cache {
            c.clear();
        }
        Ok(())
    }

    /// Deletes every key starting with one of `prefixes` (Go's
    /// `DropPrefix`). Writes and compactions pause meanwhile.
    ///
    /// # Panics
    /// In read-only mode.
    pub fn drop_prefix(&self, prefixes: &[&[u8]]) -> Result<()> {
        if prefixes.is_empty() {
            return Ok(());
        }
        log::info!("DropPrefix called for {prefixes:?}");
        let _guard = self.prepare_to_drop()?;
        let filtered = self.filter_prefixes_to_drop(prefixes)?;
        if filtered.is_empty() {
            log::info!("No prefixes to drop");
            return Ok(());
        }
        let db = &*self.inner;
        {
            // Flush the memtable without the prefixes (the flush queue is
            // empty and writes are blocked).
            let mut mem = db.mem.lock().unwrap();
            if let Some(mt) = mem.mt.take() {
                if !mt.skiplist().is_empty()
                    && let Err(e) = write::handle_memtable_flush(db, &mt, &filtered)
                {
                    log::error!("While trying to flush memtable: {e}");
                    mem.mt = Some(mt);
                    return Err(e);
                }
                mt.delete_wal_on_drop();
            }
            let fid = mem.next_mem_fid;
            mem.mt = Some(Arc::new(
                MemTable::create(&db.opt, &db.registry, fid)
                    .context("cannot create new mem table")?,
            ));
            mem.next_mem_fid += 1;
        }
        self.stop_compactions();
        let res = db.lc.drop_prefixes(db, &filtered);
        let started = self.start_compactions();
        res.and(started)?;
        log::info!("DropPrefix done");
        Ok(())
    }

    /// The prefixes that have at least one key.
    fn filter_prefixes_to_drop(&self, prefixes: &[&[u8]]) -> Result<Vec<Vec<u8>>> {
        let mut filtered = Vec::new();
        for &prefix in prefixes {
            let found = self.view(|txn| {
                let mut it = txn.iter(IteratorOptions {
                    prefix: prefix.to_vec(),
                    prefetch_values: false,
                    ..Default::default()
                });
                it.rewind();
                Ok(it.valid_for_prefix(prefix))
            })?;
            if found {
                filtered.push(prefix.to_vec());
            }
        }
        Ok(filtered)
    }

    /// Bans namespace `ns`: reads and writes of keys in it fail with
    /// [`Error::BannedKey`] (Go's `BanNamespace`). Needs
    /// `namespace_offset`.
    ///
    /// # Panics
    /// In read-only mode.
    pub fn ban_namespace(&self, ns: u64) -> Result<()> {
        if self.inner.opt.namespace_offset.is_none() {
            return Err(Error::NamespaceMode);
        }
        assert!(
            !self.inner.opt.read_only,
            "Cannot ban namespace in read-only mode."
        );
        log::info!("Banning namespace: {ns}");
        // Stored first, then applied in memory.
        let mut key = BANNED_NS_KEY.to_vec();
        key.extend_from_slice(&ns.to_be_bytes());
        let entry = Entry::new(key_with_ts(&key, 1), Vec::new());
        let rx = self.inner.send_to_write_ch(vec![entry])?;
        rx.recv().unwrap_or(Err(Error::DbClosed))?;
        self.inner.banned_namespaces.write().unwrap().insert(ns);
        Ok(())
    }

    /// The banned namespaces (Go's `BannedNamespaces`).
    pub fn banned_namespaces(&self) -> Vec<u64> {
        let mut out: Vec<u64> = self
            .inner
            .banned_namespaces
            .read()
            .unwrap()
            .iter()
            .copied()
            .collect();
        out.sort_unstable();
        out
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
        // A flush stalled on L0 gives up waiting now.
        inner.lc.signal_l0_drained();
        if inner.opt.read_only {
            inner.publisher.close();
            if let Some(t) = self.publisher.take() {
                let _ = t.join();
            }
            return Ok(());
        }

        // Stop the writer once it has drained the queue: dropping the sender
        // disconnects the channel after the queued requests.
        drop(inner.write_tx.write().unwrap().take());
        if let Some(t) = self.writer.take() {
            let _ = t.join();
        }
        // No more writes to publish: end the subscriptions.
        inner.publisher.close();
        if let Some(t) = self.publisher.take() {
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
        self.stop_compactions();

        // Nothing else runs now.
        if inner.opt.compact_l0_on_close {
            match inner
                .lc
                .do_compact(inner, 173, CompactionPriority::new(0, 1.73))
            {
                Ok(true) => log::debug!("Force compaction on level 0 done"),
                // Nothing worth compacting.
                Ok(false) => {}
                Err(e) => log::warn!("While forcing compaction on level 0: {e}"),
            }
        }

        let mut result = inner.vlog.close().context("DB.Close");
        log::info!(
            "Lifetime L0 stalled for: {}ms",
            inner.lc.l0_stalls_ms.load(Ordering::Relaxed)
        );
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
    pub(crate) fn send_to_write_ch(&self, entries: Vec<Entry>) -> Result<Receiver<Result<()>>> {
        self.send_commit_to_write_ch(entries, None)
    }

    /// Like [`DbInner::send_to_write_ch`], for a transaction whose commit
    /// mark the writer drops after writing (or anything else on failure).
    pub(crate) fn send_commit_to_write_ch(
        &self,
        mut entries: Vec<Entry>,
        commit_mark: Option<CommitMark>,
    ) -> Result<Receiver<Result<()>>> {
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
        // Checked again under the gate: `block_write` sets the flag and then
        // takes the gate, so no request can slip in after its barrier.
        if self.block_writes.load(Ordering::SeqCst) {
            return Err(Error::BlockedWrites);
        }
        let Some(tx) = gate.as_ref() else {
            return Err(Error::BlockedWrites);
        };
        tx.send(Request {
            entries,
            done: done_tx,
            commit_mark,
        })
        .map_err(|_| Error::BlockedWrites)?;
        Ok(done_rx)
    }
}

impl DbInner {
    pub(crate) fn l0_stall_disabled(&self) -> bool {
        self.l0_stall_disabled.load(Ordering::SeqCst)
    }

    /// Stops accepting writes and waits until every queued write is applied
    /// (Go's `blockWrite`).
    fn block_write(&self) -> Result<()> {
        if self
            .block_writes
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return Err(Error::BlockedWrites);
        }
        // Wait out senders that passed the check, then push an empty request
        // through the queue: once it is done, so is everything before it.
        let tx = self.write_tx.write().unwrap().clone();
        if let Some(tx) = tx {
            let (done_tx, done_rx) = crossbeam_channel::bounded(1);
            if tx
                .send(Request {
                    entries: Vec::new(),
                    done: done_tx,
                    commit_mark: None,
                })
                .is_ok()
            {
                let _ = done_rx.recv();
            }
        }
        log::info!("Writes flushed. Stopping compactions now...");
        Ok(())
    }

    /// Writes `entries` (keys with versions) as one batch and waits for it
    /// (Go's `batchSet`).
    pub(crate) fn batch_set(&self, entries: Vec<Entry>) -> Result<()> {
        let rx = self.send_to_write_ch(entries)?;
        rx.recv().unwrap_or(Err(Error::DbClosed))
    }
}

/// Go's `errFillTables`: a compaction found no tables to compact.
fn fill_tables_error() -> Error {
    Error::Context {
        context: "Unable to fill tables".into(),
        source: Box::new(Error::InvalidRequest),
    }
}

/// Undoes `prepare_to_drop` when dropped: writes resume.
pub(crate) struct DropGuard<'a> {
    db: &'a DbInner,
}

impl Drop for DropGuard<'_> {
    fn drop(&mut self) {
        log::info!("Resuming writes");
        self.db.l0_stall_disabled.store(false, Ordering::SeqCst);
        self.db.block_writes.store(false, Ordering::SeqCst);
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
mod compaction_tests;
#[cfg(all(test, not(miri)))]
mod gc_tests;
#[cfg(all(test, not(miri)))]
mod iterator_tests;
#[cfg(all(test, not(miri)))]
mod tests;
