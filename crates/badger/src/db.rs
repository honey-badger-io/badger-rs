//! The database. Port of Go's `db.go` (and `managed_db.go`'s read side).
//!
//! So far a DB can only be opened read-only: [`DB::open`] replays the
//! manifest, the memtable WALs and the value log, and transactions read
//! through memtables → L0 → Ln. The write path comes in Phase 5.

use std::collections::HashSet;
use std::fs;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};

use crate::dir_lock::DirLockGuard;
use crate::error::{Error, Result, ResultExt};
use crate::iterator::IteratorOptions;
use crate::key_registry::{KeyRegistry, KeyRegistryOptions};
use crate::levels::{LevelsController, TableInfo, base_table_options};
use crate::manifest::ManifestFile;
use crate::memtable::{MemTable, open_mem_tables};
use crate::options::Options;
use crate::oracle::Oracle;
use crate::table::{new_block_cache, new_index_cache};
use crate::txn::Txn;
use crate::util::OwnedValueStruct;
use crate::util::keys::parse_ts;
use crate::vlog::ValueLog;

/// Key prefix under which banned namespaces are stored.
pub(crate) const BANNED_NS_KEY: &[u8] = b"!badger!banned";

/// An open database. Shareable between threads (`&DB` is `Send`).
pub struct DB {
    inner: Arc<DbInner>,
}

/// Memtables: the one being written (none in read-only mode) and the
/// immutable ones waiting to be flushed, oldest first.
#[derive(Default)]
pub(crate) struct MemTables {
    pub mt: Option<Arc<MemTable>>,
    pub imm: Vec<Arc<MemTable>>,
}

pub(crate) struct DbInner {
    pub opt: Options,
    pub mem: RwLock<MemTables>,
    pub lc: LevelsController,
    pub vlog: ValueLog,
    pub orc: Oracle,
    #[allow(dead_code)] // flushes and compactions add changes (Phases 5–6)
    pub manifest: ManifestFile,
    #[allow(dead_code)] // new files get keys from it (Phase 5)
    pub registry: Arc<KeyRegistry>,
    banned_namespaces: RwLock<HashSet<u64>>,
    closed: AtomicBool,
    // Released last (fields drop in declaration order).
    _dir_locks: Vec<DirLockGuard>,
}

impl DB {
    /// Opens the database described by `opt`.
    ///
    /// Only read-only mode (`opt.read_only`) is implemented so far.
    pub fn open(opt: Options) -> Result<DB> {
        Self::open_inner(opt, false)
    }

    /// Opens a database whose transaction timestamps the application manages
    /// (Go's `OpenManaged`). Read with [`DB::new_transaction_at`].
    pub fn open_managed(opt: Options) -> Result<DB> {
        Self::open_inner(opt, true)
    }

    fn open_inner(mut opt: Options, managed: bool) -> Result<DB> {
        opt.managed_txns = managed;
        opt.check_and_set()?;
        if !opt.read_only {
            return Err(Error::InvalidOptions(
                "only read-only mode is implemented so far".into(),
            ));
        }

        let mut dir_locks = Vec::new();
        if !opt.in_memory && !opt.bypass_lock_guard {
            dir_locks.push(DirLockGuard::acquire(&opt.dir, opt.read_only)?);
            let abs = |p| std::path::absolute(p).map_err(Error::from);
            if abs(&opt.value_dir)? != abs(&opt.dir)? {
                dir_locks.push(DirLockGuard::acquire(&opt.value_dir, opt.read_only)?);
            }
        }

        let (manifest_file, manifest) = ManifestFile::open_or_create(&opt)?;

        let block_cache = (opt.block_cache_size > 0).then(|| new_block_cache(opt.block_cache_size));
        let index_cache = (opt.index_cache_size > 0).then(|| new_index_cache(opt.index_cache_size));

        let registry = Arc::new(KeyRegistry::open(KeyRegistryOptions::from_options(&opt))?);

        let (imm, _next_mem_fid) =
            open_mem_tables(&opt, &registry).context("while opening memtables")?;
        let mem = MemTables {
            mt: None,
            imm: imm.into_iter().map(Arc::new).collect(),
        };

        let base = base_table_options(&opt, block_cache, index_cache);
        let lc = LevelsController::open(&opt, &manifest, &registry, &base)?;
        let vlog = ValueLog::open(&opt, &registry).context("During db.vlog.open")?;

        // Reads start at the newest version found.
        let max_version = mem
            .imm
            .iter()
            .map(|m| m.max_version())
            .chain(lc.table_info().iter().map(|t| t.max_version))
            .max()
            .unwrap_or(0);
        let inner = DbInner {
            orc: Oracle::new(managed, max_version),
            opt,
            mem: RwLock::new(mem),
            lc,
            vlog,
            manifest: manifest_file,
            registry,
            banned_namespaces: RwLock::new(HashSet::new()),
            closed: AtomicBool::new(false),
            _dir_locks: dir_locks,
        };
        let db = DB {
            inner: Arc::new(inner),
        };
        db.init_banned_namespaces()
            .context("While setting banned keys")?;
        Ok(db)
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

    /// Starts a transaction (Go's `NewTransaction`). Writes are not
    /// implemented yet, and `update` is ignored in read-only mode.
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

    pub fn is_closed(&self) -> bool {
        self.inner.closed.load(Ordering::SeqCst)
    }

    fn check_open(&self) -> Result<()> {
        if self.is_closed() {
            return Err(Error::DbClosed);
        }
        Ok(())
    }

    /// Closes the DB, releasing its files and directory lock. Dropping it
    /// does the same; `close` reports errors.
    pub fn close(self) -> Result<()> {
        self.inner.closed.store(true, Ordering::SeqCst);
        // Nothing to flush in read-only mode; files close as they drop.
        Ok(())
    }
}

impl DbInner {
    /// Memtables to read, newest first.
    pub(crate) fn mem_tables(&self) -> Vec<Arc<MemTable>> {
        let mem = self.mem.read().unwrap();
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
}

/// Creates the DB directories if missing (Go's `createDirs`).
#[allow(dead_code)] // used once DBs can be created (Phase 5)
pub(crate) fn create_dirs(opt: &Options) -> Result<()> {
    for dir in [&opt.dir, &opt.value_dir] {
        fs::create_dir_all(dir)
            .with_context(|| format!("Error Creating Dir: {}", dir.display()))?;
    }
    Ok(())
}
