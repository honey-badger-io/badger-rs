//! The LSM levels: which tables are at each level, point lookups through them
//! and iterators over them. Port of Go's `levels.go` (the non-compaction
//! parts) and `level_handler.go`; compaction is in [`crate::compaction`].
//!
//! L0 tables may overlap and are kept oldest first (by age, which is table id
//! order for flushed tables). Tables in L1 and below do not overlap and are
//! sorted by smallest key.
//!
//! Differences from Go: tables are opened one after another (Go uses three
//! goroutines); in read-only mode, table files missing from the manifest are
//! left alone instead of deleted. Removed tables are marked
//! [`Table::delete_on_drop`] instead of having their reference count dropped.
//! An L0 → L0 compaction's output takes the place of its inputs in L0 (Go
//! sorts L0 by smallest key after it, losing the age order).

use std::collections::{BTreeSet, HashSet};
use std::fs;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, RwLock, RwLockReadGuard};
use std::time::Instant;

use crate::compaction::{CompactStatus, KeyRange};
use crate::db::DbInner;
use crate::error::{Error, Result, ResultExt};
use crate::iterator::IteratorOptions;
use crate::key_registry::KeyRegistry;
use crate::manifest::{Manifest, new_create_change, new_delete_change};
use crate::options::Options;
use crate::table::{
    BlockCache, ConcatIterator, IndexCache, Table, TableOptions, new_filename, parse_file_id,
};
use crate::util::bloom;
use crate::util::keys::{compare_keys, parse_key, parse_ts, same_key};
use crate::util::{KvIterator, OwnedValueStruct, sync_dir};

pub(crate) type BoxedIter = Box<dyn KvIterator>;

/// Table options for the DB's tables (Go's `buildTableOptions`), without a
/// data key or compression: those are per table.
pub(crate) fn base_table_options(
    opt: &Options,
    block_cache: Option<BlockCache>,
    index_cache: Option<IndexCache>,
) -> TableOptions {
    TableOptions {
        table_size: opt.base_table_size,
        checksum_mode: opt.checksum_verification_mode,
        bloom_false_positive: opt.bloom_false_positive,
        block_size: opt.block_size,
        data_key: None,
        compression: opt.compression,
        zstd_compression_level: opt.zstd_compression_level,
        block_cache,
        index_cache,
    }
}

/// Summary of one table (Go's `TableInfo`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableInfo {
    pub id: u64,
    pub level: usize,
    /// Smallest and biggest internal keys.
    pub left: Vec<u8>,
    pub right: Vec<u8>,
    pub key_count: u32,
    pub on_disk_size: u32,
    pub stale_data_size: u32,
    pub uncompressed_size: u32,
    pub index_sz: usize,
    pub bloom_filter_size: usize,
    pub max_version: u64,
}

/// Summary of one level (Go's `LevelInfo`).
#[derive(Debug, Clone, PartialEq)]
pub struct LevelInfo {
    pub level: usize,
    pub num_tables: usize,
    pub size: u64,
    pub target_size: u64,
    pub target_file_size: u64,
    pub is_base_level: bool,
    pub score: f64,
    pub adjusted: f64,
    pub stale_data_size: u64,
}

pub(crate) struct LevelsController {
    pub(crate) levels: Vec<LevelHandler>,
    next_file_id: AtomicU64,
    pub(crate) cstatus: Mutex<CompactStatus>,
    /// The flush thread waits on this while L0 has too many tables; it is
    /// signalled when compaction removes L0 tables, and when closing.
    l0_stall_lock: Mutex<()>,
    l0_stall: Condvar,
    /// Total time spent stalled, in milliseconds.
    pub(crate) l0_stalls_ms: AtomicU64,
}

impl LevelsController {
    /// Opens every table in the manifest (Go's `newLevelsController`).
    pub fn open(
        opt: &Options,
        manifest: &Manifest,
        registry: &KeyRegistry,
        base: &TableOptions,
    ) -> Result<LevelsController> {
        let levels: Vec<_> = (0..opt.max_levels).map(LevelHandler::new).collect();
        let lc = LevelsController {
            levels,
            next_file_id: AtomicU64::new(1),
            cstatus: Mutex::new(CompactStatus::new(opt.max_levels)),
            l0_stall_lock: Mutex::new(()),
            l0_stall: Condvar::new(),
            l0_stalls_ms: AtomicU64::new(0),
        };
        if opt.in_memory {
            return Ok(lc);
        }
        if manifest.levels.len() > opt.max_levels {
            return Err(Error::InvalidOptions(format!(
                "the manifest has {} levels, but max_levels is {}",
                manifest.levels.len(),
                opt.max_levels
            )));
        }
        revert_to_manifest(&opt.dir, manifest, opt.read_only)?;

        let mut tables: Vec<Vec<Arc<Table>>> = vec![Vec::new(); opt.max_levels];
        let mut max_file_id = 0;
        for (&id, tm) in &manifest.tables {
            max_file_id = max_file_id.max(id);
            let path = new_filename(id, &opt.dir);
            let mut topt = base.clone();
            // Compression and key come from how the table was written.
            topt.compression = tm.compression;
            topt.data_key = registry
                .data_key(tm.key_id)
                .context("Error while reading datakey")?;
            match Table::open(&path, topt) {
                Ok(t) => tables[tm.level as usize].push(t),
                Err(e) if matches!(e.root(), Error::ChecksumMismatch { .. }) => {
                    // Go skips tables that fail their checksum.
                    log::error!("{e}");
                    log::error!("Ignoring table {}", path.display());
                }
                Err(e) => {
                    return Err(e).with_context(|| format!("Opening table: {}", path.display()));
                }
            }
        }
        lc.next_file_id.store(max_file_id + 1, Ordering::Relaxed);
        for (handler, tables) in lc.levels.iter().zip(tables) {
            handler.init_tables(tables);
        }
        lc.validate().context("Level validation")?;
        if !opt.read_only {
            sync_dir(&opt.dir)?;
        }
        Ok(lc)
    }

    pub(crate) fn validate(&self) -> Result<()> {
        self.levels
            .iter()
            .try_for_each(|l| l.validate())
            .context("Levels Controller")
    }

    pub fn last_level(&self) -> &LevelHandler {
        self.levels.last().expect("at least one level")
    }

    /// The newest value for `key` (an internal key) at or below its version,
    /// across the levels; `max_vs` is the best found so far (in memtables).
    pub fn get(
        &self,
        key: &[u8],
        mut max_vs: Option<OwnedValueStruct>,
        start_level: usize,
    ) -> Result<Option<OwnedValueStruct>> {
        // Levels are searched from L0 down, so a concurrent compaction moving
        // data down is not missed.
        let version = parse_ts(key);
        for h in &self.levels[start_level..] {
            let Some(vs) = h
                .get(key)
                .with_context(|| format!("get key: {}", String::from_utf8_lossy(key)))?
            else {
                continue;
            };
            if vs.version == version {
                return Ok(Some(vs));
            }
            if max_vs.as_ref().is_none_or(|m| m.version < vs.version) {
                max_vs = Some(vs);
            }
        }
        Ok(max_vs)
    }

    /// Adds iterators over every level to `iters`.
    pub fn append_iterators(
        &self,
        iters: &mut Vec<BoxedIter>,
        opt: &IteratorOptions,
        prefix_is_key: bool,
    ) {
        for level in &self.levels {
            level.append_iterators(iters, opt, prefix_is_key);
        }
    }

    /// Information about every table, by level (Go's `getTableInfo`).
    pub fn table_info(&self) -> Vec<TableInfo> {
        let mut out = Vec::new();
        for l in &self.levels {
            for t in l.read().tables.iter() {
                out.push(TableInfo {
                    id: t.id(),
                    level: l.level,
                    left: t.smallest().to_vec(),
                    right: t.biggest().to_vec(),
                    key_count: t.key_count(),
                    on_disk_size: t.on_disk_size(),
                    stale_data_size: t.stale_data_size(),
                    uncompressed_size: t.uncompressed_size(),
                    index_sz: t.index_size(),
                    bloom_filter_size: t.bloom_filter_size(),
                    max_version: t.max_version(),
                });
            }
        }
        out.sort_by_key(|t| (t.level, t.id));
        out
    }

    /// Size, targets and compaction scores of every level (Go's
    /// `getLevelInfo`).
    pub fn level_info(&self, opt: &Options) -> Vec<LevelInfo> {
        let t = self.level_targets(opt);
        let prios = self.pick_compact_levels(opt);
        let mut out: Vec<LevelInfo> = self
            .levels
            .iter()
            .map(|l| {
                let s = l.read();
                LevelInfo {
                    level: l.level,
                    num_tables: s.tables.len(),
                    size: s.total_size,
                    target_size: t.target_sz[l.level],
                    target_file_size: t.file_sz[l.level],
                    is_base_level: t.base_level == l.level,
                    score: 0.0,
                    adjusted: 0.0,
                    stale_data_size: s.total_stale_size,
                }
            })
            .collect();
        for p in prios {
            out[p.level].score = p.score;
            out[p.level].adjusted = p.adjusted;
        }
        out
    }

    /// Adds a table flushed from a memtable to L0, recording it in the
    /// manifest first (Go's `addLevel0Table`). While L0 holds
    /// `num_level_zero_tables_stall` tables, this waits for compaction to
    /// drain it, unless the DB is closing or a drop is in progress.
    pub fn add_level0_table(&self, db: &DbInner, t: Arc<Table>) -> Result<()> {
        // The manifest is updated before the table becomes visible, so a
        // compaction that later deletes it is recorded after its creation.
        if !t.is_in_memory() {
            db.manifest.add_changes(vec![new_create_change(
                t.id(),
                0,
                t.key_id(),
                t.compression_type(),
            )])?;
        }
        let mut guard = self.l0_stall_lock.lock().unwrap();
        let start = Instant::now();
        let mut stalled = false;
        while self.levels[0].num_tables() >= db.opt.num_level_zero_tables_stall {
            // The stall is flow control, not an invariant: when closing (or
            // dropping data) compaction may never drain L0, so give up.
            if db.is_closed() || db.l0_stall_disabled() {
                break;
            }
            stalled = true;
            guard = self.l0_stall.wait(guard).unwrap();
        }
        if stalled {
            let dur = start.elapsed();
            if dur.as_secs() >= 1 {
                log::info!("L0 was stalled for {dur:?}");
            }
            self.l0_stalls_ms
                .fetch_add(dur.as_millis() as u64, Ordering::Relaxed);
        }
        self.levels[0].write().push(t);
        drop(guard);
        Ok(())
    }

    /// Wakes the flush thread if it is stalled on L0 (Go's
    /// `signalL0Drained`).
    pub fn signal_l0_drained(&self) {
        let _guard = self.l0_stall_lock.lock().unwrap();
        self.l0_stall.notify_all();
    }

    /// The id the next new table gets.
    pub fn reserve_file_id(&self) -> u64 {
        self.next_file_id.fetch_add(1, Ordering::Relaxed)
    }

    /// Removes every table, recording it in the manifest first (Go's
    /// `dropTree`). Returns the number of tables removed.
    pub fn drop_tree(&self, db: &DbInner) -> Result<usize> {
        let all: Vec<Arc<Table>> = self
            .levels
            .iter()
            .flat_map(|l| l.read().tables.clone())
            .collect();
        if all.is_empty() {
            return Ok(0);
        }
        let changes = all
            .iter()
            .filter(|t| !t.is_in_memory())
            .map(|t| new_delete_change(t.id()))
            .collect();
        db.manifest.add_changes(changes)?;
        for l in &self.levels {
            let mut s = l.write();
            s.tables.clear();
            s.total_size = 0;
            s.total_stale_size = 0;
        }
        for t in &all {
            t.delete_on_drop();
        }
        Ok(all.len())
    }

    /// Verifies the checksum of every table (Go's `verifyChecksum`).
    pub fn verify_checksum(&self) -> Result<()> {
        for l in &self.levels {
            let tables = l.read().tables.clone();
            for t in tables {
                t.verify_checksum()?;
            }
        }
        Ok(())
    }

    /// Whether any level from `lev` on holds a table overlapping `tables`
    /// (Go's `checkOverlap`).
    pub fn check_overlap(&self, tables: &[Arc<Table>], lev: usize) -> bool {
        let kr = KeyRange::of_tables(tables);
        self.levels[lev.min(self.levels.len())..].iter().any(|l| {
            let (left, right) = overlapping_tables(&l.read().tables, &kr);
            right > left
        })
    }
}

/// Table files in `dir` that the manifest does not know are deleted; tables
/// the manifest lists must exist (Go's `revertToManifest`).
fn revert_to_manifest(dir: &Path, mf: &Manifest, read_only: bool) -> Result<()> {
    let mut on_disk = BTreeSet::new();
    for entry in fs::read_dir(dir)? {
        if let Some(id) = parse_file_id(&entry?.path()) {
            on_disk.insert(id);
        }
    }
    for id in mf.tables.keys() {
        if !on_disk.contains(id) {
            return Err(Error::corruption(format!(
                "file does not exist for table {id}"
            )));
        }
    }
    if read_only {
        return Ok(());
    }
    for id in on_disk {
        if !mf.tables.contains_key(&id) {
            log::debug!("Table file {id} not referenced in MANIFEST");
            fs::remove_file(new_filename(id, dir))
                .with_context(|| format!("While removing table {id}"))?;
        }
    }
    Ok(())
}

/// The tables of a level `[left, right)` that overlap `kr` (Go's
/// `overlappingTables`). `tables` must be sorted and non-overlapping.
pub(crate) fn overlapping_tables(tables: &[Arc<Table>], kr: &KeyRange) -> (usize, usize) {
    if kr.left.is_empty() || kr.right.is_empty() {
        return (0, 0);
    }
    let left = tables.partition_point(|t| compare_keys(&kr.left, t.biggest()).is_gt());
    let right = tables.partition_point(|t| compare_keys(&kr.right, t.smallest()).is_ge());
    (left, right.max(left))
}

/// A level's tables and their total sizes.
#[derive(Default)]
pub(crate) struct LevelState {
    pub tables: Vec<Arc<Table>>,
    pub total_size: u64,
    pub total_stale_size: u64,
}

impl LevelState {
    fn add_size(&mut self, t: &Table) {
        self.total_size += t.size();
        self.total_stale_size += t.stale_data_size() as u64;
    }

    fn subtract_size(&mut self, t: &Table) {
        self.total_size -= t.size();
        self.total_stale_size -= t.stale_data_size() as u64;
    }

    fn push(&mut self, t: Arc<Table>) {
        self.add_size(&t);
        self.tables.push(t);
    }
}

/// The tables of one level (Go's `levelHandler`).
pub(crate) struct LevelHandler {
    pub(crate) level: usize,
    state: RwLock<LevelState>,
}

impl LevelHandler {
    fn new(level: usize) -> Self {
        LevelHandler {
            level,
            state: RwLock::new(LevelState::default()),
        }
    }

    pub(crate) fn read(&self) -> RwLockReadGuard<'_, LevelState> {
        self.state.read().unwrap()
    }

    pub(crate) fn write(&self) -> std::sync::RwLockWriteGuard<'_, LevelState> {
        self.state.write().unwrap()
    }

    pub(crate) fn total_size(&self) -> u64 {
        self.read().total_size
    }

    #[cfg(all(test, not(miri)))]
    pub(crate) fn total_stale_size(&self) -> u64 {
        self.read().total_stale_size
    }

    pub(crate) fn num_tables(&self) -> usize {
        self.read().tables.len()
    }

    fn init_tables(&self, mut tables: Vec<Arc<Table>>) {
        if self.level == 0 {
            tables.sort_by_key(|t| t.id());
        } else {
            tables.sort_by(|a, b| compare_keys(a.smallest(), b.smallest()));
        }
        let mut s = self.write();
        *s = LevelState::default();
        for t in tables {
            s.push(t);
        }
    }

    /// Adds a table without sorting (tests and the stream writer); call
    /// [`LevelHandler::sort_tables`] afterwards.
    #[allow(dead_code)] // used by tests and the stream writer (Phase 7)
    pub(crate) fn add_table(&self, t: Arc<Table>) {
        self.write().push(t);
    }

    #[allow(dead_code)] // used by tests and the stream writer (Phase 7)
    pub(crate) fn sort_tables(&self) {
        let mut s = self.write();
        if self.level == 0 {
            s.tables.sort_by_key(|t| t.id());
        } else {
            s.tables
                .sort_by(|a, b| compare_keys(a.smallest(), b.smallest()));
        }
    }

    /// Removes `to_del` from the level; their files go once unused (Go's
    /// `deleteTables`).
    pub(crate) fn delete_tables(&self, to_del: &[Arc<Table>]) {
        self.replace_tables(to_del, &[]);
    }

    /// Replaces `to_del` with `to_add` (Go's `replaceTables`). Below L0 the
    /// result is sorted by key; in L0 the new tables take the position of
    /// the first deleted one (or go last), keeping L0 in age order.
    pub(crate) fn replace_tables(&self, to_del: &[Arc<Table>], to_add: &[Arc<Table>]) {
        let del: HashSet<u64> = to_del.iter().map(|t| t.id()).collect();
        let mut s = self.write();
        let old = std::mem::take(&mut s.tables);
        let mut new = Vec::with_capacity(old.len() + to_add.len());
        let mut added = false;
        for t in old {
            if del.contains(&t.id()) {
                s.subtract_size(&t);
                if self.level == 0 && !added {
                    new.extend(to_add.iter().cloned());
                    added = true;
                }
                t.delete_on_drop();
            } else {
                new.push(t);
            }
        }
        if !added {
            new.extend(to_add.iter().cloned());
        }
        for t in to_add {
            s.add_size(t);
        }
        if self.level != 0 {
            new.sort_by(|a, b| compare_keys(a.smallest(), b.smallest()));
        }
        s.tables = new;
    }

    /// Levels below L0 must hold sorted, non-overlapping tables.
    fn validate(&self) -> Result<()> {
        if self.level == 0 {
            return Ok(());
        }
        let s = self.read();
        for (j, pair) in s.tables.windows(2).enumerate() {
            let (a, b) = (&pair[0], &pair[1]);
            if compare_keys(a.biggest(), b.smallest()).is_ge() {
                return Err(Error::corruption(format!(
                    "Inter: Biggest(j-1)[{}] {} vs Smallest(j)[{}]: {}: level={} j={}",
                    a.id(),
                    hex(a.biggest()),
                    b.id(),
                    hex(b.smallest()),
                    self.level,
                    j + 1
                )));
            }
            if compare_keys(b.smallest(), b.biggest()).is_gt() {
                return Err(Error::corruption(format!(
                    "Intra: {} vs {}: level={} j={}",
                    hex(b.smallest()),
                    hex(b.biggest()),
                    self.level,
                    j + 1
                )));
            }
        }
        Ok(())
    }

    /// Tables that may hold `key`: all of L0 (newest first), or the one table
    /// of a lower level whose range covers it.
    fn tables_for_key(&self, key: &[u8]) -> Vec<Arc<Table>> {
        let s = self.read();
        if self.level == 0 {
            return s.tables.iter().rev().cloned().collect();
        }
        let idx = s
            .tables
            .partition_point(|t| compare_keys(t.biggest(), key).is_lt());
        s.tables.get(idx).cloned().into_iter().collect()
    }

    /// The newest version of `key` at or below its version in this level.
    fn get(&self, key: &[u8]) -> Result<Option<OwnedValueStruct>> {
        let hash = bloom::hash(parse_key(key));
        let mut max_vs: Option<OwnedValueStruct> = None;
        for t in self.tables_for_key(key) {
            if t.does_not_have(hash) {
                continue;
            }
            let mut it = t.iter(false, true);
            it.seek(key);
            if let Some(e) = it.error() {
                return Err(Error::corruption(format!("{}: {e}", t.filename())));
            }
            if !it.valid() || !same_key(key, it.key()) {
                continue;
            }
            let version = parse_ts(it.key());
            if max_vs.as_ref().is_none_or(|m| m.version < version) {
                let mut vs = OwnedValueStruct::from(it.value());
                vs.version = version;
                max_vs = Some(vs);
            }
        }
        Ok(max_vs)
    }

    fn append_iterators(
        &self,
        iters: &mut Vec<BoxedIter>,
        opt: &IteratorOptions,
        prefix_is_key: bool,
    ) {
        let s = self.read();
        if self.level == 0 {
            // Newest first, so the merge iterator prefers newer tables.
            for t in s
                .tables
                .iter()
                .rev()
                .filter(|t| opt.pick_table(t, prefix_is_key))
            {
                iters.push(Box::new(t.iter(opt.reverse, true)));
            }
            return;
        }
        let picked = opt.pick_tables(&s.tables, prefix_is_key);
        if !picked.is_empty() {
            iters.push(Box::new(ConcatIterator::new(picked, opt.reverse, true)));
        }
    }
}

pub(crate) fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}
