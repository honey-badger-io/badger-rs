//! The LSM levels: which tables are at each level, point lookups through them
//! and iterators over them. Port of the read side of Go's `levels.go` and
//! `level_handler.go`; compaction comes in Phase 6.
//!
//! L0 tables may overlap and are kept in table id order (newest last). Tables
//! in L1 and below do not overlap and are sorted by smallest key.
//!
//! Differences from Go: tables are opened one after another (Go uses three
//! goroutines); in read-only mode, table files missing from the manifest are
//! left alone instead of deleted.

use std::collections::BTreeSet;
use std::fs;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

use crate::error::{Error, Result, ResultExt};
use crate::iterator::IteratorOptions;
use crate::key_registry::KeyRegistry;
use crate::manifest::Manifest;
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
    pub uncompressed_size: u32,
    pub index_sz: usize,
    pub bloom_filter_size: usize,
    pub max_version: u64,
}

pub(crate) struct LevelsController {
    levels: Vec<LevelHandler>,
    next_file_id: AtomicU64,
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

    fn validate(&self) -> Result<()> {
        self.levels
            .iter()
            .try_for_each(|l| l.validate())
            .context("Levels Controller")
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
            for t in l.tables.read().unwrap().iter() {
                out.push(TableInfo {
                    id: t.id(),
                    level: l.level,
                    left: t.smallest().to_vec(),
                    right: t.biggest().to_vec(),
                    key_count: t.key_count(),
                    on_disk_size: t.on_disk_size(),
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

    /// The id the next new table gets.
    #[allow(dead_code)] // used by flushes and compaction (Phases 5–6)
    pub fn reserve_file_id(&self) -> u64 {
        self.next_file_id.fetch_add(1, Ordering::Relaxed)
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

/// The tables of one level (Go's `levelHandler`).
pub(crate) struct LevelHandler {
    level: usize,
    tables: RwLock<Vec<Arc<Table>>>,
}

impl LevelHandler {
    fn new(level: usize) -> Self {
        LevelHandler {
            level,
            tables: RwLock::new(Vec::new()),
        }
    }

    fn init_tables(&self, mut tables: Vec<Arc<Table>>) {
        if self.level == 0 {
            tables.sort_by_key(|t| t.id());
        } else {
            tables.sort_by(|a, b| compare_keys(a.smallest(), b.smallest()));
        }
        *self.tables.write().unwrap() = tables;
    }

    /// Levels below L0 must hold sorted, non-overlapping tables.
    fn validate(&self) -> Result<()> {
        if self.level == 0 {
            return Ok(());
        }
        let tables = self.tables.read().unwrap();
        for (j, pair) in tables.windows(2).enumerate() {
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
        let tables = self.tables.read().unwrap();
        if self.level == 0 {
            return tables.iter().rev().cloned().collect();
        }
        let idx = tables.partition_point(|t| compare_keys(t.biggest(), key).is_lt());
        tables.get(idx).cloned().into_iter().collect()
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
        let tables = self.tables.read().unwrap();
        if self.level == 0 {
            // Newest first, so the merge iterator prefers newer tables.
            for t in tables
                .iter()
                .rev()
                .filter(|t| opt.pick_table(t, prefix_is_key))
            {
                iters.push(Box::new(t.iter(opt.reverse, true)));
            }
            return;
        }
        let picked = opt.pick_tables(&tables, prefix_is_key);
        if !picked.is_empty() {
            iters.push(Box::new(ConcatIterator::new(picked, opt.reverse, true)));
        }
    }
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}
