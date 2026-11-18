//! Leveled compaction. Port of Go's `compaction.go` and the compaction half
//! of `levels.go`.
//!
//! ```text
//! compactor threads ── every 50ms: pick_compact_levels ──► do_compact(priority)
//!   do_compact: fill_tables (pick top + overlapping bottom tables, register
//!               their key ranges in CompactStatus) ──► run_compact_def
//!   run_compact_def: merge top + bottom, split into key ranges, build new
//!                    tables per range in parallel ──► manifest ──► swap tables
//! ```
//!
//! - **Targets** (dynamic level sizes, as in RocksDB): the last level's size
//!   sets the target of each level above it (divided by the multiplier per
//!   level). L0 is compacted into the *base level*, the deepest level whose
//!   target fits `base_level_size`, never below a non-empty level.
//! - **Scores**: L0 by table count, other levels by size over target, then
//!   adjusted (Pebble's heuristic) to prefer draining lower levels first.
//! - **Dropping versions**: entries at or below the oracle's discard
//!   timestamp may go: all but `num_versions_to_keep` versions, everything
//!   below a delete, expiry or `discard_earlier_versions` marker. Deletes are
//!   kept while a lower level may still hold older versions of the key.
//!   Dropped value pointers are counted in the discard stats for value log
//!   GC.
//!
//! Differences from Go: sub-compactions run on scoped threads and each builds
//! its tables on its own thread (Go builds every table on a new goroutine).
//! L0 → L0 compactions pick a contiguous run of L0 tables, so the output can
//! take their place in the age order (Go may skip tables in the middle).

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use crossbeam_channel::{Receiver, RecvTimeoutError};

use crate::db::DbInner;
use crate::error::{Error, Result, ResultExt};
use crate::iterator::is_deleted_or_expired;
use crate::levels::{LevelState, LevelsController, hex, overlapping_tables};
use crate::manifest::{new_create_change, new_delete_change};
use crate::options::Options;
use crate::pb;
use crate::structs::{
    BIT_DISCARD_EARLIER_VERSIONS, BIT_MERGE_ENTRY, BIT_VALUE_POINTER, ValuePointer,
};
use crate::table::{ConcatIterator, MergeIterator, Table, TableBuilder, new_filename};
use crate::util::keys::{compare_keys, key_with_ts, parse_key, parse_ts, same_key};
use crate::util::value::ValueStruct;
use crate::util::{KvIterator, sync_dir};

/// A range of internal keys, `[left, right]` (Go's `keyRange`). Empty means
/// "unset"; `inf` covers everything.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct KeyRange {
    pub left: Vec<u8>,
    pub right: Vec<u8>,
    pub inf: bool,
}

impl KeyRange {
    pub fn inf() -> KeyRange {
        KeyRange {
            inf: true,
            ..Default::default()
        }
    }

    pub fn is_empty(&self) -> bool {
        self.left.is_empty() && self.right.is_empty() && !self.inf
    }

    /// Grows the range to cover `kr` too.
    pub fn extend(&mut self, kr: &KeyRange) {
        if kr.is_empty() {
            return;
        }
        if self.is_empty() {
            *self = kr.clone();
        }
        // (An infinite range has no keys to compare.)
        if self.left.is_empty()
            || (!kr.left.is_empty() && compare_keys(&kr.left, &self.left).is_lt())
        {
            self.left = kr.left.clone();
        }
        if self.right.is_empty()
            || (!kr.right.is_empty() && compare_keys(&kr.right, &self.right).is_gt())
        {
            self.right = kr.right.clone();
        }
        if kr.inf {
            self.inf = true;
        }
    }

    /// Whether the ranges share a key. An empty range overlaps everything,
    /// and nothing overlaps an empty `dst`.
    pub fn overlaps_with(&self, dst: &KeyRange) -> bool {
        if self.is_empty() {
            return true;
        }
        if dst.is_empty() {
            return false;
        }
        if self.inf || dst.inf {
            return true;
        }
        if compare_keys(&self.left, &dst.right).is_gt() {
            return false;
        }
        if compare_keys(&self.right, &dst.left).is_lt() {
            return false;
        }
        true
    }

    /// All versions of the smallest to the biggest key in `tables` (Go's
    /// `getKeyRange`).
    pub fn of_tables(tables: &[Arc<Table>]) -> KeyRange {
        let Some(first) = tables.first() else {
            return KeyRange::default();
        };
        let mut smallest = first.smallest();
        let mut biggest = first.biggest();
        for t in &tables[1..] {
            if compare_keys(t.smallest(), smallest).is_lt() {
                smallest = t.smallest();
            }
            if compare_keys(t.biggest(), biggest).is_gt() {
                biggest = t.biggest();
            }
        }
        KeyRange {
            left: key_with_ts(parse_key(smallest), u64::MAX),
            right: key_with_ts(parse_key(biggest), 0),
            inf: false,
        }
    }
}

impl std::fmt::Display for KeyRange {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "[left={}, right={}, inf={}]",
            hex(&self.left),
            hex(&self.right),
            self.inf
        )
    }
}

#[derive(Debug, Default)]
pub(crate) struct LevelCompactStatus {
    pub ranges: Vec<KeyRange>,
    /// Bytes of this level's tables being compacted away.
    pub del_size: u64,
}

impl LevelCompactStatus {
    fn overlaps_with(&self, dst: &KeyRange) -> bool {
        self.ranges.iter().any(|r| r.overlaps_with(dst))
    }

    fn remove(&mut self, dst: &KeyRange) -> bool {
        let before = self.ranges.len();
        self.ranges.retain(|r| r != dst);
        self.ranges.len() != before
    }
}

/// Key ranges and tables of the running compactions (Go's
/// `compactStatus`), so concurrent compactions never touch the same keys.
#[derive(Debug)]
pub(crate) struct CompactStatus {
    pub levels: Vec<LevelCompactStatus>,
    pub tables: HashSet<u64>,
}

impl CompactStatus {
    pub fn new(max_levels: usize) -> CompactStatus {
        CompactStatus {
            levels: (0..max_levels)
                .map(|_| LevelCompactStatus::default())
                .collect(),
            tables: HashSet::new(),
        }
    }

    fn overlaps_with(&self, level: usize, kr: &KeyRange) -> bool {
        self.levels[level].overlaps_with(kr)
    }

    /// Registers `cd` unless it overlaps a running compaction (Go's
    /// `compareAndAdd`). The caller holds read locks on both levels.
    fn compare_and_add(&mut self, cd: &CompactDef) -> bool {
        if self.levels[cd.this_level].overlaps_with(&cd.this_range)
            || self.levels[cd.next_level].overlaps_with(&cd.next_range)
        {
            return false;
        }
        self.levels[cd.this_level]
            .ranges
            .push(cd.this_range.clone());
        self.levels[cd.next_level]
            .ranges
            .push(cd.next_range.clone());
        self.levels[cd.this_level].del_size += cd.this_size;
        for t in cd.top.iter().chain(&cd.bot) {
            self.tables.insert(t.id());
        }
        true
    }

    /// Unregisters a finished compaction (Go's `delete`).
    pub(crate) fn delete(&mut self, cd: &CompactDef) {
        let this = &mut self.levels[cd.this_level];
        this.del_size -= cd.this_size;
        let mut found = this.remove(&cd.this_range);
        // Lmax → Lmax and L0 → L0 register a single range.
        if cd.this_level != cd.next_level && !cd.next_range.is_empty() {
            found = self.levels[cd.next_level].remove(&cd.next_range) && found;
        }
        assert!(
            found,
            "keyRange not found: {} in level {}, {} in level {}",
            cd.this_range, cd.this_level, cd.next_range, cd.next_level
        );
        for t in cd.top.iter().chain(&cd.bot) {
            assert!(
                self.tables.remove(&t.id()),
                "table {} not registered",
                t.id()
            );
        }
    }
}

/// Target sizes per level (Go's `targets`).
#[derive(Debug, Clone, Default)]
pub(crate) struct Targets {
    pub base_level: usize,
    pub target_sz: Vec<u64>,
    pub file_sz: Vec<u64>,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct CompactionPriority {
    pub level: usize,
    pub score: f64,
    pub adjusted: f64,
    pub drop_prefixes: Vec<Vec<u8>>,
    /// Computed when the compaction runs if unset.
    pub t: Option<Targets>,
}

impl CompactionPriority {
    pub fn new(level: usize, score: f64) -> CompactionPriority {
        CompactionPriority {
            level,
            score,
            ..Default::default()
        }
    }
}

/// One compaction: `top` tables of `this_level` merged with the overlapping
/// `bot` tables of `next_level` (Go's `compactDef`).
#[derive(Debug, Clone, Default)]
pub(crate) struct CompactDef {
    pub compactor_id: i32,
    pub t: Targets,
    pub p: CompactionPriority,
    pub this_level: usize,
    pub next_level: usize,
    pub top: Vec<Arc<Table>>,
    pub bot: Vec<Arc<Table>>,
    pub this_range: KeyRange,
    pub next_range: KeyRange,
    pub splits: Vec<KeyRange>,
    pub this_size: u64,
    pub drop_prefixes: Vec<Vec<u8>>,
}

impl CompactDef {
    fn all_tables(&self) -> Vec<Arc<Table>> {
        self.top.iter().chain(&self.bot).cloned().collect()
    }
}

fn has_any_prefixes(key: &[u8], prefixes: &[Vec<u8>]) -> bool {
    prefixes.iter().any(|p| key.starts_with(p))
}

/// Whether `t` may hold a key starting with `prefix` (Go's `containsPrefix`).
pub(crate) fn contains_prefix(t: &Arc<Table>, prefix: &[u8]) -> bool {
    let (small, large) = (t.smallest(), t.biggest());
    if small.starts_with(prefix) || large.starts_with(prefix) {
        return true;
    }
    if prefix > small && prefix < large {
        // The range covers the prefix, but a key with it may not exist.
        let mut it = t.iter(false, true);
        it.seek(&key_with_ts(prefix, u64::MAX));
        return it.valid() && it.key().starts_with(prefix);
    }
    false
}

fn contains_any_prefixes(t: &Arc<Table>, prefixes: &[Vec<u8>]) -> bool {
    prefixes.iter().any(|p| contains_prefix(t, p))
}

/// Read guards on the two levels of a compaction (one guard when they are
/// the same level: std's `RwLock` must not be read-locked twice).
struct LevelGuards<'a> {
    this: std::sync::RwLockReadGuard<'a, LevelState>,
    next: Option<std::sync::RwLockReadGuard<'a, LevelState>>,
}

impl LevelGuards<'_> {
    fn next(&self) -> &LevelState {
        self.next.as_deref().unwrap_or(&self.this)
    }
}

impl LevelsController {
    fn lock_levels(&self, cd: &CompactDef) -> LevelGuards<'_> {
        let this = self.levels[cd.this_level].read();
        let next = (cd.next_level != cd.this_level).then(|| self.levels[cd.next_level].read());
        LevelGuards { this, next }
    }

    /// Target sizes of the levels and the base level that L0 compacts into
    /// (Go's `levelTargets`).
    pub(crate) fn level_targets(&self, opt: &Options) -> Targets {
        let n = self.levels.len();
        let base_size = opt.base_level_size;
        let adjust = |sz: u64| sz.max(base_size);
        let mut t = Targets {
            base_level: 0,
            target_sz: vec![0; n],
            file_sz: vec![0; n],
        };
        // The DB's size is the size of the last level.
        let mut db_size = self.last_level().total_size();
        for i in (1..n).rev() {
            let ltarget = adjust(db_size);
            t.target_sz[i] = ltarget;
            // The desired base level is the deepest one within base_size.
            if t.base_level == 0 && ltarget <= base_size {
                t.base_level = i;
            }
            db_size /= opt.level_size_multiplier as u64;
        }
        // A very big tree can leave no level within base_size: use L1.
        if t.base_level == 0 {
            t.base_level = 1;
        }
        // Bring the base level down through empty levels.
        let mut i = t.base_level;
        while i + 1 < n {
            if self.levels[i].total_size() > 0 {
                break;
            }
            t.base_level = i;
            i += 1;
        }
        // An empty base level with a small next level: use the next one.
        let b = t.base_level;
        if b + 1 < n
            && self.levels[b].total_size() == 0
            && self.levels[b + 1].total_size() < t.target_sz[b + 1]
            && self.levels[b + 1].total_size() < base_size
        {
            t.base_level += 1;
        }
        // Never compact L0 below a non-empty level: newer versions would end
        // up under older ones.
        if let Some(i) = (1..t.base_level).find(|&i| self.levels[i].total_size() > 0) {
            t.base_level = i;
        }

        let mut tsz = opt.base_table_size;
        for i in 0..n {
            t.file_sz[i] = if i == 0 {
                // L0 is compacted by table count; one table per memtable.
                opt.mem_table_size
            } else if i <= t.base_level {
                tsz
            } else {
                tsz *= opt.table_size_multiplier as u64;
                tsz
            };
        }
        if t.base_level == 0 {
            t.base_level = 1;
        }
        t
    }

    /// The levels worth compacting, best first (Go's `pickCompactLevels`).
    pub(crate) fn pick_compact_levels(&self, opt: &Options) -> Vec<CompactionPriority> {
        let t = self.level_targets(opt);
        let n = self.levels.len();
        let mut prios = Vec::with_capacity(n);
        let mut add = |level: usize, score: f64| {
            prios.push(CompactionPriority {
                level,
                score,
                adjusted: score,
                drop_prefixes: Vec::new(),
                t: Some(t.clone()),
            })
        };
        add(
            0,
            self.levels[0].num_tables() as f64 / opt.num_level_zero_tables as f64,
        );
        for i in 1..n {
            // Tables already being compacted away do not count.
            let del_size = self.cstatus.lock().unwrap().levels[i].del_size;
            let sz = self.levels[i].total_size().saturating_sub(del_size);
            add(i, sz as f64 / t.target_sz[i] as f64);
        }

        // From Pebble: divide a level's score by the next level's, so an
        // overflowing lower level is drained before more is pushed into it.
        const MIN_SCORE: f64 = 0.01;
        let mut prev = 0;
        for level in t.base_level..n {
            if prios[prev].adjusted >= 1.0 {
                if prios[level].score >= MIN_SCORE {
                    prios[prev].adjusted /= prios[level].adjusted;
                } else {
                    prios[prev].adjusted /= MIN_SCORE;
                }
            }
            prev = level;
        }

        // Levels with a raw score of at least 1, the last level excluded,
        // sorted by adjusted score.
        prios.truncate(n - 1);
        prios.retain(|p| p.score >= 1.0);
        prios.sort_by(|a, b| b.adjusted.total_cmp(&a.adjusted));
        prios
    }

    /// Picks the tables for an L0 compaction: into the base level, or else
    /// within L0 (Go's `fillTablesL0`).
    fn fill_tables_l0(&self, cd: &mut CompactDef) -> bool {
        self.fill_tables_l0_to_lbase(cd) || self.fill_tables_l0_to_l0(cd)
    }

    fn fill_tables_l0_to_lbase(&self, cd: &mut CompactDef) -> bool {
        assert!(cd.next_level != 0, "Base level can't be zero.");
        // A zero adjusted score comes from callers forcing a compaction.
        if cd.p.adjusted > 0.0 && cd.p.adjusted < 1.0 {
            return false;
        }
        let guards = self.lock_levels(cd);
        let top = &guards.this.tables;
        if top.is_empty() {
            return false;
        }
        let out: Vec<Arc<Table>> = if !cd.drop_prefixes.is_empty() {
            // Dropping prefixes compacts all of L0.
            top.clone()
        } else {
            // The oldest tables first, as long as they overlap each other.
            let mut kr = KeyRange::default();
            let mut out = Vec::new();
            for t in top {
                let dkr = KeyRange::of_tables(std::slice::from_ref(t));
                if !kr.overlaps_with(&dkr) {
                    break;
                }
                out.push(Arc::clone(t));
                kr.extend(&dkr);
            }
            out
        };
        cd.this_range = KeyRange::of_tables(&out);
        cd.top = out;
        let next = guards.next();
        let (left, right) = overlapping_tables(&next.tables, &cd.this_range);
        cd.bot = next.tables[left..right].to_vec();
        cd.next_range = if cd.bot.is_empty() {
            cd.this_range.clone()
        } else {
            KeyRange::of_tables(&cd.bot)
        };
        self.cstatus.lock().unwrap().compare_and_add(cd)
    }

    /// Merges at least four L0 tables into one, when L0 cannot go down
    /// (Go's `fillTablesL0ToL0`). Only compactor 0 does this, and it blocks
    /// other L0 compactions meanwhile.
    fn fill_tables_l0_to_l0(&self, cd: &mut CompactDef) -> bool {
        if cd.compactor_id != 0 {
            return false;
        }
        cd.next_level = 0;
        cd.next_range = KeyRange::default();
        cd.bot.clear();
        assert_eq!(cd.this_level, 0);

        let l0 = self.levels[0].read();
        let mut cs = self.cstatus.lock().unwrap();
        let now = SystemTime::now();
        let mut out = Vec::new();
        for t in &l0.tables {
            let too_big = t.size() >= 2 * cd.t.file_sz[0];
            let busy = cs.tables.contains(&t.id());
            // Tables just flushed are the newest: stop there.
            let recent = now
                .duration_since(t.created_at())
                .is_ok_and(|age| age < Duration::from_secs(10));
            if recent {
                break;
            }
            if too_big || busy {
                // Skip ineligible tables before the run; after it, the run
                // ends (it must stay contiguous in the age order).
                if out.is_empty() {
                    continue;
                }
                break;
            }
            out.push(Arc::clone(t));
        }
        if out.len() < 4 {
            return false;
        }
        cd.this_range = KeyRange::inf();
        // Keep any other L0 → Lbase from starting meanwhile.
        cs.levels[0].ranges.push(KeyRange::inf());
        for t in &out {
            cs.tables.insert(t.id());
        }
        cd.top = out;
        // The output is always one table, which reduces L0 stalls.
        cd.t.file_sz[0] = u32::MAX as u64;
        true
    }

    /// Picks the tables of a level below L0 to compact into the next level
    /// (Go's `fillTables`).
    pub(crate) fn fill_tables(&self, db: &DbInner, cd: &mut CompactDef) -> bool {
        let guards = self.lock_levels(cd);
        let mut tables = guards.this.tables.clone();
        if tables.is_empty() {
            return false;
        }
        if cd.this_level == self.levels.len() - 1 {
            return self.fill_max_level_tables(db, &guards, tables, cd);
        }
        // Oldest data first (RocksDB's kOldestLargestSeqFirst).
        tables.sort_by_key(|t| t.max_version());
        let next = guards.next();
        for t in tables {
            cd.this_size = t.size();
            cd.this_range = KeyRange::of_tables(std::slice::from_ref(&t));
            let mut cs = self.cstatus.lock().unwrap();
            if cs.overlaps_with(cd.this_level, &cd.this_range) {
                continue;
            }
            cd.top = vec![t];
            let (left, right) = overlapping_tables(&next.tables, &cd.this_range);
            cd.bot = next.tables[left..right].to_vec();
            if cd.bot.is_empty() {
                cd.next_range = cd.this_range.clone();
            } else {
                cd.next_range = KeyRange::of_tables(&cd.bot);
                if cs.overlaps_with(cd.next_level, &cd.next_range) {
                    continue;
                }
            }
            if cs.compare_and_add(cd) {
                return true;
            }
        }
        false
    }

    /// Picks last-level tables with enough stale data to rewrite in place
    /// (Go's `fillMaxLevelTables`).
    fn fill_max_level_tables(
        &self,
        db: &DbInner,
        guards: &LevelGuards<'_>,
        tables: Vec<Arc<Table>>,
        cd: &mut CompactDef,
    ) -> bool {
        let mut sorted = tables.clone();
        sorted.sort_by_key(|t| std::cmp::Reverse(t.stale_data_size()));
        if sorted.first().is_some_and(|t| t.stale_data_size() == 0) {
            return false;
        }
        let level_tables = &guards.this.tables;
        let collect_bot = |cd: &mut CompactDef, t: &Arc<Table>, need_sz: u64| {
            let mut total = t.size();
            let mut j =
                level_tables.partition_point(|x| compare_keys(x.smallest(), t.smallest()).is_lt());
            assert_eq!(level_tables[j].id(), t.id());
            j += 1;
            while j < level_tables.len() {
                let new_t = &level_tables[j];
                total += new_t.size();
                if total >= need_sz {
                    break;
                }
                cd.bot.push(Arc::clone(new_t));
                cd.next_range
                    .extend(&KeyRange::of_tables(std::slice::from_ref(new_t)));
                j += 1;
            }
        };
        let now = SystemTime::now();
        let discard_ts = db.orc.discard_at_or_below();
        cd.bot.clear();
        for t in sorted {
            // Nothing to clean if the data is newer than what reads can drop.
            if t.max_version() > discard_ts {
                continue;
            }
            if now
                .duration_since(t.created_at())
                .is_ok_and(|age| age < Duration::from_secs(3600))
            {
                continue;
            }
            // Under 10 MB of stale data is not worth a rewrite.
            if t.stale_data_size() < 10 << 20 {
                continue;
            }
            cd.this_size = t.size();
            cd.this_range = KeyRange::of_tables(std::slice::from_ref(&t));
            // Same range, so several max level compactions can run at once.
            cd.next_range = cd.this_range.clone();
            if self
                .cstatus
                .lock()
                .unwrap()
                .overlaps_with(cd.this_level, &cd.this_range)
            {
                continue;
            }
            cd.top = vec![Arc::clone(&t)];
            let need = cd.t.file_sz[cd.this_level];
            if t.size() >= need {
                break;
            }
            // Small tables: merge several neighbours into one.
            collect_bot(cd, &t, need);
            if !self.cstatus.lock().unwrap().compare_and_add(cd) {
                cd.bot.clear();
                cd.next_range = KeyRange::default();
                continue;
            }
            return true;
        }
        if cd.top.is_empty() {
            return false;
        }
        self.cstatus.lock().unwrap().compare_and_add(cd)
    }

    /// Splits the bottom tables into at most 5 key ranges, compacted in
    /// parallel (Go's `addSplits`).
    fn add_splits(cd: &mut CompactDef) {
        cd.splits.clear();
        let width = cd.bot.len().div_ceil(5).max(3);
        let mut skr = cd.this_range.clone();
        skr.extend(&cd.next_range);
        for (i, t) in cd.bot.iter().enumerate() {
            if i == cd.bot.len() - 1 {
                skr.right.clear();
                cd.splits.push(skr);
                return;
            }
            if i % width == width - 1 {
                // Version 0 sorts last, so the split takes every version of
                // the table's biggest key.
                skr.right = key_with_ts(parse_key(t.biggest()), 0);
                cd.splits.push(skr.clone());
                skr.left = skr.right.clone();
            }
        }
    }

    /// Merges the compaction's tables into new tables for the next level
    /// (Go's `compactBuildTables`). Returns them sorted by biggest key.
    fn compact_build_tables(
        &self,
        db: &DbInner,
        lev: usize,
        cd: &CompactDef,
    ) -> Result<Vec<Arc<Table>>> {
        // Bottom tables made only of dropped prefixes can go without reading.
        let valid: Vec<Arc<Table>> = cd
            .bot
            .iter()
            .filter(|t| {
                !cd.drop_prefixes
                    .iter()
                    .any(|p| t.smallest().starts_with(p) && t.biggest().starts_with(p))
            })
            .cloned()
            .collect();
        let new_iterator = || -> Option<Box<dyn KvIterator>> {
            let mut iters: Vec<Box<dyn KvIterator>> = Vec::new();
            if lev == 0 {
                // Newest first: the merge prefers earlier iterators.
                for t in cd.top.iter().rev() {
                    iters.push(Box::new(t.iter(false, false)));
                }
            } else if let Some(t) = cd.top.first() {
                assert_eq!(cd.top.len(), 1);
                iters.push(Box::new(t.iter(false, false)));
            }
            if !valid.is_empty() {
                iters.push(Box::new(ConcatIterator::new(valid.clone(), false, false)));
            }
            MergeIterator::merge(iters, false)
        };

        let results: Vec<Result<Vec<Arc<Table>>>> = std::thread::scope(|s| {
            let handles: Vec<_> = cd
                .splits
                .iter()
                .map(|kr| {
                    let new_iterator = &new_iterator;
                    std::thread::Builder::new()
                        .name("badger-subcompact".into())
                        .spawn_scoped(s, move || match new_iterator() {
                            Some(it) => self.subcompact(db, it, kr, cd),
                            None => Ok(Vec::new()),
                        })
                        .map_err(Error::from)
                })
                .collect();
            handles
                .into_iter()
                .map(|h| h.and_then(|h| h.join().expect("subcompaction panicked")))
                .collect()
        });

        let mut new_tables = Vec::new();
        let mut first_err = None;
        for r in results {
            match r {
                Ok(ts) => new_tables.extend(ts),
                Err(e) => {
                    first_err.get_or_insert(e);
                }
            }
        }
        if first_err.is_none() && !db.opt.in_memory {
            // Make the new files' directory entries durable.
            if let Err(e) = sync_dir(&db.opt.dir) {
                first_err = Some(e);
            }
        }
        if let Some(e) = first_err {
            for t in &new_tables {
                t.delete_on_drop();
            }
            return Err(e).with_context(|| {
                format!(
                    "while running compactions for: L{} -> L{}",
                    cd.this_level, cd.next_level
                )
            });
        }
        new_tables.sort_by(|a, b| compare_keys(a.biggest(), b.biggest()));
        Ok(new_tables)
    }

    /// Compacts the keys of one split (Go's `subcompact`), building tables
    /// of the next level's target size.
    fn subcompact(
        &self,
        db: &DbInner,
        mut it: Box<dyn KvIterator>,
        kr: &KeyRange,
        cd: &CompactDef,
    ) -> Result<Vec<Arc<Table>>> {
        let opt = &db.opt;
        // Deletes must stay while a lower level may hold older versions.
        let has_overlap = self.check_overlap(&cd.all_tables(), cd.next_level + 1);
        // Versions above this may still be read by a transaction.
        let mut discard_ts = db.orc.discard_at_or_below();
        // A value log GC rewrite writes values back with their old versions;
        // a delete committed after it started must outlive the rewrite, or
        // the deleted key would reappear (Go #2286).
        if let Some(gc_max) = db.vlog.gc_discard_ts()
            && gc_max > 0
            && gc_max < discard_ts
        {
            discard_ts = gc_max;
        }

        let mut discard_stats: HashMap<u32, i64> = HashMap::new();
        let mut update_stats = |vs: &ValueStruct<'_>| {
            if !opt.in_memory
                && vs.meta & BIT_VALUE_POINTER != 0
                && let Ok(vp) = ValuePointer::decode(vs.value)
            {
                *discard_stats.entry(vp.fid).or_default() += vp.len as i64;
            }
        };
        // Whether a new table's range would overlap 10 or more tables two
        // levels down, which makes the next compaction expensive.
        let exceeds_allowed_overlap = |kr: &KeyRange| {
            let n2n = cd.next_level + 1;
            if n2n <= 1 || n2n >= self.levels.len() {
                return false;
            }
            let (l, r) = overlapping_tables(&self.levels[n2n].read().tables, kr);
            r - l >= 10
        };

        let mut last_key: Vec<u8> = Vec::new();
        let mut skip_key: Vec<u8> = Vec::new();
        let mut num_versions = 0usize;
        // The key's newest version has discard_earlier_versions set.
        let mut first_key_has_discard_set = false;
        let mut out = Vec::new();

        let result = (|| -> Result<()> {
            if kr.left.is_empty() {
                it.rewind();
            } else {
                it.seek(&kr.left);
            }
            while it.valid() {
                if !kr.right.is_empty() && compare_keys(it.key(), &kr.right).is_ge() {
                    break;
                }
                let mut bopts = db.table_opts.clone();
                bopts.table_size = cd.t.file_sz[cd.next_level];
                bopts.data_key = db.registry.latest_data_key()?;
                let mut builder = TableBuilder::new(bopts.clone());
                let start = Instant::now();
                let (mut num_keys, mut num_skips) = (0u64, 0u64);
                let mut range_check = 0u64;
                let mut table_kr = KeyRange::default();

                while it.valid() {
                    let key = it.key();
                    if !cd.drop_prefixes.is_empty() && has_any_prefixes(key, &cd.drop_prefixes) {
                        num_skips += 1;
                        update_stats(&it.value());
                        it.next();
                        continue;
                    }
                    if !skip_key.is_empty() {
                        if same_key(key, &skip_key) {
                            num_skips += 1;
                            update_stats(&it.value());
                            it.next();
                            continue;
                        }
                        skip_key.clear();
                    }
                    if !same_key(key, &last_key) {
                        first_key_has_discard_set = false;
                        if !kr.right.is_empty() && compare_keys(key, &kr.right).is_ge() {
                            break;
                        }
                        // All versions of a key stay in one table.
                        if builder.reached_capacity() {
                            break;
                        }
                        last_key.clear();
                        last_key.extend_from_slice(key);
                        num_versions = 0;
                        first_key_has_discard_set =
                            it.value().meta & BIT_DISCARD_EARLIER_VERSIONS != 0;
                        if table_kr.left.is_empty() {
                            table_kr.left = key.to_vec();
                        }
                        table_kr.right = last_key.clone();
                        range_check += 1;
                        // Checked every 5000 keys to amortize its cost.
                        if range_check.is_multiple_of(5000) && exceeds_allowed_overlap(&table_kr) {
                            break;
                        }
                    }

                    let vs = it.value();
                    let version = parse_ts(key);
                    let is_expired = is_deleted_or_expired(vs.meta, vs.expires_at);
                    // Merge operands stay until they are merged.
                    if version <= discard_ts && vs.meta & BIT_MERGE_ENTRY == 0 {
                        // Only versions no transaction can read count.
                        num_versions += 1;
                        let last_valid_version = vs.meta & BIT_DISCARD_EARLIER_VERSIONS != 0
                            || num_versions == opt.num_versions_to_keep;
                        if is_expired || last_valid_version {
                            // Every older version goes.
                            skip_key = key.to_vec();
                            if (!is_expired && last_valid_version) || has_overlap {
                                // Keep this version (a delete marker must
                                // shadow older versions in lower levels).
                            } else {
                                num_skips += 1;
                                update_stats(&vs);
                                it.next();
                                continue;
                            }
                        }
                    }
                    num_keys += 1;
                    let vlen = if vs.meta & BIT_VALUE_POINTER != 0 {
                        ValuePointer::decode(vs.value)?.len
                    } else {
                        0
                    };
                    if first_key_has_discard_set || is_expired {
                        // The next compaction may drop it.
                        builder.add_stale_key(key, vs, vlen);
                    } else {
                        builder.add(key, vs, vlen);
                    }
                    it.next();
                }
                log::debug!(
                    "[{}] LOG Compact. Added {num_keys} keys. Skipped {num_skips} keys. \
                     Iteration took: {:?}",
                    cd.compactor_id,
                    start.elapsed()
                );
                if builder.is_empty() {
                    continue;
                }
                let id = self.reserve_file_id();
                let table = if opt.in_memory {
                    Table::open_in_memory(builder.finish()?, id, bopts)?
                } else {
                    Table::create(new_filename(id, &opt.dir), builder)?
                };
                out.push(table);
            }
            Ok(())
        })();
        if let Err(e) = result {
            for t in &out {
                t.delete_on_drop();
            }
            return Err(e);
        }
        db.vlog.update_discard_stats(&discard_stats);
        log::debug!("Discard stats: {discard_stats:?}");
        Ok(out)
    }

    /// Runs a compaction picked by `do_compact` or built by the caller (Go's
    /// `runCompactDef`).
    pub(crate) fn run_compact_def(
        &self,
        db: &DbInner,
        id: i32,
        l: usize,
        cd: &mut CompactDef,
    ) -> Result<()> {
        if cd.t.file_sz.is_empty() {
            return Err(Error::InvalidRequest)
                .context("Filesizes cannot be zero. Targets are not set");
        }
        if cd.this_level == 0 {
            for i in (1..cd.next_level).rev() {
                if self.levels[i].total_size() > 0 {
                    return Err(Error::InvalidRequest).with_context(|| {
                        format!(
                            "there is a non-empty level {i} above base level {}",
                            cd.next_level
                        )
                    });
                }
            }
        }
        let start = Instant::now();
        assert!(cd.splits.is_empty());
        // L0 → L0 and Lmax → Lmax rewrite a level in place: no splits.
        if cd.this_level != cd.next_level {
            Self::add_splits(cd);
        }
        if cd.splits.is_empty() {
            cd.splits.push(KeyRange::default());
        }

        // Tables are always rewritten, never moved, so stale versions go.
        let new_tables = self.compact_build_tables(db, l, cd)?;
        let changes = build_change_set(cd, &new_tables);
        // The manifest is written after the new files and before the old
        // ones are deleted.
        if let Err(e) = db.manifest.add_changes(changes.changes) {
            for t in &new_tables {
                t.delete_on_drop();
            }
            return Err(e);
        }
        // Readers go from L0 down, so add to the next level before removing
        // from this one.
        self.levels[cd.next_level].replace_tables(&cd.bot, &new_tables);
        self.levels[cd.this_level].delete_tables(&cd.top);
        if cd.this_level == 0 && !cd.top.is_empty() {
            self.signal_l0_drained();
        }

        let dur = start.elapsed();
        if dur > Duration::from_secs(2) {
            let names = |ts: &[Arc<Table>]| {
                ts.iter()
                    .map(|t| format!("{:05}", t.id()))
                    .collect::<Vec<_>>()
                    .join(" ")
            };
            log::info!(
                "[{id}] LOG Compact {}->{} ({}, {} -> {} tables with {} splits). \
                 [{} {}] -> [{}], took {dur:?}",
                cd.this_level,
                cd.next_level,
                cd.top.len(),
                cd.bot.len(),
                new_tables.len(),
                cd.splits.len(),
                names(&cd.top),
                names(&cd.bot),
                names(&new_tables),
            );
        }
        Ok(())
    }

    /// Picks tables for priority `p` and compacts them (Go's `doCompact`).
    /// Returns `Ok(false)` when no tables could be picked (Go's
    /// `errFillTables`).
    pub(crate) fn do_compact(&self, db: &DbInner, id: i32, p: CompactionPriority) -> Result<bool> {
        let l = p.level;
        assert!(l < self.levels.len());
        let t = p.t.clone().unwrap_or_else(|| self.level_targets(&db.opt));
        let mut cd = CompactDef {
            compactor_id: id,
            t,
            drop_prefixes: p.drop_prefixes.clone(),
            p,
            this_level: l,
            ..Default::default()
        };
        // The picked tables cannot change until they are registered.
        if l == 0 {
            cd.next_level = cd.t.base_level;
            if !self.fill_tables_l0(&mut cd) {
                return Ok(false);
            }
        } else {
            cd.next_level = if l == self.levels.len() - 1 { l } else { l + 1 };
            if !self.fill_tables(db, &mut cd) {
                return Ok(false);
            }
        }
        let res = self.run_compact_def(db, id, l, &mut cd);
        self.cstatus.lock().unwrap().delete(&cd);
        match res {
            Ok(()) => {
                log::debug!("[Compactor: {id}] Compaction for level: {l} DONE");
                Ok(true)
            }
            Err(e) => {
                log::warn!("[Compactor: {id}] LOG Compact FAILED with error: {e}");
                Err(e)
            }
        }
    }

    /// Removes every key with one of `prefixes` from the levels (Go's
    /// `dropPrefixes`). Compactions must be stopped.
    pub(crate) fn drop_prefixes(&self, db: &DbInner, prefixes: &[Vec<u8>]) -> Result<()> {
        // Bottom up: dropping the oldest data first means a read never finds
        // an older version once a newer one is gone.
        for l in self.levels.iter().rev() {
            if l.level == 0 {
                if l.num_tables() > 0 {
                    // A score above 1 forces the compaction; 1.74 marks it in
                    // logs.
                    let mut cp = CompactionPriority::new(0, 1.74);
                    cp.drop_prefixes = prefixes.to_vec();
                    if let Err(e) = self.do_compact(db, 174, cp) {
                        log::warn!("While compacting level 0: {e}");
                        return Ok(());
                    }
                }
                continue;
            }
            // Groups of consecutive tables that may hold a prefix.
            let mut groups: Vec<Vec<Arc<Table>>> = Vec::new();
            let mut group = Vec::new();
            for t in &l.read().tables {
                if contains_any_prefixes(t, prefixes) {
                    group.push(Arc::clone(t));
                } else if !group.is_empty() {
                    groups.push(std::mem::take(&mut group));
                }
            }
            if !group.is_empty() {
                groups.push(group);
            }
            if groups.is_empty() {
                continue;
            }
            log::info!(
                "Dropping prefix at level {} ({} tableGroups)",
                l.level,
                groups.len()
            );
            for bot in groups {
                let mut t = self.level_targets(&db.opt);
                t.base_level = l.level;
                let mut cd = CompactDef {
                    compactor_id: -1,
                    t,
                    this_level: l.level,
                    next_level: l.level,
                    bot,
                    drop_prefixes: prefixes.to_vec(),
                    ..Default::default()
                };
                self.run_compact_def(db, -1, l.level, &mut cd)
                    .inspect_err(|e| log::warn!("While running compact def: {e}"))?;
            }
        }
        Ok(())
    }
}

/// Manifest changes for a finished compaction (Go's `buildChangeSet`).
fn build_change_set(cd: &CompactDef, new_tables: &[Arc<Table>]) -> pb::ManifestChangeSet {
    let mut changes: Vec<pb::ManifestChange> = new_tables
        .iter()
        .map(|t| {
            new_create_change(
                t.id(),
                cd.next_level as u32,
                t.key_id(),
                t.compression_type(),
            )
        })
        .collect();
    changes.extend(
        cd.top
            .iter()
            .filter(|t| !t.is_in_memory())
            .map(|t| new_delete_change(t.id())),
    );
    changes.extend(cd.bot.iter().map(|t| new_delete_change(t.id())));
    pb::ManifestChangeSet { changes }
}

/// A compactor thread (Go's `runCompactor`): every 50ms, compacts the best
/// level that needs it, until `stop` is disconnected.
pub(crate) fn run_compactor(db: &DbInner, id: i32, stop: Receiver<()>) {
    // Desynchronize the compactors.
    let nanos = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map_or(0, |d| d.subsec_nanos() as u64);
    let delay = Duration::from_millis((nanos.wrapping_mul(id as u64 + 1)) % 1000);
    if let Err(RecvTimeoutError::Disconnected) = stop.recv_timeout(delay) {
        return;
    }
    let lc = &db.lc;
    let run = |p: CompactionPriority| match lc.do_compact(db, id, p) {
        Ok(done) => done,
        Err(e) => {
            log::warn!("While running doCompact: {e}");
            false
        }
    };
    let run_once = || {
        let mut prios = lc.pick_compact_levels(&db.opt);
        if id == 0 {
            // Compactor 0 prefers L0.
            if let Some(idx) = prios.iter().position(|p| p.level == 0) {
                let p = prios.remove(idx);
                prios.insert(0, p);
            }
        }
        for p in prios {
            if !(id == 0 && p.level == 0) && p.adjusted < 1.0 {
                break;
            }
            if run(p) {
                return true;
            }
        }
        false
    };
    let mut count = 0;
    loop {
        match stop.recv_timeout(Duration::from_millis(50)) {
            Err(RecvTimeoutError::Timeout) => {}
            _ => return,
        }
        count += 1;
        // Every 10 seconds, compactor 2 rewrites the last level if enabled.
        if db.opt.lmax_compaction && id == 2 && count >= 200 {
            let mut p = CompactionPriority::new(lc.levels.len() - 1, 0.0);
            p.t = Some(lc.level_targets(&db.opt));
            run(p);
            count = 0;
        } else {
            run_once();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn kr(l: &[u8], r: &[u8]) -> KeyRange {
        KeyRange {
            left: key_with_ts(l, u64::MAX),
            right: key_with_ts(r, 0),
            inf: false,
        }
    }

    #[test]
    fn key_range_overlap_and_extend() {
        let empty = KeyRange::default();
        assert!(empty.overlaps_with(&kr(b"a", b"b")));
        assert!(!kr(b"a", b"b").overlaps_with(&empty));
        assert!(kr(b"a", b"c").overlaps_with(&kr(b"b", b"d")));
        assert!(kr(b"a", b"b").overlaps_with(&kr(b"b", b"d")));
        assert!(!kr(b"a", b"b").overlaps_with(&kr(b"c", b"d")));
        assert!(!kr(b"c", b"d").overlaps_with(&kr(b"a", b"b")));
        assert!(KeyRange::inf().overlaps_with(&kr(b"x", b"y")));

        let mut r = KeyRange::default();
        r.extend(&kr(b"m", b"n"));
        assert_eq!(r, kr(b"m", b"n"));
        r.extend(&kr(b"a", b"b"));
        r.extend(&kr(b"x", b"z"));
        assert_eq!(r, kr(b"a", b"z"));
        r.extend(&KeyRange::inf());
        assert!(r.inf);
    }

    #[test]
    fn compact_status_add_and_delete() {
        let mut cs = CompactStatus::new(3);
        let cd = CompactDef {
            this_level: 1,
            next_level: 2,
            this_range: kr(b"a", b"c"),
            next_range: kr(b"a", b"d"),
            this_size: 10,
            ..Default::default()
        };
        assert!(cs.compare_and_add(&cd));
        assert_eq!(cs.levels[1].del_size, 10);
        let mut other = cd.clone();
        other.this_range = kr(b"b", b"b");
        assert!(!cs.compare_and_add(&other), "overlaps the running one");
        other.this_range = kr(b"x", b"y");
        other.next_range = kr(b"x", b"y");
        assert!(cs.compare_and_add(&other));
        cs.delete(&cd);
        cs.delete(&other);
        assert!(
            cs.levels
                .iter()
                .all(|l| l.ranges.is_empty() && l.del_size == 0)
        );
    }
}
