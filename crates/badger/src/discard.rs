//! Discard statistics: how many bytes of each value log file are no longer
//! referenced, so value log GC can pick the file worth rewriting. Port of
//! Go's `discard.go`.
//!
//! The stats live in the mmapped `DISCARD` file in the value directory: a
//! sorted array of 16-byte slots, `fid (u64 BE) | discard bytes (u64 BE)`,
//! ending at the first slot whose fid is zero (value log ids start at 1).
//! The file starts at 1 MiB (65,536 slots) and doubles when full.
//!
//! Compaction adds the length of every value pointer it drops; GC resets a
//! file's entry once the file is rewritten.

use std::path::Path;

use crate::error::{Result, ResultExt};
use crate::util::MmapFile;

/// Name of the discard stats file.
pub const DISCARD_FILENAME: &str = "DISCARD";

const SLOT: usize = 16;
const INITIAL_SIZE: u64 = 1 << 20;

pub(crate) struct DiscardStats {
    file: MmapFile,
    next_empty_slot: usize,
}

impl DiscardStats {
    /// Opens or creates `DISCARD` in `value_dir` (Go's `InitDiscardStats`).
    pub fn open(value_dir: &Path) -> Result<DiscardStats> {
        let path = value_dir.join(DISCARD_FILENAME);
        let (file, _new) = MmapFile::open(&path, true, false, INITIAL_SIZE)
            .with_context(|| format!("while opening file: {DISCARD_FILENAME}"))?;
        let mut ds = DiscardStats {
            file,
            next_empty_slot: 0,
        };
        // A new file is all zeros already. An existing one ends at the first
        // empty slot (or fills the whole file).
        ds.next_empty_slot = (0..ds.max_slot())
            .find(|&slot| ds.get(SLOT * slot) == 0)
            .unwrap_or(ds.max_slot());
        if ds.next_empty_slot == ds.max_slot() {
            ds.grow()?;
        }
        ds.sort();
        log::info!("Discard stats nextEmptySlot: {}", ds.next_empty_slot);
        Ok(ds)
    }

    fn get(&self, offset: usize) -> u64 {
        u64::from_be_bytes(self.file.data()[offset..offset + 8].try_into().unwrap())
    }

    fn set(&mut self, offset: usize, val: u64) {
        self.file.data_mut()[offset..offset + 8].copy_from_slice(&val.to_be_bytes());
    }

    fn max_slot(&self) -> usize {
        self.file.data().len() / SLOT
    }

    /// Zeroes the slot after the last entry, marking the end.
    fn zero_out(&mut self) {
        let off = self.next_empty_slot * SLOT;
        self.set(off, 0);
        self.set(off + 8, 0);
    }

    fn grow(&mut self) -> Result<()> {
        let size = 2 * self.file.data().len() as u64;
        self.file.truncate(size.max(INITIAL_SIZE))
    }

    /// Sorts the used slots by fid.
    fn sort(&mut self) {
        let n = self.next_empty_slot * SLOT;
        let mut slots: Vec<[u8; SLOT]> = self.file.data()[..n].as_chunks::<SLOT>().0.to_vec();
        slots.sort_by_key(|s| u64::from_be_bytes(s[..8].try_into().unwrap()));
        self.file.data_mut()[..n].copy_from_slice(slots.as_flattened());
    }

    /// Adds `discard` bytes to file `fid` and returns its new total (Go's
    /// `Update`). `discard == 0` only reads the total; `discard < 0` resets
    /// it to zero.
    pub fn update(&mut self, fid: u32, discard: i64) -> Result<i64> {
        let fid = fid as u64;
        let n = self.next_empty_slot;
        let idx = partition_point(n, |slot| self.get(slot * SLOT) < fid);
        if idx < n && self.get(idx * SLOT) == fid {
            let off = idx * SLOT + 8;
            let cur = self.get(off);
            if discard == 0 {
                return Ok(cur as i64);
            }
            if discard < 0 {
                self.set(off, 0);
                return Ok(0);
            }
            let total = cur + discard as u64;
            self.set(off, total);
            return Ok(total as i64);
        }
        if discard <= 0 {
            return Ok(0);
        }
        // Insert at `idx`, keeping the slots sorted (Go appends and sorts).
        while n + 1 >= self.max_slot() {
            self.grow()?;
        }
        let data = self.file.data_mut();
        data.copy_within(idx * SLOT..n * SLOT, (idx + 1) * SLOT);
        let off = idx * SLOT;
        self.set(off, fid);
        self.set(off + 8, discard as u64);
        self.next_empty_slot += 1;
        self.zero_out();
        Ok(discard)
    }

    /// Calls `f(fid, discard)` for every entry, by ascending fid.
    pub fn iterate(&self, mut f: impl FnMut(u64, u64)) {
        for slot in 0..self.next_empty_slot {
            f(self.get(slot * SLOT), self.get(slot * SLOT + 8));
        }
    }

    /// The file with the most discardable bytes, or `(0, 0)` if none (Go's
    /// `MaxDiscard`).
    pub fn max_discard(&self) -> (u32, i64) {
        let (mut max_fid, mut max_val) = (0, 0);
        self.iterate(|fid, val| {
            if max_val < val {
                max_val = val;
                max_fid = fid;
            }
        });
        (max_fid as u32, max_val as i64)
    }

    pub fn sync(&self) -> Result<()> {
        self.file.sync()
    }
}

/// `sort.Search`: the first index in `0..n` for which `pred` is false.
fn partition_point(n: usize, pred: impl Fn(usize) -> bool) -> usize {
    let (mut lo, mut hi) = (0, n);
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        if pred(mid) {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    lo
}

// Miri cannot mmap files.
#[cfg(all(test, not(miri)))]
mod tests {
    // Ports of Go's discard_test.go.
    use super::*;

    #[test]
    fn discard_stats() {
        let dir = tempfile::tempdir().unwrap();
        let mut ds = DiscardStats::open(dir.path()).unwrap();
        assert_eq!(ds.next_empty_slot, 0);
        assert_eq!(ds.max_discard(), (0, 0));

        for i in 0..20u32 {
            assert_eq!(ds.update(i, i as i64 * 100).unwrap(), i as i64 * 100);
        }
        // Zero discard is not stored.
        assert_eq!(ds.next_empty_slot, 19);
        let mut seen = Vec::new();
        ds.iterate(|fid, val| seen.push((fid, val)));
        assert_eq!(seen.len(), 19);
        assert!(seen.iter().all(|&(fid, val)| val == fid * 100));
        for i in 0..10u32 {
            assert_eq!(ds.update(i, -1).unwrap(), 0);
        }
        assert_eq!(ds.max_discard(), (19, 1900));
        assert_eq!(ds.update(19, 0).unwrap(), 1900);
        assert_eq!(ds.update(19, 5).unwrap(), 1905);
    }

    #[test]
    fn reload_discard_stats() {
        let dir = tempfile::tempdir().unwrap();
        let mut ds = DiscardStats::open(dir.path()).unwrap();
        // Out of order, so the slots must be sorted.
        for fid in [7u32, 3, 9, 1] {
            ds.update(fid, fid as i64 * 10).unwrap();
        }
        ds.sync().unwrap();
        drop(ds);

        let ds = DiscardStats::open(dir.path()).unwrap();
        assert_eq!(ds.next_empty_slot, 4);
        let mut seen = Vec::new();
        ds.iterate(|fid, val| seen.push((fid, val)));
        assert_eq!(seen, vec![(1, 10), (3, 30), (7, 70), (9, 90)]);
        assert_eq!(ds.max_discard(), (9, 90));
    }

    #[test]
    fn grows_when_full() {
        let dir = tempfile::tempdir().unwrap();
        let mut ds = DiscardStats::open(dir.path()).unwrap();
        let slots = ds.max_slot();
        for fid in 1..=slots as u32 {
            ds.update(fid, 1).unwrap();
        }
        assert!(ds.max_slot() >= 2 * slots);
        assert_eq!(ds.next_empty_slot, slots);
        assert_eq!(ds.update(slots as u32, 0).unwrap(), 1);
    }
}
