//! The oracle hands out transaction timestamps. Port of the read side of Go's
//! `oracle` (in `txn.go`); commit timestamps and conflict detection come in
//! Phase 5.
//!
//! - `next_txn_ts` is the next commit timestamp; a read transaction reads at
//!   `next_txn_ts - 1`.
//! - `read_mark` tracks timestamps of running reads, so compaction can tell
//!   which versions nobody can see any more.
//! - `txn_mark` tracks commits; a new read waits until every commit at or
//!   below its read timestamp is applied.

use std::sync::Mutex;

use crate::util::WaterMark;

pub(crate) struct Oracle {
    is_managed: bool,
    next_txn_ts: Mutex<u64>,
    pub read_mark: WaterMark,
    pub txn_mark: WaterMark,
}

impl Oracle {
    /// An oracle for a DB whose newest version is `max_version` (the end of
    /// Go's `Open`: marks are done up to it, the next commit gets one more).
    pub fn new(is_managed: bool, max_version: u64) -> Oracle {
        let orc = Oracle {
            is_managed,
            next_txn_ts: Mutex::new(max_version + 1),
            read_mark: WaterMark::new("badger.PendingReads"),
            txn_mark: WaterMark::new("badger.TxnTimestamp"),
        };
        orc.txn_mark.done(max_version);
        orc.read_mark.done(max_version);
        orc
    }

    pub fn is_managed(&self) -> bool {
        self.is_managed
    }

    /// The timestamp for a new read transaction.
    ///
    /// # Panics
    /// In managed mode, where the application picks read timestamps.
    pub fn read_ts(&self) -> u64 {
        assert!(
            !self.is_managed,
            "ReadTs should not be retrieved for managed DB"
        );
        let read_ts = {
            let next = self.next_txn_ts.lock().unwrap();
            let ts = *next - 1;
            self.read_mark.begin(ts);
            ts
        };
        // Wait for commits up to read_ts to be applied, so they are visible.
        self.txn_mark.wait_for_mark(read_ts);
        read_ts
    }

    /// Ends a read started by [`Oracle::read_ts`].
    pub fn done_read(&self, read_ts: u64) {
        self.read_mark.done(read_ts);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn read_ts_follows_max_version() {
        for max in [0, 1, 42] {
            let orc = Oracle::new(false, max);
            let ts = orc.read_ts();
            assert_eq!(ts, max);
            // A second concurrent read gets the same timestamp.
            assert_eq!(orc.read_ts(), max);
            orc.done_read(ts);
            orc.done_read(ts);
            assert_eq!(orc.read_mark.done_until(), max);
        }
    }

    #[test]
    #[should_panic(expected = "managed")]
    fn managed_has_no_read_ts() {
        Oracle::new(true, 5).read_ts();
    }
}
