//! The oracle hands out transaction timestamps and detects conflicts. Port of
//! Go's `oracle` (in `txn.go`).
//!
//! - `next_txn_ts` is the next commit timestamp; a read transaction reads at
//!   `next_txn_ts - 1`.
//! - `read_mark` tracks timestamps of running reads, so compaction can tell
//!   which versions nobody can see any more.
//! - `txn_mark` tracks commits; a new read waits until every commit at or
//!   below its read timestamp is applied, so it sees them.
//! - Conflict detection (optimistic concurrency): each update transaction
//!   records fingerprints of the keys it read. At commit, if a transaction
//!   that committed after our read timestamp wrote any of them, we abort with
//!   [`Error::Conflict`](crate::Error::Conflict). Committed transactions are
//!   remembered until no running read can predate them.

use std::collections::HashSet;
use std::sync::Mutex;

use crate::util::WaterMark;

pub(crate) struct Oracle {
    is_managed: bool,
    detect_conflicts: bool,
    state: Mutex<State>,
    /// Held from picking a commit timestamp until the entries are queued, so
    /// writes reach the write channel in commit timestamp order.
    pub write_ch_lock: Mutex<()>,
    pub read_mark: WaterMark,
    pub txn_mark: WaterMark,
}

#[derive(Default)]
struct State {
    next_txn_ts: u64,
    /// Managed mode: versions at or below this may be discarded.
    discard_ts: u64,
    /// Transactions committed after some running read began.
    committed_txns: Vec<CommittedTxn>,
    last_cleanup_ts: u64,
}

struct CommittedTxn {
    ts: u64,
    /// Fingerprints of the keys it wrote.
    conflict_keys: HashSet<u64>,
}

/// What a committing transaction tells the oracle.
pub(crate) struct CommitRequest<'a> {
    pub read_ts: u64,
    /// The timestamp the transaction registered with the read mark.
    pub read_mark_ts: u64,
    /// Fingerprints of keys read.
    pub reads: &'a [u64],
    /// Fingerprints of keys written.
    pub conflict_keys: &'a HashSet<u64>,
    /// Managed mode: the timestamp chosen by the application.
    pub managed_commit_ts: u64,
}

impl Oracle {
    /// An oracle for a DB whose newest version is `max_version` (the end of
    /// Go's `Open`: marks are done up to it, the next commit gets one more).
    pub fn new(is_managed: bool, detect_conflicts: bool, max_version: u64) -> Oracle {
        let orc = Oracle {
            is_managed,
            detect_conflicts,
            state: Mutex::new(State {
                next_txn_ts: max_version + 1,
                ..Default::default()
            }),
            write_ch_lock: Mutex::new(()),
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
            let state = self.state.lock().unwrap();
            let ts = state.next_txn_ts - 1;
            self.read_mark.begin(ts);
            ts
        };
        // Wait for commits up to read_ts to be applied, so they are visible.
        self.txn_mark.wait_for_mark(read_ts);
        read_ts
    }

    /// The next commit timestamp.
    #[allow(dead_code)] // used by Stream and backups (Phase 7)
    pub fn next_ts(&self) -> u64 {
        self.state.lock().unwrap().next_txn_ts
    }

    /// Managed mode: allows discarding versions at or below `ts`.
    pub fn set_discard_ts(&self, ts: u64) {
        let mut state = self.state.lock().unwrap();
        state.discard_ts = ts;
        self.cleanup_committed_transactions(&mut state);
    }

    /// Versions at or below this timestamp are invisible to every read.
    #[allow(dead_code)] // used by compaction (Phase 6)
    pub fn discard_at_or_below(&self) -> u64 {
        if self.is_managed {
            return self.state.lock().unwrap().discard_ts;
        }
        self.read_mark.done_until()
    }

    /// Picks the commit timestamp for a transaction, or returns `None` on a
    /// conflict (Go's `newCommitTs`). In non-managed mode this also ends the
    /// transaction's read (`done_read` is set) and begins its commit mark.
    pub fn new_commit_ts(&self, req: &CommitRequest<'_>, done_read: &mut bool) -> Option<u64> {
        let mut state = self.state.lock().unwrap();
        if self.has_conflict(&state, req) {
            return None;
        }
        let ts = if !self.is_managed {
            if !*done_read {
                *done_read = true;
                self.read_mark.done(req.read_mark_ts);
            }
            self.cleanup_committed_transactions(&mut state);
            let ts = state.next_txn_ts;
            state.next_txn_ts += 1;
            self.txn_mark.begin(ts);
            ts
        } else {
            req.managed_commit_ts
        };
        assert!(ts >= state.last_cleanup_ts);
        if self.detect_conflicts {
            state.committed_txns.push(CommittedTxn {
                ts,
                conflict_keys: req.conflict_keys.clone(),
            });
        }
        Some(ts)
    }

    fn has_conflict(&self, state: &State, req: &CommitRequest<'_>) -> bool {
        if req.reads.is_empty() {
            return false;
        }
        state
            .committed_txns
            .iter()
            // Transactions committed before our read are visible to it.
            .filter(|c| c.ts > req.read_ts)
            .any(|c| req.reads.iter().any(|r| c.conflict_keys.contains(r)))
    }

    /// Forgets committed transactions that no running read can conflict with.
    fn cleanup_committed_transactions(&self, state: &mut State) {
        if !self.detect_conflicts {
            return;
        }
        let max_read_ts = if self.is_managed {
            state.discard_ts
        } else {
            self.read_mark.done_until()
        };
        assert!(max_read_ts >= state.last_cleanup_ts);
        if max_read_ts == state.last_cleanup_ts {
            return;
        }
        state.last_cleanup_ts = max_read_ts;
        state.committed_txns.retain(|c| c.ts > max_read_ts);
    }

    /// Ends a read started by [`Oracle::read_ts`].
    pub fn done_read(&self, read_ts: u64) {
        self.read_mark.done(read_ts);
    }

    /// Marks commit `ts` as applied, making it visible to new reads.
    pub fn done_commit(&self, ts: u64) {
        if !self.is_managed {
            self.txn_mark.done(ts);
        }
    }
}

/// Fingerprint of a key for conflict detection (Go uses `z.MemHash`; any
/// in-memory hash works, it is never stored).
pub(crate) fn fingerprint(key: &[u8]) -> u64 {
    xxhash_rust::xxh64::xxh64(key, 0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn read_ts_follows_max_version() {
        for max in [0, 1, 42] {
            let orc = Oracle::new(false, true, max);
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
        Oracle::new(true, true, 5).read_ts();
    }

    #[test]
    fn conflicts_and_cleanup() {
        let orc = Oracle::new(false, true, 0);
        let k = [fingerprint(b"k")].into_iter().collect::<HashSet<_>>();
        let none = HashSet::new();

        // Two transactions read at 0; the first writes k.
        let (r1, r2) = (orc.read_ts(), orc.read_ts());
        let (mut d1, mut d2) = (false, false);
        let c1 = orc
            .new_commit_ts(
                &CommitRequest {
                    read_ts: r1,
                    read_mark_ts: r1,
                    reads: &[],
                    conflict_keys: &k,
                    managed_commit_ts: 0,
                },
                &mut d1,
            )
            .unwrap();
        assert_eq!(c1, 1);
        assert!(d1);
        orc.done_commit(c1);
        // The second read k before c1 committed: conflict.
        let reads = [fingerprint(b"k")];
        let req2 = CommitRequest {
            read_ts: r2,
            read_mark_ts: r2,
            reads: &reads,
            conflict_keys: &none,
            managed_commit_ts: 0,
        };
        assert_eq!(orc.new_commit_ts(&req2, &mut d2), None);
        orc.done_read(r2);

        // A read after c1 does not conflict, and c1 is cleaned up once no
        // read predates it.
        let r3 = orc.read_ts();
        assert_eq!(r3, 1);
        let mut d3 = false;
        let req3 = CommitRequest {
            read_ts: r3,
            read_mark_ts: r3,
            reads: &reads,
            conflict_keys: &k,
            managed_commit_ts: 0,
        };
        assert_eq!(orc.new_commit_ts(&req3, &mut d3), Some(2));
        orc.done_commit(2);
        assert!(
            orc.state
                .lock()
                .unwrap()
                .committed_txns
                .iter()
                .all(|c| c.ts > 1)
        );
    }
}
