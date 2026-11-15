//! [`WaterMark`] tracks the highest index below which every index is done.
//! The oracle uses it for read timestamps and commit timestamps. Port of Go's
//! `y/watermark.go`.
//!
//! An index `k` is done once `done(k)` has been called as often as `begin(k)`
//! (and at least once). `done_until()` is the largest index such that it and
//! every smaller index that was begun are done.
//!
//! Design difference from Go: Go sends begin/done marks over a channel to a
//! background goroutine, so `DoneUntil` lags slightly behind. Here marks are
//! applied immediately under a mutex and waiters block on a condvar. There is
//! no thread to start or stop, and the observable ordering is the same.

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Condvar, Mutex, MutexGuard};
use std::time::{Duration, Instant};

#[derive(Debug)]
pub struct WaterMark {
    name: String,
    done_until: AtomicU64,
    last_index: AtomicU64,
    state: Mutex<State>,
    advanced: Condvar,
}

#[derive(Debug, Default)]
struct State {
    /// Indices with pending begin/done marks, smallest first.
    indices: BinaryHeap<Reverse<u64>>,
    /// Number of begins minus dones per index.
    pending: HashMap<u64, i64>,
}

impl WaterMark {
    pub fn new(name: impl Into<String>) -> Self {
        WaterMark {
            name: name.into(),
            done_until: AtomicU64::new(0),
            last_index: AtomicU64::new(0),
            state: Mutex::new(State::default()),
            advanced: Condvar::new(),
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    /// Marks `index` as started.
    pub fn begin(&self, index: u64) {
        self.last_index.store(index, Ordering::SeqCst);
        self.apply(&[index], false);
    }

    /// Marks all `indices` as started. The last one becomes [`WaterMark::last_index`].
    pub fn begin_many(&self, indices: &[u64]) {
        if let Some(&last) = indices.last() {
            self.last_index.store(last, Ordering::SeqCst);
            self.apply(indices, false);
        }
    }

    /// Marks `index` as finished.
    pub fn done(&self, index: u64) {
        self.apply(&[index], true);
    }

    pub fn done_many(&self, indices: &[u64]) {
        self.apply(indices, true);
    }

    /// The largest index such that it and all smaller begun indices are done.
    pub fn done_until(&self) -> u64 {
        self.done_until.load(Ordering::SeqCst)
    }

    /// Sets the done-until index directly. Must not be mixed with concurrent
    /// begin/done calls (same rule as Go).
    pub fn set_done_until(&self, val: u64) {
        let _state = self.lock();
        self.done_until.store(val, Ordering::SeqCst);
        self.advanced.notify_all();
    }

    /// The last index passed to `begin` / `begin_many`.
    pub fn last_index(&self) -> u64 {
        self.last_index.load(Ordering::SeqCst)
    }

    /// Blocks until `done_until() >= index`.
    pub fn wait_for_mark(&self, index: u64) {
        let mut state = self.lock();
        while self.done_until() < index {
            state = self.advanced.wait(state).unwrap_or_else(|e| e.into_inner());
        }
    }

    /// Like [`WaterMark::wait_for_mark`] with a timeout (Go uses a context).
    /// Returns whether the mark was reached.
    pub fn wait_for_mark_timeout(&self, index: u64, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        let mut state = self.lock();
        while self.done_until() < index {
            let now = Instant::now();
            if now >= deadline {
                return false;
            }
            state = self
                .advanced
                .wait_timeout(state, deadline - now)
                .unwrap_or_else(|e| e.into_inner())
                .0;
        }
        true
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn apply(&self, indices: &[u64], done: bool) {
        let mut state = self.lock();
        let before = self.done_until();
        for &index in indices {
            self.process_one(&mut state, index, done);
        }
        if self.done_until() != before {
            self.advanced.notify_all();
        }
    }

    /// Go's `processOne`: record the mark, then advance done_until past every
    /// leading index whose pending count has dropped to zero (or below).
    fn process_one(&self, state: &mut State, index: u64, done: bool) {
        let count = state.pending.entry(index).or_insert_with(|| {
            state.indices.push(Reverse(index));
            0
        });
        *count += if done { -1 } else { 1 };

        let done_until = self.done_until();
        assert!(
            done_until <= index,
            "WaterMark {}: doneUntil {done_until} > index {index}",
            self.name
        );

        let mut until = done_until;
        while let Some(&Reverse(min)) = state.indices.peek() {
            if state.pending.get(&min).copied().unwrap_or(0) > 0 {
                break;
            }
            // Even if done was called too often (count < 0), pop the index.
            state.indices.pop();
            state.pending.remove(&min);
            until = min;
        }
        if until != done_until {
            self.done_until.store(until, Ordering::SeqCst);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn done_until_advances_in_order() {
        let w = WaterMark::new("test");
        w.begin(1);
        w.begin(2);
        w.begin(3);
        assert_eq!(w.last_index(), 3);
        w.done(2);
        assert_eq!(w.done_until(), 0, "1 is still pending");
        w.done(1);
        assert_eq!(w.done_until(), 2);
        w.done(3);
        assert_eq!(w.done_until(), 3);
    }

    #[test]
    fn index_needs_as_many_dones_as_begins() {
        let w = WaterMark::new("test");
        w.begin_many(&[5, 5, 6]);
        w.done(5);
        assert_eq!(w.done_until(), 0);
        w.done_many(&[5, 6]);
        assert_eq!(w.done_until(), 6);
    }

    #[test]
    fn waiters_wake_up() {
        let w = Arc::new(WaterMark::new("test"));
        w.begin(1);
        w.begin(2);
        let waiter = {
            let w = Arc::clone(&w);
            thread::spawn(move || w.wait_for_mark(2))
        };
        assert!(!w.wait_for_mark_timeout(2, Duration::from_millis(20)));
        w.done(1);
        w.done(2);
        waiter.join().unwrap();
        assert!(w.wait_for_mark_timeout(2, Duration::ZERO));
    }

    #[test]
    #[cfg_attr(miri, ignore)] // too slow under Miri; no unsafe code involved
    fn concurrent_begin_done() {
        // Like the oracle: indices are allocated and begun in order under a
        // lock, then finished concurrently in any order. (Beginning an index
        // below done_until is a bug, and panics in Go too.)
        let w = Arc::new(WaterMark::new("test"));
        let next = Arc::new(std::sync::Mutex::new(0u64));
        let threads: Vec<_> = (0..8)
            .map(|_| {
                let (w, next) = (Arc::clone(&w), Arc::clone(&next));
                thread::spawn(move || {
                    for _ in 0..1000 {
                        let idx = {
                            let mut n = next.lock().unwrap();
                            *n += 1;
                            w.begin(*n);
                            *n
                        };
                        w.done(idx);
                    }
                })
            })
            .collect();
        for t in threads {
            t.join().unwrap();
        }
        assert_eq!(w.done_until(), 8000);
    }
}
