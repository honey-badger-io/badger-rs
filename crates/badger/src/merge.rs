//! The merge operator. Port of Go's `merge.go`.
//!
//! A [`MergeOperator`] keeps one key's value as the fold of every value added
//! to it: [`MergeOperator::add`] writes each operand as a new version of the
//! key (marked with the merge bit, so compaction keeps it), and
//! [`MergeOperator::get`] merges the versions down to the newest delete. A
//! background thread periodically writes the merged value back at the
//! newest version, marked "discard earlier versions", so compaction can
//! drop the operands it covers.
//!
//! Differences from Go:
//! - The operator borrows the [`DB`]; dropping it stops the background
//!   thread (Go's `Stop`), after one last merge, as in Go.
//! - The merged value is written synchronously on the background thread
//!   (Go's `batchSetAsync` does not wait), so after `stop` returns it is in
//!   the DB.
//! - It needs a non-managed DB: Go's background goroutine would panic on
//!   the first merge in managed mode, here `get_merge_operator` panics.

use std::sync::{Arc, RwLock};
use std::thread::JoinHandle;
use std::time::Duration;

use crossbeam_channel::{Sender, select};

use crate::db::{DB, DbInner};
use crate::error::{Error, Result};
use crate::iterator::IteratorOptions;
use crate::structs::{BIT_DISCARD_EARLIER_VERSIONS, Entry};
use crate::txn::Txn;
use crate::util::keys::key_with_ts;

/// Merges an existing (older) value with a newer one and returns the result
/// (Go's `MergeFunc`). The order of the operands is kept: `f(older, newer)`.
pub type MergeFunc = dyn Fn(&[u8], &[u8]) -> Vec<u8> + Send + Sync;

/// What the merge thread and the operator share.
struct Shared {
    db: Arc<DbInner>,
    key: Vec<u8>,
    f: Box<MergeFunc>,
    /// Write-locked while merging and writing back, read-locked by `get`.
    lock: RwLock<()>,
}

/// A merge operator on one key; see the [module docs](self).
pub struct MergeOperator<'db> {
    shared: Arc<Shared>,
    /// Dropped to stop the thread.
    stop: Option<Sender<()>>,
    thread: Option<JoinHandle<()>>,
    _db: std::marker::PhantomData<&'db DB>,
}

impl DB {
    /// A merge operator for `key` merging with `f`, which merges in the
    /// background every `dur` (Go's `GetMergeOperator`).
    ///
    /// # Panics
    /// In managed mode, or if the thread cannot be started.
    pub fn get_merge_operator(
        &self,
        key: impl Into<Vec<u8>>,
        f: impl Fn(&[u8], &[u8]) -> Vec<u8> + Send + Sync + 'static,
        dur: Duration,
    ) -> MergeOperator<'_> {
        assert!(
            !self.inner.opt.managed_txns,
            "the merge operator needs a non-managed DB"
        );
        let shared = Arc::new(Shared {
            db: Arc::clone(&self.inner),
            key: key.into(),
            f: Box::new(f),
            lock: RwLock::new(()),
        });
        let (stop, stopped) = crossbeam_channel::bounded::<()>(0);
        let thread_shared = Arc::clone(&shared);
        let thread = std::thread::Builder::new()
            .name("badger-merge".into())
            .spawn(move || {
                let ticker = crossbeam_channel::tick(dur);
                loop {
                    let stop = select! {
                        recv(stopped) -> _ => true,
                        recv(ticker) -> _ => false,
                    };
                    if let Err(e) = thread_shared.compact() {
                        log::error!("failure while running merge operation: {e}");
                    }
                    if stop {
                        break;
                    }
                }
            })
            .expect("cannot start the merge thread");
        MergeOperator {
            shared,
            stop: Some(stop),
            thread: Some(thread),
            _db: std::marker::PhantomData,
        }
    }
}

impl MergeOperator<'_> {
    /// Adds `val`, to be merged into the earlier values (Go's `Add`).
    pub fn add(&self, val: impl Into<Vec<u8>>) -> Result<()> {
        let db = &self.shared.db;
        if db.is_closed() {
            return Err(Error::DbClosed);
        }
        let mut txn = Txn::new(db, true, None);
        txn.set_entry(Entry::new(self.shared.key.clone(), val).with_merge_bit())?;
        txn.commit()
    }

    /// The merge of every value added since the key was last deleted (Go's
    /// `Get`). Fails with [`Error::KeyNotFound`] if there is none.
    pub fn get(&self) -> Result<Vec<u8>> {
        let _r = self.shared.lock.read().unwrap();
        if self.shared.db.is_closed() {
            return Err(Error::DbClosed);
        }
        match self.shared.iterate_and_merge()? {
            Some(m) => Ok(m.value),
            None => Err(Error::KeyNotFound),
        }
    }

    /// Stops the background thread after one last merge (Go's `Stop`).
    /// Dropping the operator does the same.
    pub fn stop(self) {}
}

impl Drop for MergeOperator<'_> {
    fn drop(&mut self) {
        drop(self.stop.take());
        if let Some(t) = self.thread.take() {
            let _ = t.join();
        }
    }
}

/// The result of merging a key's versions.
struct Merged {
    value: Vec<u8>,
    /// The newest version.
    version: u64,
    /// How many versions were merged.
    versions: usize,
}

impl Shared {
    /// Merges the key's versions, newest first, down to a delete or an entry
    /// that discards earlier versions (Go's `iterateAndMerge`). `None` if
    /// there is no live version.
    fn iterate_and_merge(&self) -> Result<Option<Merged>> {
        let txn = Txn::new(&self.db, false, None);
        let mut it = txn.key_iter(&self.key, IteratorOptions::default());
        it.rewind();
        let mut merged: Option<Merged> = None;
        while it.valid() {
            let item = it.item();
            if item.is_deleted_or_expired() {
                break;
            }
            match &mut merged {
                None => {
                    merged = Some(Merged {
                        value: item.value_copy()?,
                        version: item.version(),
                        versions: 1,
                    })
                }
                Some(m) => {
                    // The older value comes first.
                    m.value = item.value(|old| (self.f)(old, &m.value))?;
                    m.versions += 1;
                }
            }
            if item.discard_earlier_versions() {
                break;
            }
            it.next();
        }
        Ok(merged)
    }

    /// Writes the merged value back at the newest version, so compaction
    /// can drop the older ones (Go's `compact`).
    fn compact(&self) -> Result<()> {
        let _w = self.lock.write().unwrap();
        if self.db.is_closed() {
            return Ok(());
        }
        let Some(m) = self.iterate_and_merge()? else {
            return Ok(());
        };
        if m.versions == 1 {
            // Nothing to merge.
            return Ok(());
        }
        // Without the merge bit: compaction removes the older operands.
        let entry = Entry {
            key: key_with_ts(&self.key, m.version),
            value: m.value,
            meta: BIT_DISCARD_EARLIER_VERSIONS,
            ..Default::default()
        };
        self.db.batch_set(vec![entry])
    }
}

#[cfg(all(test, not(miri)))]
mod tests;
