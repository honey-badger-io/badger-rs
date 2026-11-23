//! Batched writes. Port of Go's `batch.go` (and the write batch
//! constructors from `managed_db.go`).
//!
//! A [`WriteBatch`] packs writes into transactions as large as the batch
//! limits allow and commits each full one without waiting for it, keeping
//! up to `max_pending` commits in flight. Blind writes never conflict.
//!
//! Differences from Go:
//! - Methods take `&mut self` instead of locking a mutex; share a batch
//!   between threads behind a `Mutex`.
//! - [`WriteBatch::flush`] consumes the batch and dropping it cancels it, so
//!   "flush after flush/cancel" (`ErrCommitAfterFinish`) cannot happen.
//! - In-flight commits are [`PendingCommit`]s waited on in order instead of
//!   callbacks counted by a `y.Throttle`. Once an error is seen, nothing
//!   more is committed (Go may still commit one more transaction).
//! - [`WriteBatch::flush`] waits for every commit in flight even when its
//!   own commit fails (Go returns at once, leaving them running).

use std::collections::VecDeque;
use std::sync::Arc;

use crate::db::{DB, DbInner};
use crate::error::{Error, Result, ResultExt};
use crate::pb;
use crate::structs::{BIT_DELETE, Entry};
use crate::txn::{ModifyError, PendingCommit, Txn};

/// Go's default for `SetMaxPendingTxns`.
const DEFAULT_MAX_PENDING: usize = 16;

/// Writes many entries efficiently; see the [module docs](self). Call
/// [`WriteBatch::flush`] at the end: dropping the batch discards writes not
/// committed yet (commits already started still go through).
pub struct WriteBatch<'db> {
    db: &'db DbInner,
    /// `None` only while being replaced.
    txn: Option<Txn<'db>>,
    managed: bool,
    /// Managed mode: the commit timestamp of every transaction (0 if each
    /// entry carries its own version).
    commit_ts: u64,
    max_pending: usize,
    pending: VecDeque<PendingCommit>,
    /// The first error; once set, nothing more is committed.
    err: Option<Arc<Error>>,
}

impl DB {
    /// A write batch (Go's `NewWriteBatch`).
    ///
    /// # Panics
    /// In managed mode; use [`DB::new_write_batch_at`] or
    /// [`DB::new_managed_write_batch`].
    pub fn new_write_batch(&self) -> WriteBatch<'_> {
        assert!(
            !self.inner.opt.managed_txns,
            "cannot use NewWriteBatch in managed mode. Use NewWriteBatchAt instead"
        );
        WriteBatch::new(&self.inner, false, 0)
    }

    /// Managed mode: a write batch committing everything at `commit_ts` (Go's
    /// `NewWriteBatchAt`).
    ///
    /// # Panics
    /// If the DB is not managed.
    pub fn new_write_batch_at(&self, commit_ts: u64) -> WriteBatch<'_> {
        assert!(
            self.inner.opt.managed_txns,
            "cannot use NewWriteBatchAt with managedDB=false. Use NewWriteBatch instead"
        );
        WriteBatch::new(&self.inner, true, commit_ts)
    }

    /// Managed mode: a write batch whose entries carry their own versions,
    /// set with [`WriteBatch::set_entry_at`] / [`WriteBatch::delete_at`]
    /// (Go's `NewManagedWriteBatch`).
    ///
    /// # Panics
    /// If the DB is not managed.
    pub fn new_managed_write_batch(&self) -> WriteBatch<'_> {
        assert!(
            self.inner.opt.managed_txns,
            "cannot use NewManagedWriteBatch with managedDB=false. Use NewWriteBatch instead"
        );
        WriteBatch::new(&self.inner, true, 0)
    }
}

impl<'db> WriteBatch<'db> {
    fn new(db: &'db DbInner, managed: bool, commit_ts: u64) -> Self {
        let mut wb = WriteBatch {
            db,
            txn: None,
            managed,
            commit_ts,
            max_pending: DEFAULT_MAX_PENDING,
            pending: VecDeque::new(),
            err: None,
        };
        wb.txn = Some(wb.new_txn());
        wb
    }

    fn new_txn(&self) -> Txn<'db> {
        // Managed transactions start at read timestamp 0, as in Go.
        let mut txn = Txn::new(self.db, true, self.managed.then_some(0));
        txn.set_commit_ts(self.commit_ts);
        txn
    }

    /// Limits the number of commits in flight (default 16; at least 1).
    pub fn set_max_pending_txns(&mut self, max: usize) {
        self.max_pending = max.max(1);
    }

    /// Sets `key` to `value`.
    pub fn set(&mut self, key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) -> Result<()> {
        self.set_entry(Entry::new(key, value))
    }

    /// Writes `e`, like [`Txn::set_entry`].
    pub fn set_entry(&mut self, e: Entry) -> Result<()> {
        self.handle_entry(e)
    }

    /// Managed mode: writes `e` at version `ts` (Go's `SetEntryAt`).
    pub fn set_entry_at(&mut self, mut e: Entry, ts: u64) -> Result<()> {
        if !self.db.opt.managed_txns {
            return Err(Error::InvalidRequest)
                .context("SetEntryAt can only be used in managed mode. Use SetEntry instead");
        }
        e.version = ts;
        self.handle_entry(e)
    }

    /// Deletes `key`.
    pub fn delete(&mut self, key: impl Into<Vec<u8>>) -> Result<()> {
        self.handle_entry(Entry {
            key: key.into(),
            meta: BIT_DELETE,
            ..Default::default()
        })
    }

    /// Managed mode: deletes `key` at version `ts` (Go's `DeleteAt`).
    pub fn delete_at(&mut self, key: impl Into<Vec<u8>>, ts: u64) -> Result<()> {
        self.handle_entry(Entry {
            key: key.into(),
            meta: BIT_DELETE,
            version: ts,
            ..Default::default()
        })
    }

    /// Writes a list of key-value pairs, each at its own version (Go's
    /// `WriteList`, used by streams).
    ///
    /// # Panics
    /// If a pair has version 0.
    pub fn write_list(&mut self, list: &pb::KvList) -> Result<()> {
        for kv in &list.kv {
            self.write_kv(kv)?;
        }
        Ok(())
    }

    fn write_kv(&mut self, kv: &pb::Kv) -> Result<()> {
        assert_ne!(kv.version, 0, "a KV written to a batch needs a version");
        self.handle_entry(Entry {
            key: kv.key.clone(),
            value: kv.value.clone(),
            user_meta: kv.user_meta.first().copied().unwrap_or(0),
            version: kv.version,
            ..Default::default()
        })
    }

    /// Adds `e` to the current transaction, committing it first if full.
    fn handle_entry(&mut self, e: Entry) -> Result<()> {
        let txn = self.txn.as_mut().expect("write batch has a transaction");
        let e = match txn.try_modify(e) {
            Ok(()) => return Ok(()),
            Err(ModifyError::TooBig(e)) => *e,
            Err(ModifyError::Other(err)) => return Err(err),
        };
        // The transaction is full: commit it and retry in a fresh one. A
        // second failure is permanent.
        self.commit()?;
        let txn = self.txn.as_mut().expect("write batch has a transaction");
        if let Err(err) = txn.try_modify(e) {
            let err = Arc::new(match err {
                ModifyError::TooBig(_) => Error::TxnTooBig,
                ModifyError::Other(e) => e,
            });
            self.err = Some(Arc::clone(&err));
            return Err(Error::Shared(err));
        }
        Ok(())
    }

    /// The first error so far (Go's `Error`). Once there is one, nothing
    /// more is committed.
    pub fn error(&self) -> Option<Error> {
        self.err.as_ref().map(|e| Error::Shared(Arc::clone(e)))
    }

    fn check_err(&self) -> Result<()> {
        self.error().map_or(Ok(()), Err)
    }

    fn record(&mut self, res: Result<()>) {
        if let Err(e) = res
            && self.err.is_none()
        {
            self.err = Some(Arc::new(e));
        }
    }

    /// Starts committing the current transaction and replaces it, after
    /// waiting for room among the commits in flight.
    fn commit(&mut self) -> Result<()> {
        self.check_err()?;
        while self.pending.len() >= self.max_pending {
            let res = self.pending.pop_front().unwrap().wait();
            self.record(res);
        }
        // An earlier commit may have failed meanwhile.
        self.check_err()?;
        let txn = self.txn.take().expect("write batch has a transaction");
        let started = txn.start_commit();
        self.txn = Some(self.new_txn());
        match started {
            Ok(p) => self.pending.push_back(p),
            Err(e) => self.record(Err(e)),
        }
        self.check_err()
    }

    /// Waits for every commit in flight.
    fn wait_all(&mut self) {
        while let Some(p) = self.pending.pop_front() {
            let res = p.wait();
            self.record(res);
        }
    }

    /// Commits the remaining writes and waits for every commit (Go's
    /// `Flush`). Returns the first error of the batch, if any.
    pub fn flush(mut self) -> Result<()> {
        let res = self.commit();
        // Only the commit's own transaction is left; drop it.
        self.txn = None;
        self.wait_all();
        res?;
        self.check_err()
    }
}

impl Drop for WriteBatch<'_> {
    /// Cancels the batch (Go's `Cancel`): writes not committed yet are
    /// discarded, and commits in flight are waited for.
    fn drop(&mut self) {
        self.txn = None;
        self.wait_all();
    }
}

#[cfg(all(test, not(miri)))]
mod tests;
