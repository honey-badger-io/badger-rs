//! Transactions. Port of Go's `txn.go` (the oracle is in `oracle.rs`).
//!
//! A transaction reads a snapshot of the DB at its read timestamp. An update
//! transaction buffers its writes; [`Txn::commit`] checks for conflicts, takes
//! a commit timestamp and hands the writes to the DB's writer thread as one
//! batch, which is atomic: replay after a crash keeps all of it or none.
//!
//! Differences from Go: dropping a transaction discards it; `commit` consumes
//! it. Writing needs `&mut Txn`, so a transaction cannot be written to while
//! one of its iterators is alive (Go allows it; the iterator would not see
//! the new writes anyway).

use std::collections::{HashMap, HashSet};
use std::sync::Mutex;

use crate::db::DbInner;
use crate::error::{Error, Result, ResultExt};
use crate::iterator::{BADGER_PREFIX, Item, Iterator, IteratorOptions, is_deleted_or_expired};
use crate::oracle::{CommitRequest, fingerprint};
use crate::structs::{BIT_DELETE, BIT_FIN_TXN, BIT_TXN, Entry};
use crate::util::KvIterator;
use crate::util::keys::{key_with_ts, parse_key};
use crate::util::value::ValueStruct;

/// Key of the entry that marks the end of a transaction in the logs.
pub(crate) const TXN_KEY: &[u8] = b"!badger!txn";

/// Largest key a transaction accepts.
const MAX_KEY_SIZE: usize = 65000;

/// A snapshot of the DB, plus buffered writes for update transactions.
/// Dropping it discards it (Go's `Discard`).
pub struct Txn<'db> {
    db: &'db DbInner,
    read_ts: u64,
    /// The timestamp registered with the oracle's read mark (only differs
    /// from `read_ts` in tests that move the read timestamp).
    read_mark_ts: u64,
    /// Managed mode: the commit timestamp chosen by the application.
    commit_ts: u64,
    /// Estimated size and count of the writes, against the batch limits.
    size: i64,
    count: i64,
    update: bool,
    /// Fingerprints of keys read, for conflict detection.
    reads: Mutex<Vec<u64>>,
    /// Fingerprints of keys written.
    conflict_keys: HashSet<u64>,
    pending_writes: HashMap<Vec<u8>, Entry>,
    /// Managed mode: earlier writes of a key set at a different version.
    duplicate_writes: Vec<Entry>,
    /// Whether the read mark was released (at commit, or on drop).
    done_read: bool,
}

impl<'db> Txn<'db> {
    /// Starts a transaction. `read_ts` is `None` to take one from the oracle
    /// (non-managed mode).
    pub(crate) fn new(db: &'db DbInner, update: bool, read_ts: Option<u64>) -> Self {
        // A read-only DB only has read-only transactions.
        let update = update && !db.opt.read_only;
        let read_ts = read_ts.unwrap_or_else(|| db.orc.read_ts());
        Txn {
            db,
            read_ts,
            read_mark_ts: read_ts,
            commit_ts: 0,
            // Room for the transaction's finish marker.
            count: 1,
            size: (TXN_KEY.len() + 10) as i64,
            update,
            reads: Mutex::new(Vec::new()),
            conflict_keys: HashSet::new(),
            pending_writes: HashMap::new(),
            duplicate_writes: Vec::new(),
            done_read: false,
        }
    }

    /// Managed mode: the commit timestamp to use (Go assigns `txn.commitTs`).
    pub(crate) fn set_commit_ts(&mut self, ts: u64) {
        self.commit_ts = ts;
    }

    /// Reads at another timestamp (Go's tests assign `txn.readTs`).
    #[cfg(test)]
    pub(crate) fn set_read_ts(&mut self, ts: u64) {
        self.read_ts = ts;
    }

    pub(crate) fn db(&self) -> &'db DbInner {
        self.db
    }

    /// The timestamp this transaction reads at.
    pub fn read_ts(&self) -> u64 {
        self.read_ts
    }

    /// Whether this transaction may write.
    pub fn is_update(&self) -> bool {
        self.update
    }

    /// Records a key read, for conflict detection (update transactions only).
    pub(crate) fn add_read_key(&self, key: &[u8]) {
        if self.update {
            self.reads.lock().unwrap().push(fingerprint(key));
        }
    }

    /// Looks up `key`. Deleted and expired keys give [`Error::KeyNotFound`].
    /// An update transaction sees its own pending writes.
    pub fn get(&self, key: &[u8]) -> Result<Item<'db>> {
        if key.is_empty() {
            return Err(Error::EmptyKey);
        }
        self.db.is_banned(key)?;
        if self.update {
            if let Some(e) = self.pending_writes.get(key) {
                if is_deleted_or_expired(e.meta, e.expires_at) {
                    return Err(Error::KeyNotFound);
                }
                return Ok(Item {
                    key: key.to_vec(),
                    vptr: e.value.clone(),
                    version: self.read_ts,
                    expires_at: e.expires_at,
                    meta: e.meta,
                    user_meta: e.user_meta,
                    db: self.db,
                });
            }
            self.add_read_key(key);
        }
        let seek = key_with_ts(key, self.read_ts);
        let vs = self
            .db
            .get(&seek)
            .with_context(|| format!("DB::Get key: {:?}", String::from_utf8_lossy(key)))?;
        let Some(vs) = vs else {
            return Err(Error::KeyNotFound);
        };
        if is_deleted_or_expired(vs.meta, vs.expires_at) {
            return Err(Error::KeyNotFound);
        }
        Ok(Item {
            key: key.to_vec(),
            vptr: vs.value,
            version: vs.version,
            expires_at: vs.expires_at,
            meta: vs.meta,
            user_meta: vs.user_meta,
            db: self.db,
        })
    }

    /// An iterator over this transaction's snapshot (including its pending
    /// writes).
    pub fn iter(&self, opt: IteratorOptions) -> Iterator<'_> {
        Iterator::new(self, opt, false)
    }

    /// An iterator over all versions of one key (Go's `NewKeyIterator`).
    ///
    /// # Panics
    /// If `opt.prefix` is set.
    pub fn key_iter(&self, key: &[u8], mut opt: IteratorOptions) -> Iterator<'_> {
        assert!(
            opt.prefix.is_empty(),
            "opt.prefix should be empty for key_iter"
        );
        opt.prefix = key.to_vec();
        opt.all_versions = true;
        Iterator::new(self, opt, true)
    }

    /// An iterator over the pending writes, as of the read timestamp.
    pub(crate) fn pending_writes_iter(&self, reversed: bool) -> Option<PendingWritesIterator> {
        if !self.update || self.pending_writes.is_empty() {
            return None;
        }
        let mut entries: Vec<Entry> = self.pending_writes.values().cloned().collect();
        entries.sort_by(|a, b| a.key.cmp(&b.key));
        if reversed {
            entries.reverse();
        }
        Some(PendingWritesIterator {
            entries,
            next_idx: 0,
            read_ts: self.read_ts,
            reversed,
            key_buf: Vec::new(),
        })
    }

    /// Sets `key` to `value`.
    pub fn set(&mut self, key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) -> Result<()> {
        self.set_entry(Entry::new(key, value))
    }

    /// Writes `e` (with its TTL and meta, see [`Entry`]).
    pub fn set_entry(&mut self, e: Entry) -> Result<()> {
        self.modify(e)
    }

    /// Deletes `key`.
    pub fn delete(&mut self, key: impl Into<Vec<u8>>) -> Result<()> {
        self.modify(Entry {
            key: key.into(),
            meta: BIT_DELETE,
            ..Default::default()
        })
    }

    fn modify(&mut self, e: Entry) -> Result<()> {
        self.try_modify(e).map_err(|err| match err {
            ModifyError::TooBig(_) => Error::TxnTooBig,
            ModifyError::Other(e) => e,
        })
    }

    /// Like `modify`, but hands the entry back when the transaction is full
    /// ([`Error::TxnTooBig`]), so a write batch can retry it in a new one.
    pub(crate) fn try_modify(&mut self, mut e: Entry) -> std::result::Result<(), ModifyError> {
        self.check_entry(&e).map_err(ModifyError::Other)?;
        // The batch must fit in one write request.
        let count = self.count + 1;
        let size = self.size + e.estimate_size_and_set_threshold(self.db.value_threshold()) + 10;
        let opt = &self.db.opt;
        if count >= opt.max_batch_count() as i64 || size >= opt.max_batch_size() as i64 {
            return Err(ModifyError::TooBig(Box::new(e)));
        }
        (self.count, self.size) = (count, size);

        if opt.detect_conflicts {
            self.conflict_keys.insert(fingerprint(&e.key));
        }
        // Managed mode can write a key at several versions in one transaction.
        if let Some(old) = self.pending_writes.remove(&e.key)
            && old.version != e.version
        {
            self.duplicate_writes.push(old);
        }
        self.pending_writes.insert(e.key.clone(), e);
        Ok(())
    }

    fn check_entry(&self, e: &Entry) -> Result<()> {
        let opt = &self.db.opt;
        let exceeds = |what: &str, max: u64, len: usize| {
            Err(Error::InvalidRequest)
                .with_context(|| format!("{what} with size {len} exceeded {max} limit"))
        };
        if !self.update {
            return Err(Error::ReadOnlyTxn);
        }
        if e.key.is_empty() {
            return Err(Error::EmptyKey);
        }
        if e.key.starts_with(BADGER_PREFIX) {
            return Err(Error::InvalidKey);
        }
        if e.key.len() > MAX_KEY_SIZE {
            return exceeds("Key", MAX_KEY_SIZE as u64, e.key.len());
        }
        if e.value.len() as u64 > opt.value_log_file_size {
            return exceeds("Value", opt.value_log_file_size, e.value.len());
        }
        if opt.in_memory && e.value.len() as i64 > self.db.value_threshold() {
            return exceeds("Value", self.db.value_threshold() as u64, e.value.len());
        }
        self.db.is_banned(&e.key)
    }

    /// Commits the writes (a no-op for none). Fails with [`Error::Conflict`]
    /// if a key this transaction read was written by a transaction that
    /// committed after this one started.
    pub fn commit(self) -> Result<()> {
        self.start_commit()?.wait()
    }

    /// Starts committing without waiting for the write (Go's `CommitWith`,
    /// with [`PendingCommit::wait`] in place of the callback). Conflicts and
    /// other early failures are returned here; write errors come from
    /// `wait`. The commit becomes visible to new transactions once written,
    /// whether or not anyone waits for it.
    pub fn start_commit(mut self) -> Result<PendingCommit> {
        if self.pending_writes.is_empty() {
            return Ok(PendingCommit { done: None });
        }
        self.commit_precheck()?;
        let done = self.commit_and_send()?;
        Ok(PendingCommit { done: Some(done) })
    }

    /// Managed mode: commits at `commit_ts` (Go's `CommitAt`).
    ///
    /// # Panics
    /// If the DB is not managed.
    pub fn commit_at(mut self, commit_ts: u64) -> Result<()> {
        assert!(
            self.db.orc.is_managed(),
            "Cannot use CommitAt with managedDB=false. Use Commit instead."
        );
        self.commit_ts = commit_ts;
        self.commit()
    }

    fn commit_precheck(&self) -> Result<()> {
        let keep_together = self.pending_writes.values().all(|e| e.version == 0);
        if keep_together && self.db.opt.managed_txns && self.commit_ts == 0 {
            return Err(Error::InvalidRequest)
                .context("CommitTs cannot be zero. Please use commit_at instead");
        }
        Ok(())
    }

    /// Picks the commit timestamp and queues the writes. Returns the channel
    /// the writer answers on; the writer marks the commit done.
    fn commit_and_send(&mut self) -> Result<crossbeam_channel::Receiver<Result<()>>> {
        let orc = &self.db.orc;
        let _ordered = orc.write_ch_lock.lock().unwrap();
        let reads = std::mem::take(&mut *self.reads.lock().unwrap());
        let req = CommitRequest {
            read_ts: self.read_ts,
            read_mark_ts: self.read_mark_ts,
            reads: &reads,
            conflict_keys: &self.conflict_keys,
            managed_commit_ts: self.commit_ts,
        };
        let Some(commit_ts) = orc.new_commit_ts(&req, &mut self.done_read) else {
            return Err(Error::Conflict);
        };

        // Entries without an explicit version (all of them, outside managed
        // mode) get the commit timestamp and are marked as one transaction.
        let mut keep_together = true;
        let pending = std::mem::take(&mut self.pending_writes);
        let duplicates = std::mem::take(&mut self.duplicate_writes);
        let mut entries: Vec<Entry> = pending.into_values().chain(duplicates).collect();
        for e in &mut entries {
            if e.version == 0 {
                e.version = commit_ts;
            } else {
                keep_together = false;
            }
        }
        for e in &mut entries {
            e.key = key_with_ts(&e.key, e.version);
            if keep_together {
                e.meta |= BIT_TXN;
            }
        }
        if keep_together {
            assert_ne!(commit_ts, 0);
            entries.push(Entry {
                key: key_with_ts(TXN_KEY, commit_ts),
                value: commit_ts.to_string().into_bytes(),
                meta: BIT_FIN_TXN,
                ..Default::default()
            });
        }
        match self.db.send_commit_to_write_ch(entries, commit_ts) {
            Ok(rx) => Ok(rx),
            Err(e) => {
                orc.done_commit(commit_ts);
                Err(e)
            }
        }
    }
}

impl Drop for Txn<'_> {
    fn drop(&mut self) {
        if !self.db.orc.is_managed() && !self.done_read {
            self.done_read = true;
            self.db.orc.done_read(self.read_mark_ts);
        }
    }
}

/// Why [`Txn::try_modify`] refused an entry.
pub(crate) enum ModifyError {
    /// The transaction is full ([`Error::TxnTooBig`]); here is the entry back.
    TooBig(Box<Entry>),
    Other(Error),
}

/// A commit queued by [`Txn::start_commit`].
#[must_use = "a pending commit can fail; call `wait` to see the result"]
pub struct PendingCommit {
    /// `None` when there was nothing to write.
    done: Option<crossbeam_channel::Receiver<Result<()>>>,
}

impl PendingCommit {
    /// Waits until the writes are applied and returns the outcome.
    pub fn wait(self) -> Result<()> {
        match self.done {
            None => Ok(()),
            Some(rx) => rx.recv().unwrap_or(Err(Error::DbClosed)),
        }
    }
}

/// Iterates over a transaction's pending writes, which all appear at the
/// read timestamp (Go's `pendingWritesIterator`).
pub(crate) struct PendingWritesIterator {
    /// Sorted by key (descending if reversed).
    entries: Vec<Entry>,
    next_idx: usize,
    read_ts: u64,
    reversed: bool,
    /// The current internal key.
    key_buf: Vec<u8>,
}

impl PendingWritesIterator {
    fn update_key(&mut self) {
        self.key_buf.clear();
        if let Some(e) = self.entries.get(self.next_idx) {
            self.key_buf = key_with_ts(&e.key, self.read_ts);
        }
    }
}

impl KvIterator for PendingWritesIterator {
    fn next(&mut self) {
        self.next_idx += 1;
        self.update_key();
    }

    fn rewind(&mut self) {
        self.next_idx = 0;
        self.update_key();
    }

    fn seek(&mut self, key: &[u8]) {
        let key = parse_key(key);
        self.next_idx = self.entries.partition_point(|e| {
            if self.reversed {
                e.key.as_slice() > key
            } else {
                e.key.as_slice() < key
            }
        });
        self.update_key();
    }

    fn key(&self) -> &[u8] {
        assert!(self.valid());
        &self.key_buf
    }

    fn value(&self) -> ValueStruct<'_> {
        let e = &self.entries[self.next_idx];
        ValueStruct {
            meta: e.meta,
            user_meta: e.user_meta,
            expires_at: e.expires_at,
            value: &e.value,
            version: self.read_ts,
        }
    }

    fn valid(&self) -> bool {
        self.next_idx < self.entries.len()
    }
}
