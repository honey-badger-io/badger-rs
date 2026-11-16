//! Transactions. Port of the read side of Go's `txn.go`: a transaction reads a
//! snapshot of the DB at its read timestamp. Writes, commits and conflict
//! detection come in Phase 5.

use crate::db::DbInner;
use crate::error::{Error, Result, ResultExt};
use crate::iterator::{Item, Iterator, IteratorOptions, is_deleted_or_expired};
use crate::util::keys::key_with_ts;

/// A snapshot of the DB. Dropping it ends it (Go's `Discard`).
pub struct Txn<'db> {
    db: &'db DbInner,
    read_ts: u64,
    /// Whether the transaction may write (always false in read-only mode).
    update: bool,
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
            update,
        }
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

    /// Looks up `key`. Deleted and expired keys give [`Error::KeyNotFound`].
    pub fn get(&self, key: &[u8]) -> Result<Item<'db>> {
        if key.is_empty() {
            return Err(Error::EmptyKey);
        }
        self.db.is_banned(key)?;
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

    /// An iterator over this transaction's snapshot.
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
}

impl Drop for Txn<'_> {
    fn drop(&mut self) {
        if !self.db.orc.is_managed() {
            self.db.orc.done_read(self.read_ts);
        }
    }
}
