//! Backups and loading them. Port of Go's `backup.go`.
//!
//! A backup is a sequence of [`pb::KvList`]s, each written as its encoded
//! length (`u64`, little-endian) followed by the encoded list. It holds
//! every version newer than `since`, deletes and expired entries included
//! (without values), so an incremental backup can be applied on top of an
//! older one. The format is Go's: backups move between the two
//! implementations.
//!
//! Differences from Go:
//! - Loading writes through [`KvLoader`], which keeps at most
//!   `max_pending_writes` batches in flight by waiting on the oldest
//!   instead of a `y.Throttle`.
//! - After loading, the oracle is advanced in place (Go assigns
//!   `nextTxnTs` while reading and marks it done at the end), also when
//!   loading fails partway.

use std::collections::VecDeque;
use std::io::{self, Read, Write};

use crossbeam_channel::Receiver;
use prost::Message;

use crate::db::{DB, DbInner};
use crate::error::{Error, Result, ResultExt};
use crate::pb;
use crate::stream::Stream;
use crate::structs::{BIT_DELETE, BIT_FIN_TXN, BIT_TXN, Entry};
use crate::util::keys::key_with_ts;

/// A loader flushes its batch once this many bytes are pending (Go's
/// `flushThreshold`).
const FLUSH_THRESHOLD: i64 = 100 << 20;

impl DB {
    /// Writes a backup of every version newer than `since` (0 for a full
    /// backup) to `w`, and returns the newest version in it, to pass as
    /// `since` to the next incremental backup (Go's `Backup`).
    ///
    /// # Panics
    /// In managed mode; use [`Stream::backup`] on a [`DB::new_stream_at`].
    pub fn backup(&self, w: &mut impl Write, since: u64) -> Result<u64> {
        let mut stream = self.new_stream();
        stream.log_prefix = "DB.Backup".into();
        stream.since_ts = since;
        stream.backup(w, since)
    }

    /// Loads a backup written by [`DB::backup`] (by Go or Rust), with up to
    /// `max_pending_writes` batches in flight (Go's `Load`). Do not write
    /// to the DB meanwhile.
    pub fn load(&self, r: impl Read, max_pending_writes: usize) -> Result<()> {
        let mut r = io::BufReader::with_capacity(16 << 10, r);
        let mut loader = self.new_kv_loader(max_pending_writes);
        let mut max_version = 0;
        let read = read_backup(&mut r, &mut loader, &mut max_version);
        // Even after an error: wait for the batches already sent, then make
        // new commits get bigger timestamps than every version written.
        let finished = loader.finish();
        self.inner.orc.advance_to(max_version);
        read.and(finished)
    }

    /// A loader for writing many KVs at their own versions (Go's
    /// `NewKVLoader`).
    pub fn new_kv_loader(&self, max_pending_writes: usize) -> KvLoader<'_> {
        KvLoader {
            db: &self.inner,
            max_pending: max_pending_writes.max(1),
            pending: VecDeque::new(),
            entries: Vec::new(),
            entries_size: 0,
            total_size: 0,
        }
    }
}

/// Feeds every KV of a backup to `loader`, tracking the newest version
/// handed to it in `max_version`.
fn read_backup(r: &mut impl Read, loader: &mut KvLoader<'_>, max_version: &mut u64) -> Result<()> {
    let mut buf = Vec::new();
    loop {
        let mut len = [0u8; 8];
        if !read_exact_or_eof(r, &mut len)? {
            return Ok(());
        }
        let len = u64::from_le_bytes(len);
        // Grow the buffer only as data arrives: a corrupt length must not
        // allocate (and abort on) an arbitrary amount of memory up front.
        buf.clear();
        let n = r
            .by_ref()
            .take(len)
            .read_to_end(&mut buf)
            .context("while reading a backup")?;
        if n as u64 != len {
            return Err(Error::InvalidDump).context("backup ends in the middle of a list");
        }
        let list = pb::KvList::decode(buf.as_slice())?;
        for kv in &list.kv {
            *max_version = (*max_version).max(kv.version);
            loader.set(kv)?;
        }
    }
}

/// Reads exactly `buf.len()` bytes, or returns `false` at a clean end of
/// input (nothing read).
fn read_exact_or_eof(r: &mut impl Read, buf: &mut [u8]) -> Result<bool> {
    let mut read = 0;
    while read < buf.len() {
        match r.read(&mut buf[read..]) {
            Ok(0) if read == 0 => return Ok(false),
            Ok(0) => {
                return Err(Error::InvalidDump).context("backup ends in a length prefix");
            }
            Ok(n) => read += n,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e.into()),
        }
    }
    Ok(true)
}

impl Stream<'_> {
    /// Writes a backup of this stream's keys (every version newer than
    /// `since`) to `w` and returns the newest version written (Go's
    /// `Stream.Backup`). Replaces `key_to_list`.
    pub fn backup(&mut self, w: &mut impl Write, since: u64) -> Result<u64> {
        self.key_to_list = Some(Box::new(move |key, it, _| {
            let mut list = pb::KvList::default();
            while it.valid() {
                let item = it.item();
                if item.key() != key {
                    break;
                }
                if item.version() < since {
                    return Err(Error::InvalidRequest).with_context(|| {
                        format!(
                            "Backup: Item Version: {} less than sinceTs: {since}",
                            item.version()
                        )
                    });
                }
                let deleted = item.is_deleted_or_expired();
                let value = if deleted {
                    Vec::new()
                } else {
                    item.value_copy().inspect_err(|e| {
                        log::error!(
                            "Key [{:?}, {}]. Error while fetching value [{e}]",
                            item.key(),
                            item.version()
                        )
                    })?
                };
                list.kv.push(pb::Kv {
                    key: item.key().to_vec(),
                    value,
                    user_meta: vec![item.user_meta()],
                    version: item.version(),
                    expires_at: item.expires_at(),
                    meta: vec![item.meta & !(BIT_TXN | BIT_FIN_TXN)],
                    ..Default::default()
                });
                if item.discard_earlier_versions() {
                    // A delete just below hides the older versions on load.
                    list.kv.push(pb::Kv {
                        key: item.key().to_vec(),
                        version: item.version() - 1,
                        meta: vec![BIT_DELETE],
                        ..Default::default()
                    });
                    break;
                }
                if deleted {
                    break;
                }
                it.next();
            }
            Ok(list)
        }));

        let mut max_version = 0;
        self.orchestrate(|mut list| {
            list.kv.retain(|kv| {
                max_version = max_version.max(kv.version);
                !kv.stream_done
            });
            write_list(&list, w)
        })?;
        Ok(max_version)
    }
}

/// Writes one length-prefixed list (Go's `writeTo`).
fn write_list(list: &pb::KvList, w: &mut impl Write) -> Result<()> {
    w.write_all(&(list.encoded_len() as u64).to_le_bytes())?;
    w.write_all(&list.encode_to_vec())?;
    Ok(())
}

/// Writes KVs at their own versions in big batches (Go's `KVLoader`).
pub struct KvLoader<'db> {
    db: &'db DbInner,
    max_pending: usize,
    pending: VecDeque<Receiver<Result<()>>>,
    entries: Vec<Entry>,
    entries_size: i64,
    total_size: i64,
}

impl KvLoader<'_> {
    /// Adds a KV, sending the current batch first if it is full (Go's
    /// `Set`).
    pub fn set(&mut self, kv: &pb::Kv) -> Result<()> {
        let mut e = Entry {
            key: key_with_ts(&kv.key, kv.version),
            value: kv.value.clone(),
            user_meta: kv.user_meta.first().copied().unwrap_or(0),
            expires_at: kv.expires_at,
            meta: kv.meta.first().copied().unwrap_or(0),
            ..Default::default()
        };
        let size = e.estimate_size_and_set_threshold(self.db.value_threshold());
        let opt = &self.db.opt;
        if self.entries.len() as u64 + 1 >= opt.max_batch_count()
            || (self.entries_size + size) as u64 >= opt.max_batch_size()
            || self.total_size >= FLUSH_THRESHOLD
        {
            self.send()?;
        }
        self.total_size += size + e.value.len() as i64;
        self.entries_size += size;
        self.entries.push(e);
        Ok(())
    }

    fn send(&mut self) -> Result<()> {
        while self.pending.len() >= self.max_pending {
            let rx = self.pending.pop_front().unwrap();
            rx.recv().unwrap_or(Err(Error::DbClosed))?;
        }
        let entries = std::mem::take(&mut self.entries);
        self.pending.push_back(self.db.send_to_write_ch(entries)?);
        self.entries_size = 0;
        self.total_size = 0;
        Ok(())
    }

    /// Sends the last batch and waits for every write (Go's `Finish`).
    pub fn finish(mut self) -> Result<()> {
        if !self.entries.is_empty() {
            self.send()?;
        }
        let mut first_err = None;
        while let Some(rx) = self.pending.pop_front() {
            if let Err(e) = rx.recv().unwrap_or(Err(Error::DbClosed)) {
                first_err.get_or_insert(e);
            }
        }
        first_err.map_or(Ok(()), Err)
    }
}

#[cfg(all(test, not(miri)))]
mod tests;
