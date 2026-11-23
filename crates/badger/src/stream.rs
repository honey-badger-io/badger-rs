//! Streaming the whole DB (or a prefix) out, fast. Port of Go's `stream.go`.
//!
//! [`Stream::orchestrate`] splits the key space into ranges (from table
//! block boundaries and the memtables), iterates over them on `num_go`
//! threads, turns each key into a [`pb::KvList`] (by default every live
//! version, see [`Stream::to_list`]) and hands batches of those to a `send`
//! callback on the calling thread. Backups and the `StreamWriter` are built
//! on it.
//!
//! Differences from Go:
//! - `send` receives a [`pb::KvList`] instead of a `z.Buffer` of encoded
//!   KVs, and runs on the calling thread (so it need not be `Send`).
//! - All producer threads read from one transaction: the stream is a single
//!   snapshot (in non-managed mode Go gives each goroutine its own read
//!   timestamp).
//! - Range boundaries are user keys, so every key belongs to exactly one
//!   range by plain comparison (Go uses internal keys, with timestamps, from
//!   table indexes and memtables).
//! - There is no context: a failing producer or `send` stops the stream and
//!   its error is returned. `KeyToList` and `KeyToListWithThreadId` are one
//!   callback that receives the thread id.

use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use crossbeam_channel::RecvTimeoutError;
use prost::Message;

use crate::db::{DB, DbInner};
use crate::error::{Error, Result};
use crate::iterator::{Item, Iterator, IteratorOptions};
use crate::pb;
use crate::txn::Txn;
use crate::util::KvIterator;
use crate::util::keys::parse_key;

/// A producer sends its batch once it is this big (Go's `batchSize`).
const BATCH_SIZE: usize = 16 << 20;

/// Default largest batch handed to `send` (Go's `maxStreamSize`).
pub const MAX_STREAM_SIZE: u64 = 100 << 20;

/// Turns the key the iterator is at into the KVs to send, advancing the
/// iterator past what it used. Gets the producer thread's id.
pub type KeyToList<'a> = dyn Fn(&[u8], &mut Iterator<'_>, usize) -> Result<pb::KvList> + Sync + 'a;

/// Chooses which keys to stream.
pub type ChooseKey<'a> = dyn Fn(&Item<'_>) -> bool + Sync + 'a;

/// Called with a thread id after the thread finishes a range; its KVs are
/// sent too (Go's `FinishThread`).
pub type FinishThread<'a> = dyn Fn(usize) -> Result<pb::KvList> + Sync + 'a;

/// A configured stream over a DB; see the [module docs](self).
pub struct Stream<'db> {
    /// Only stream keys with this prefix.
    pub prefix: Vec<u8>,
    /// Number of producer threads.
    pub num_go: usize,
    /// Prefix for log messages.
    pub log_prefix: String,
    /// If set, only keys it returns true for are streamed.
    pub choose_key: Option<Box<ChooseKey<'db>>>,
    /// Largest batch (in encoded bytes, roughly) handed to `send` at once.
    pub max_size: u64,
    /// Replaces [`Stream::to_list`].
    pub key_to_list: Option<Box<KeyToList<'db>>>,
    /// See [`FinishThread`].
    pub finish_thread: Option<Box<FinishThread<'db>>>,
    /// Only stream versions above this timestamp.
    pub since_ts: u64,
    /// Managed mode: the timestamp to read at.
    read_ts: Option<u64>,
    done_markers: bool,
    db: &'db DbInner,
}

impl DB {
    /// A stream over the DB (Go's `NewStream`).
    ///
    /// # Panics
    /// In managed mode; use [`DB::new_stream_at`].
    pub fn new_stream(&self) -> Stream<'_> {
        assert!(
            !self.inner.opt.managed_txns,
            "This API can not be called in managed mode."
        );
        Stream::new(&self.inner, None)
    }

    /// Managed mode: a stream reading at `read_ts` (Go's `NewStreamAt`).
    ///
    /// # Panics
    /// If the DB is not managed.
    pub fn new_stream_at(&self, read_ts: u64) -> Stream<'_> {
        assert!(
            self.inner.opt.managed_txns,
            "This API can only be called in managed mode."
        );
        Stream::new(&self.inner, Some(read_ts))
    }

    /// The on-disk and uncompressed sizes of the tables whose keys all start
    /// with `prefix` (Go's `EstimateSize`).
    pub fn estimate_size(&self, prefix: &[u8]) -> (u64, u64) {
        estimate_size(&self.inner, prefix)
    }
}

fn estimate_size(db: &DbInner, prefix: &[u8]) -> (u64, u64) {
    let mut on_disk = 0;
    let mut uncompressed = 0;
    for ti in db.lc.table_info() {
        if ti.left.starts_with(prefix) && ti.right.starts_with(prefix) {
            on_disk += ti.on_disk_size as u64;
            uncompressed += ti.uncompressed_size as u64;
        }
    }
    (on_disk, uncompressed)
}

impl<'db> Stream<'db> {
    pub(crate) fn new(db: &'db DbInner, read_ts: Option<u64>) -> Self {
        Stream {
            prefix: Vec::new(),
            num_go: db.opt.num_goroutines.max(1),
            log_prefix: "Badger.Stream".into(),
            choose_key: None,
            max_size: MAX_STREAM_SIZE,
            key_to_list: None,
            finish_thread: None,
            since_ts: 0,
            read_ts,
            done_markers: false,
            db,
        }
    }

    /// Whether to end each range with a KV marked `stream_done` (Go's
    /// `SendDoneMarkers`; the stream writer uses them).
    pub fn send_done_markers(&mut self, done: bool) {
        self.done_markers = done;
    }

    /// The default [`KeyToList`]: every version of `key` from the
    /// iterator's position down to a delete, an expired version, or one that
    /// discards earlier versions (only the newest if the DB keeps one
    /// version). Go's `ToList`.
    pub fn to_list(&self, key: &[u8], it: &mut Iterator<'_>) -> Result<pb::KvList> {
        to_list(self.db, key, it)
    }

    /// Streams the selected keys, calling `send` with batches of KVs until
    /// done or an error (Go's `Orchestrate`).
    pub fn orchestrate(&self, mut send: impl FnMut(pb::KvList) -> Result<()>) -> Result<()> {
        let db = self.db;
        if db.is_closed() {
            return Err(Error::DbClosed);
        }
        let txn = Txn::new(db, false, self.read_ts);
        let ranges = ranges(db, &self.prefix, self.num_go);
        log::info!(
            "{} Number of ranges found: {}",
            self.log_prefix,
            ranges.len()
        );
        let (range_tx, range_rx) = crossbeam_channel::unbounded();
        for r in ranges {
            range_tx.send(r).unwrap();
        }
        drop(range_tx);
        let (kv_tx, kv_rx) = crossbeam_channel::bounded::<pb::KvList>(32);
        let state = State {
            cancel: AtomicBool::new(false),
            next_stream_id: AtomicU32::new(0),
            scanned: AtomicU64::new(0),
        };

        std::thread::scope(|s| {
            let producers: Vec<_> = (0..self.num_go)
                .map(|thread_id| {
                    let (txn, state, range_rx, kv_tx) =
                        (&txn, &state, range_rx.clone(), kv_tx.clone());
                    s.spawn(move || {
                        let res = self.produce_kvs(txn, state, thread_id, &range_rx, &kv_tx);
                        if res.is_err() {
                            state.cancel.store(true, Ordering::SeqCst);
                        }
                        res
                    })
                })
                .collect();
            // The channel closes once every producer is done.
            drop(kv_tx);
            let sent = self.stream_kvs(&state, &kv_rx, &mut send);
            if sent.is_err() {
                state.cancel.store(true, Ordering::SeqCst);
                // Unblock producers waiting for room.
                for _ in kv_rx.iter() {}
            }
            let mut first_err = None;
            for p in producers {
                if let Err(e) = p.join().expect("stream producer panicked") {
                    first_err.get_or_insert(e);
                }
            }
            // A failed send makes producers fail too; report the cause.
            sent?;
            first_err.map_or(Ok(()), Err)
        })
    }

    /// One producer thread: iterates over ranges until none is left (Go's
    /// `produceKVs`).
    fn produce_kvs(
        &self,
        txn: &Txn<'_>,
        state: &State,
        thread_id: usize,
        ranges: &crossbeam_channel::Receiver<KeyRange>,
        out: &crossbeam_channel::Sender<pb::KvList>,
    ) -> Result<()> {
        while let Ok(kr) = ranges.recv() {
            if state.cancel.load(Ordering::SeqCst) {
                return Ok(());
            }
            self.iterate(txn, state, thread_id, &kr, out)?;
        }
        Ok(())
    }

    fn iterate(
        &self,
        txn: &Txn<'_>,
        state: &State,
        thread_id: usize,
        kr: &KeyRange,
        out: &crossbeam_channel::Sender<pb::KvList>,
    ) -> Result<()> {
        let mut it = txn.iter(IteratorOptions {
            all_versions: true,
            prefix: self.prefix.clone(),
            prefetch_values: true,
            since_ts: self.since_ts,
            ..Default::default()
        });
        let stream_id = state.next_stream_id.fetch_add(1, Ordering::SeqCst) + 1;
        let mut batch = Batch::default();
        let send = |batch: &mut Batch| -> Result<()> {
            state
                .scanned
                .fetch_add(batch.size as u64, Ordering::Relaxed);
            let list = std::mem::take(batch).list;
            // Fails only once the consumer gave up (after an error).
            out.send(list).map_err(|_| Error::Rejected)
        };
        let add = |batch: &mut Batch, list: pb::KvList| -> Result<()> {
            for mut kv in list.kv {
                kv.stream_id = stream_id;
                batch.push(kv);
                if batch.size >= BATCH_SIZE {
                    send(batch)?;
                }
            }
            Ok(())
        };

        let mut prev_key: Vec<u8> = Vec::new();
        it.seek(&kr.left);
        while it.valid() {
            if state.cancel.load(Ordering::SeqCst) {
                return Ok(());
            }
            let item = it.item();
            // Older versions of a key already handled.
            if item.key() == prev_key.as_slice() {
                it.next();
                continue;
            }
            prev_key.clear();
            prev_key.extend_from_slice(item.key());
            if !kr.right.is_empty() && item.key() >= kr.right.as_slice() {
                break;
            }
            if let Some(choose) = &self.choose_key
                && !choose(item)
            {
                continue;
            }
            let key = item.key().to_vec();
            let list = match &self.key_to_list {
                Some(f) => f(&key, &mut it, thread_id),
                None => to_list(self.db, &key, &mut it),
            };
            match list {
                Ok(list) => add(&mut batch, list)?,
                Err(e) => log::warn!("While reading key: {}, got error: {e}", hex(&key)),
            }
        }
        if let Some(finish) = &self.finish_thread {
            add(&mut batch, finish(thread_id)?)?;
        }
        if self.done_markers {
            batch.push(pb::Kv {
                stream_id,
                stream_done: true,
                ..Default::default()
            });
        }
        if batch.list.kv.is_empty() {
            return Ok(());
        }
        send(&mut batch)
    }

    /// The consumer: gathers producers' batches up to `max_size` and calls
    /// `send` with them, logging progress (Go's `streamKVs`).
    fn stream_kvs(
        &self,
        state: &State,
        rx: &crossbeam_channel::Receiver<pb::KvList>,
        send: &mut impl FnMut(pb::KvList) -> Result<()>,
    ) -> Result<()> {
        let (on_disk, uncompressed) = estimate_size(self.db, &self.prefix);
        log::info!(
            "{} Streaming about {} bytes of uncompressed data ({} on disk)",
            self.log_prefix,
            uncompressed * 6 / 5,
            on_disk
        );
        let start = Instant::now();
        let mut bytes_sent: u64 = 0;
        loop {
            let first = match rx.recv_timeout(Duration::from_secs(5)) {
                Ok(list) => list,
                Err(RecvTimeoutError::Timeout) => {
                    log::info!(
                        "{} [{:?}] Scanned ~{} bytes. Sent: {} bytes",
                        self.log_prefix,
                        start.elapsed(),
                        state.scanned.load(Ordering::Relaxed),
                        bytes_sent
                    );
                    continue;
                }
                Err(RecvTimeoutError::Disconnected) => break,
            };
            let mut batch = Batch::default();
            batch.extend(first);
            // Slurp what else is ready, up to max_size.
            while (batch.size as u64) <= self.max_size {
                match rx.try_recv() {
                    Ok(list) => batch.extend(list),
                    Err(_) => break,
                }
            }
            if batch.list.kv.is_empty() {
                continue;
            }
            bytes_sent += batch.size as u64;
            if let Err(e) = send(batch.list) {
                log::warn!("Error while sending: {e}");
                return Err(e);
            }
        }
        log::info!("{} Sent data of size {} bytes", self.log_prefix, bytes_sent);
        Ok(())
    }
}

/// Shared by a stream's threads.
struct State {
    /// Set when a producer or the consumer fails: everyone stops.
    cancel: AtomicBool,
    next_stream_id: AtomicU32,
    /// Bytes handed to the consumer, for progress logs.
    scanned: AtomicU64,
}

/// KVs and their approximate encoded size.
#[derive(Default)]
struct Batch {
    list: pb::KvList,
    size: usize,
}

impl Batch {
    fn push(&mut self, kv: pb::Kv) {
        self.size += kv.encoded_len() + 4;
        self.list.kv.push(kv);
    }

    fn extend(&mut self, list: pb::KvList) {
        for kv in list.kv {
            self.push(kv);
        }
    }
}

fn to_list(db: &DbInner, key: &[u8], it: &mut Iterator<'_>) -> Result<pb::KvList> {
    let mut list = pb::KvList::default();
    while it.valid() {
        let item = it.item();
        if item.is_deleted_or_expired() || item.key() != key {
            break;
        }
        list.kv.push(pb::Kv {
            key: key.to_vec(),
            value: item.value_copy()?,
            version: item.version(),
            expires_at: item.expires_at(),
            user_meta: vec![item.user_meta()],
            ..Default::default()
        });
        if db.opt.num_versions_to_keep == 1 || item.discard_earlier_versions() {
            break;
        }
        it.next();
    }
    Ok(list)
}

/// A range of user keys `[left, right)`; empty ends are unbounded.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct KeyRange {
    pub left: Vec<u8>,
    pub right: Vec<u8>,
    /// Approximate uncompressed bytes in the range.
    pub size: u64,
}

/// Splits the keys with `prefix` into about `num_ranges` ranges of similar
/// size, covering everything (Go's `DB.Ranges`).
pub(crate) fn ranges(db: &DbInner, prefix: &[u8], num_ranges: usize) -> Vec<KeyRange> {
    let tables = db.lc.table_info();
    let max_level = db.opt.max_levels - 1;
    // Split at the last level's table boundaries...
    let mut splits: Vec<Vec<u8>> = tables
        .iter()
        .filter(|t| t.level == max_level && t.right.starts_with(prefix))
        .map(|t| parse_key(&t.right).to_vec())
        .collect();
    // ...or, if that is too coarse, at block boundaries of every table...
    if splits.len() < 32 {
        let per_table = (32 / tables.len().max(1)).max(1);
        splits.clear();
        for l in &db.lc.levels {
            for t in &l.read().tables {
                match t.key_splits(per_table, prefix) {
                    Ok(keys) => splits.extend(keys.iter().map(|k| parse_key(k).to_vec())),
                    Err(e) => log::warn!("while splitting table {}: {e}", t.id()),
                }
            }
        }
    }
    // ...and at every 10,000th key of the memtables.
    if splits.len() < 32 {
        for mt in db.mem_tables() {
            let mut it = mt.skiplist().uni_iter(false);
            it.rewind();
            let mut count: usize = 0;
            while it.valid() {
                if count.is_multiple_of(10_000) && it.key().starts_with(prefix) {
                    splits.push(parse_key(it.key()).to_vec());
                }
                count += 1;
                it.next();
            }
        }
    }
    splits.sort();
    splits.dedup();

    let mut ranges = Vec::with_capacity(splits.len() + 1);
    let mut start = Vec::new();
    for key in splits {
        if key.is_empty() {
            continue;
        }
        ranges.push(KeyRange {
            left: start,
            right: key.clone(),
            size: 0,
        });
        start = key;
    }
    ranges.push(KeyRange {
        left: start,
        ..Default::default()
    });

    // The approximate size of each bounded range.
    for t in &tables {
        let (tl, tr) = (parse_key(&t.left), parse_key(&t.right));
        for r in &mut ranges {
            if r.left.is_empty() || r.right.is_empty() {
                continue;
            }
            if r.left.as_slice() <= tr && tl < r.right.as_slice() {
                r.size += t.uncompressed_size as u64;
            }
        }
    }
    let total: u64 = ranges.iter().map(|r| r.size).sum();
    if total == 0 {
        return ranges;
    }
    // Merge neighbours up to the average size.
    let avg = total / num_ranges.max(1) as u64;
    let mut out: Vec<KeyRange> = Vec::new();
    let mut i = 0;
    while i < ranges.len() {
        let mut cur = ranges[i].clone();
        i += 1;
        while i < ranges.len() && cur.size + ranges[i].size <= avg {
            cur.right = ranges[i].right.clone();
            cur.size += ranges[i].size;
            i += 1;
        }
        out.push(cur);
    }
    // Biggest first, as in Go.
    out.sort_by_key(|r| std::cmp::Reverse(r.size));
    out
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

#[cfg(all(test, not(miri)))]
mod tests;
