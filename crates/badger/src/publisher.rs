//! Subscriptions to writes. Port of Go's `publisher.go` (and
//! `DB.Subscribe`).
//!
//! After the writer thread applies a batch of writes, it looks up which
//! subscribers' [`pb::Match`]es (key prefixes, with optional ignored bytes)
//! each key matches, using a [`Trie`], and queues the matching KVs to a
//! publisher thread, which hands each subscriber its KVs in commit order.
//!
//! Differences from Go:
//! - [`DB::subscribe`] returns a [`Subscription`] to receive from instead of
//!   calling a callback until a context is cancelled; dropping it
//!   unsubscribes. It does not borrow the DB, and returns `None` once the DB
//!   is closed.
//! - Matching happens on the writer thread (only when there are
//!   subscribers), and only matching KVs are queued; Go queues whole
//!   requests to the publisher goroutine. The queue sizes are Go's, but a
//!   subscriber that falls a full queue behind is dropped instead of
//!   blocking the publisher (and so every write and `close`): its
//!   [`Subscription`] ends, and [`Subscription::fell_behind`] tells why.
//! - As in Go, a KV's `meta` holds the entry's *user* meta, and deletes are
//!   published as KVs with an empty value.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crossbeam_channel::{Receiver, RecvTimeoutError, Sender, TrySendError};

use crate::db::DB;
use crate::error::{Error, Result, ResultExt};
use crate::pb;
use crate::structs::Entry;
use crate::trie::Trie;
use crate::util::keys::{parse_key, parse_ts};

/// Capacity of the publisher's queue and of each subscriber's (Go's).
const QUEUE_CAPACITY: usize = 1000;

/// Updates for each subscriber id, in commit order.
type Updates = Vec<(u64, pb::KvList)>;

struct Subscriber {
    matches: Vec<pb::Match>,
    tx: Sender<pb::KvList>,
    /// Cleared when the subscription is dropped: stop sending to it.
    active: Arc<AtomicBool>,
    /// Set when it was dropped for falling behind.
    lagged: Arc<AtomicBool>,
}

#[derive(Default)]
struct State {
    subscribers: HashMap<u64, Subscriber>,
    next_id: u64,
    indexer: Trie,
}

/// The subscriber registry; shared with every [`Subscription`].
#[derive(Default)]
pub(crate) struct Registry {
    state: Mutex<State>,
    /// Set when the publisher thread ends (the DB closed).
    closed: AtomicBool,
}

/// The DB's side of subscriptions (Go's `publisher`).
pub(crate) struct Publisher {
    pub registry: Arc<Registry>,
    /// To the publisher thread; `None` once closing.
    tx: Mutex<Option<Sender<Updates>>>,
}

impl Publisher {
    /// A publisher and the receiving end for its thread.
    pub fn new() -> (Publisher, Receiver<Updates>) {
        let (tx, rx) = crossbeam_channel::bounded(QUEUE_CAPACITY);
        let p = Publisher {
            registry: Arc::new(Registry::default()),
            tx: Mutex::new(Some(tx)),
        };
        (p, rx)
    }

    /// Queues the written entries (keys with versions) that subscribers
    /// match (Go's `sendUpdates` and the matching in `publishUpdates`).
    pub fn send_updates<'a>(&self, entries: impl IntoIterator<Item = &'a Entry>) {
        let updates = {
            let state = self.registry.state.lock().unwrap();
            if state.subscribers.is_empty() {
                return;
            }
            let mut batched: HashMap<u64, pb::KvList> = HashMap::new();
            for e in entries {
                let ids = state.indexer.get(&e.key);
                if ids.is_empty() {
                    continue;
                }
                let kv = pb::Kv {
                    key: parse_key(&e.key).to_vec(),
                    value: e.value.clone(),
                    // Go puts the user meta here.
                    meta: vec![e.user_meta],
                    expires_at: e.expires_at,
                    version: parse_ts(&e.key),
                    ..Default::default()
                };
                for id in ids {
                    batched.entry(id).or_default().kv.push(kv.clone());
                }
            }
            if batched.is_empty() {
                return;
            }
            let mut updates: Updates = batched.into_iter().collect();
            updates.sort_by_key(|(id, _)| *id);
            updates
        };
        let tx = self.tx.lock().unwrap().clone();
        if let Some(tx) = tx {
            // Blocks while the publisher is behind, as in Go.
            let _ = tx.send(updates);
        }
    }

    /// Stops queueing; the publisher thread exits once the queue is empty.
    pub fn close(&self) {
        drop(self.tx.lock().unwrap().take());
    }
}

/// The publisher thread: forwards updates to subscribers until the queue
/// closes, then ends every subscription (Go's `listenForUpdates`).
pub(crate) fn publish_loop(registry: &Registry, rx: Receiver<Updates>) {
    for updates in rx.iter() {
        let mut state = registry.state.lock().unwrap();
        for (id, list) in updates {
            let Some(s) = state.subscribers.get(&id) else {
                continue;
            };
            if !s.active.load(Ordering::SeqCst) {
                continue;
            }
            // Never block on a subscriber: one that is not reading would
            // stall every write and `DB::close`.
            match s.tx.try_send(list) {
                Ok(()) | Err(TrySendError::Disconnected(_)) => {}
                Err(TrySendError::Full(_)) => {
                    log::warn!("Subscriber {id} fell behind; dropping it");
                    s.lagged.store(true, Ordering::SeqCst);
                    s.active.store(false, Ordering::SeqCst);
                    // Dropping its sender ends the subscription once drained.
                    let s = state.subscribers.remove(&id).unwrap();
                    for m in &s.matches {
                        let _ = state.indexer.delete_match(m, id);
                    }
                }
            }
        }
    }
    // Closing: dropping the senders ends the subscriptions once drained.
    let mut state = registry.state.lock().unwrap();
    registry.closed.store(true, Ordering::SeqCst);
    state.subscribers.clear();
    state.indexer = Trie::new();
}

impl DB {
    /// Subscribes to writes of keys matching any of `matches` (Go's
    /// `Subscribe`). Writes committed from now on are delivered, in commit
    /// order, through the returned [`Subscription`].
    pub fn subscribe(&self, matches: &[pb::Match]) -> Result<Subscription> {
        if self.is_closed() {
            return Err(Error::DbClosed);
        }
        let registry = &self.inner.publisher.registry;
        let (tx, rx) = crossbeam_channel::bounded(QUEUE_CAPACITY);
        let active = Arc::new(AtomicBool::new(true));
        let lagged = Arc::new(AtomicBool::new(false));
        let mut state = registry.state.lock().unwrap();
        if registry.closed.load(Ordering::SeqCst) {
            return Err(Error::DbClosed);
        }
        let id = state.next_id;
        state.next_id += 1;
        for m in matches {
            if let Err(e) = state.indexer.add_match(m, id) {
                // Undo the matches added so far.
                for m in matches {
                    let _ = state.indexer.delete_match(m, id);
                }
                return Err(e).context("while creating a new subscriber");
            }
        }
        state.subscribers.insert(
            id,
            Subscriber {
                matches: matches.to_vec(),
                tx,
                active: Arc::clone(&active),
                lagged: Arc::clone(&lagged),
            },
        );
        Ok(Subscription {
            id,
            rx,
            active,
            lagged,
            registry: Arc::clone(registry),
        })
    }
}

/// Receives the writes a subscriber matches; see [`DB::subscribe`].
/// Dropping it unsubscribes.
pub struct Subscription {
    id: u64,
    rx: Receiver<pb::KvList>,
    active: Arc<AtomicBool>,
    lagged: Arc<AtomicBool>,
    registry: Arc<Registry>,
}

impl Subscription {
    /// Waits for updates and returns everything queued so far as one list
    /// (Go's `slurp`). `None` once the DB is closed, or the subscriber was
    /// dropped for falling behind, and all updates were received.
    pub fn recv(&self) -> Option<pb::KvList> {
        let first = self.rx.recv().ok()?;
        Some(self.slurp(first))
    }

    /// Like [`Subscription::recv`], but gives up after `timeout`. `None` on
    /// timeout or once the subscription ended (see
    /// [`Subscription::is_closed`]).
    pub fn recv_timeout(&self, timeout: Duration) -> Option<pb::KvList> {
        match self.rx.recv_timeout(timeout) {
            Ok(first) => Some(self.slurp(first)),
            Err(RecvTimeoutError::Timeout | RecvTimeoutError::Disconnected) => None,
        }
    }

    /// Whether the subscription ended (the DB closed, or it fell behind)
    /// and every update was received.
    pub fn is_closed(&self) -> bool {
        (self.registry.closed.load(Ordering::SeqCst) || self.fell_behind()) && self.rx.is_empty()
    }

    /// Whether the subscription was ended because its queue filled up (it
    /// was not read fast enough); later writes were not delivered.
    pub fn fell_behind(&self) -> bool {
        self.lagged.load(Ordering::SeqCst)
    }

    fn slurp(&self, mut batch: pb::KvList) -> pb::KvList {
        while let Ok(more) = self.rx.try_recv() {
            batch.kv.extend(more.kv);
        }
        batch
    }
}

impl Drop for Subscription {
    fn drop(&mut self) {
        // Stop sends first, so the publisher never blocks on us.
        self.active.store(false, Ordering::SeqCst);
        while self.rx.try_recv().is_ok() {}
        let mut state = self.registry.state.lock().unwrap();
        if let Some(s) = state.subscribers.remove(&self.id) {
            for m in &s.matches {
                let _ = state.indexer.delete_match(m, self.id);
            }
        }
    }
}

#[cfg(all(test, not(miri)))]
mod tests;
