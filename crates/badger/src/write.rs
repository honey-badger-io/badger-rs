//! The write path: the writer thread and the memtable flush thread. Port of
//! Go's `doWrites`, `writeRequests`, `writeToLSM`, `ensureRoomForWrite` and
//! `flushMemtable` from `db.go`.
//!
//! ```text
//! Txn::commit ──► write channel ──► writer thread: batch requests
//!                                     ├─ value log: big values, get pointers
//!                                     ├─ room check: full memtable → imm + flush channel
//!                                     └─ memtable: WAL + skiplist
//! flush thread ◄── flush channel: memtable → L0 table → manifest → delete WAL
//! ```
//!
//! Differences from Go: the writer collects a batch and then writes it on the
//! same thread (Go overlaps collecting the next batch with writing the
//! previous one on a second goroutine).

use std::sync::Arc;
use std::time::Duration;

use crossbeam_channel::{Receiver, Sender, TryRecvError};

use crate::db::{DbInner, WRITE_CH_CAPACITY};
use crate::error::{Error, Result, ResultExt};
use crate::memtable::MemTable;
use crate::oracle::CommitMark;
use crate::structs::{BIT_VALUE_POINTER, Entry, ValuePointer};
use crate::table::{Table, TableBuilder, new_filename};
use crate::util::KvIterator;
use crate::util::value::ValueStruct;

/// A batch of entries to write atomically, and where to report the result.
pub(crate) struct Request {
    pub entries: Vec<Entry>,
    pub done: Sender<Result<()>>,
    /// The transaction's commit mark, if any. The writer drops it once the
    /// entries are written, so a committer does not need to wait for the
    /// write to make it visible (Go's commit callback does this).
    pub commit_mark: Option<CommitMark>,
}

/// A memtable to flush, or `None` to stop the flush thread.
pub(crate) type FlushMsg = Option<Arc<MemTable>>;

/// The writer thread: runs until the write channel is disconnected and empty.
pub(crate) fn write_loop(db: &DbInner, rx: Receiver<Request>) {
    while let Ok(first) = rx.recv() {
        let mut reqs = vec![first];
        while reqs.len() < 3 * WRITE_CH_CAPACITY {
            match rx.try_recv() {
                Ok(r) => reqs.push(r),
                Err(TryRecvError::Empty | TryRecvError::Disconnected) => break,
            }
        }
        if let Err(e) = write_requests(db, &mut reqs) {
            log::error!("writeRequests: {e}");
        }
    }
}

/// Writes a batch of requests and reports the outcome to each (Go's
/// `writeRequests`).
fn write_requests(db: &DbInner, reqs: &mut [Request]) -> Result<()> {
    let res = write_requests_inner(db, reqs);
    if res.is_ok() {
        // Before the committers hear back, as in Go.
        db.publisher
            .send_updates(reqs.iter().flat_map(|r| r.entries.iter()));
    }
    let shared = res.as_ref().err().map(|e| Arc::new(clone_err(e)));
    for r in reqs.iter_mut() {
        let out = match &shared {
            None => Ok(()),
            Some(e) => Err(Error::Shared(Arc::clone(e))),
        };
        // Visible to new reads before the committer hears back, as in Go
        // (`doneCommit` runs before the callback).
        drop(r.commit_mark.take());
        // The committer may have given up waiting; that is fine.
        let _ = r.done.send(out);
    }
    res
}

/// Errors are not `Clone`; keep the message and the kind that matters.
fn clone_err(e: &Error) -> Error {
    match e.root() {
        Error::DbClosed => Error::DbClosed,
        Error::BlockedWrites => Error::BlockedWrites,
        _ => Error::corruption(e.to_string()),
    }
}

fn write_requests_inner(db: &DbInner, reqs: &mut [Request]) -> Result<()> {
    let mut entries: Vec<&mut Vec<Entry>> = reqs.iter_mut().map(|r| &mut r.entries).collect();
    let ptrs = db.vlog.write(&mut entries, db.value_threshold())?;
    for (entries, ptrs) in entries.iter_mut().zip(ptrs) {
        if entries.is_empty() {
            continue;
        }
        ensure_room_for_write(db).context("writeRequests")?;
        write_to_lsm(db, entries, &ptrs).context("writeRequests")?;
    }
    Ok(())
}

/// Makes sure the current memtable has room: a full one is moved to `imm`
/// and queued for flushing, and a new one takes its place. Blocks while the
/// flush queue is full (Go's `ensureRoomForWrite`).
fn ensure_room_for_write(db: &DbInner) -> Result<()> {
    let mut mem = db.mem.lock().unwrap();
    loop {
        let mt = mem.mt.as_ref().ok_or(Error::DbClosed)?;
        if !mt.is_full() {
            return Ok(());
        }
        match db.flush_tx.try_send(Some(Arc::clone(mt))) {
            Ok(()) => {
                let full = mem.mt.take().unwrap();
                mem.imm.push(full);
                let fid = mem.next_mem_fid;
                let new = MemTable::create(&db.opt, &db.registry, fid)
                    .context("cannot create new mem table")?;
                mem.next_mem_fid += 1;
                mem.mt = Some(Arc::new(new));
                return Ok(());
            }
            Err(crossbeam_channel::TrySendError::Full(_)) => {
                // Wait for the flush thread to free a slot. Only give up when
                // closing, or Close could wait on us forever.
                if db.is_closed() {
                    return Err(Error::DbClosed).context("No room for write");
                }
                mem = db.flush_cond.wait(mem).unwrap();
            }
            Err(crossbeam_channel::TrySendError::Disconnected(_)) => {
                return Err(Error::DbClosed);
            }
        }
    }
}

/// Writes a request's entries to the current memtable: small values inline,
/// big ones as value pointers (Go's `writeToLSM`).
fn write_to_lsm(db: &DbInner, entries: &mut [Entry], ptrs: &[ValuePointer]) -> Result<()> {
    if !db.opt.in_memory && ptrs.len() != entries.len() {
        return Err(Error::corruption("Ptrs and Entries don't match"));
    }
    let mt = db.mem.lock().unwrap().mt.clone().ok_or(Error::DbClosed)?;
    let threshold = db.value_threshold();
    for (i, e) in entries.iter_mut().enumerate() {
        let res = if e.skip_vlog_and_set_threshold(threshold) {
            // Clear a stray pointer bit (entries loaded from a backup with a
            // lower threshold can carry it).
            mt.put(
                &e.key,
                ValueStruct {
                    value: &e.value,
                    meta: e.meta & !BIT_VALUE_POINTER,
                    user_meta: e.user_meta,
                    expires_at: e.expires_at,
                    version: 0,
                },
            )
        } else {
            let vp = ptrs[i].encode();
            mt.put(
                &e.key,
                ValueStruct {
                    value: &vp,
                    meta: e.meta | BIT_VALUE_POINTER,
                    user_meta: e.user_meta,
                    expires_at: e.expires_at,
                    version: 0,
                },
            )
        };
        res.context("while writing to memTable")?;
    }
    if db.opt.sync_writes {
        mt.sync_wal()?;
    }
    Ok(())
}

/// The flush thread: turns queued memtables into L0 tables, in order, until
/// it receives `None` (Go's `flushMemtable`).
pub(crate) fn flush_loop(db: &DbInner, rx: Receiver<FlushMsg>) {
    while let Ok(Some(mt)) = rx.recv() {
        // Errors are retried forever, as in Go: dropping the memtable would
        // lose acknowledged writes.
        while let Err(e) = handle_memtable_flush(db, &mt, &[]) {
            log::error!("error flushing memtable to disk: {e}, retrying");
            std::thread::sleep(Duration::from_secs(1));
        }
        {
            let mut mem = db.mem.lock().unwrap();
            // Memtables are queued in imm order, so this is the oldest.
            assert!(
                Arc::ptr_eq(&mem.imm[0], &mt),
                "flushed memtable is not imm[0]"
            );
            mem.imm.remove(0);
        }
        // The data is in L0 and the manifest: the WAL can go.
        mt.delete_wal_on_drop();
        drop(mt);
        db.flush_cond.notify_all();
    }
}

/// Writes a memtable as a new L0 table, without the keys starting with one
/// of `drop_prefixes` (Go's `handleMemTableFlush`).
pub(crate) fn handle_memtable_flush(
    db: &DbInner,
    mt: &MemTable,
    drop_prefixes: &[Vec<u8>],
) -> Result<()> {
    let mut topt = db.table_opts.clone();
    topt.data_key = db.registry.latest_data_key()?;
    let mut builder = TableBuilder::new(topt.clone());
    let mut it = mt.skiplist().uni_iter(false);
    it.rewind();
    while it.valid() {
        if drop_prefixes.iter().any(|p| it.key().starts_with(p)) {
            it.next();
            continue;
        }
        let vs = it.value();
        let vlen = if vs.meta & BIT_VALUE_POINTER != 0 {
            ValuePointer::decode(vs.value)?.len
        } else {
            0
        };
        builder.add(it.key(), vs, vlen);
        it.next();
    }
    if builder.is_empty() {
        return Ok(());
    }
    let id = db.lc.reserve_file_id();
    let table = if db.opt.in_memory {
        Table::open_in_memory(builder.finish()?, id, topt)?
    } else {
        Table::create(new_filename(id, &db.opt.dir), builder)?
    };
    db.lc
        .add_level0_table(db, table)
        .context("error while creating table")
}
