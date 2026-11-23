//! Writing a sorted stream of KVs straight into tables. Port of Go's
//! `stream_writer.go`.
//!
//! A [`StreamWriter`] takes the output of a [`Stream`](crate::Stream) (KVs
//! sorted by key within each stream id, newest version first) and builds
//! tables at the bottom of the LSM tree directly, skipping the memtable and
//! compactions: the fastest way to load a backup or copy a DB. Each stream
//! id gets its own table builder. Values above the threshold go to the
//! value log first, as usual.
//!
//! [`StreamWriter::prepare`] deletes all data first;
//! [`StreamWriter::prepare_incremental`] adds to an existing DB, writing one
//! level above its data (its key ranges must not overlap the new ones).
//! Writes and compactions pause from `prepare` until the writer is flushed
//! or dropped.
//!
//! Differences from Go:
//! - `write` takes a [`pb::KvList`] (Go a `z.Buffer` of encoded KVs).
//! - Streams in one `write` are built on scoped threads, and tables are
//!   written synchronously by their stream's thread (Go has a goroutine per
//!   stream and per table).
//! - Writing to a closed stream, or keys out of order, is an error (Go
//!   panics on both).
//! - [`StreamWriter::flush`] consumes the writer; dropping it is Go's
//!   `Cancel`. After a non-managed flush, the oracle is advanced in place
//!   (Go replaces it).
//! - `prepare_incremental` on a DB whose data is all in L0 flattens it and
//!   then writes above the level the data ended up in (Go assumes the last
//!   level).

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::db::{DB, DropGuard};
use crate::error::{Error, Result, ResultExt};
use crate::manifest::new_create_change;
use crate::pb;
use crate::structs::{BIT_VALUE_POINTER, Entry, ValuePointer};
use crate::table::{Table, TableBuilder, TableOptions, new_filename};
use crate::util::keys::{compare_keys, key_with_ts, same_key};
use crate::util::sync_dir;
use crate::util::value::ValueStruct;

/// Writes sorted KV streams into tables; see the [module docs](self).
pub struct StreamWriter<'db> {
    db: &'db DB,
    /// Holds writes and compactions paused (`None` before `prepare`).
    paused: Option<Paused<'db>>,
    max_version: u64,
    /// `None` once the stream is closed by a done marker.
    writers: HashMap<u32, Option<SortedWriter<'db>>>,
    /// New streams write at the level above this one.
    prev_level: usize,
}

/// Writes and compactions stay paused while this lives (Go's `done`).
struct Paused<'db> {
    db: &'db DB,
    /// Unblocks writes when dropped, after compactions restart.
    _writes: DropGuard<'db>,
}

impl Drop for Paused<'_> {
    fn drop(&mut self) {
        if let Err(e) = self.db.start_compactions() {
            log::error!("while restarting compactions after a stream write: {e}");
        }
    }
}

impl DB {
    /// A stream writer (Go's `NewStreamWriter`). Call
    /// [`StreamWriter::prepare`] or [`StreamWriter::prepare_incremental`]
    /// before writing.
    pub fn new_stream_writer(&self) -> StreamWriter<'_> {
        StreamWriter {
            db: self,
            paused: None,
            max_version: 0,
            writers: HashMap::new(),
            prev_level: 0,
        }
    }
}

impl<'db> StreamWriter<'db> {
    /// Pauses writes and compactions and deletes all data (Go's `Prepare`).
    pub fn prepare(&mut self) -> Result<()> {
        let writes = self.db.prepare_to_drop()?;
        self.db.stop_compactions();
        self.paused = Some(Paused {
            db: self.db,
            _writes: writes,
        });
        self.db.drop_all_inner()
    }

    /// Pauses writes and compactions to add to the existing data (Go's
    /// `PrepareIncremental`). The memtables must be empty, and the new keys
    /// must not overlap the existing ones.
    pub fn prepare_incremental(&mut self) -> Result<()> {
        let writes = self.db.prepare_to_drop()?;
        self.db.stop_compactions();
        self.paused = Some(Paused {
            db: self.db,
            _writes: writes,
        });
        let inner = &self.db.inner;
        if inner.mem_tables().iter().any(|m| !m.skiplist().is_empty()) {
            return Err(Error::InvalidRequest)
                .context("Unable to do incremental writes because MemTable has data");
        }
        let first_level = |db: &DB| {
            db.levels()
                .iter()
                .find(|l| l.num_tables > 0)
                .map(|l| l.level)
        };
        let Some(level) = first_level(self.db) else {
            // An empty DB: write at the bottom, as after `prepare`.
            return Ok(());
        };
        self.prev_level = level;
        if level == 0 {
            // L0 tables overlap; consolidate them first.
            self.db
                .flatten_inner(3)
                .context("error during flatten in StreamWriter")?;
            self.prev_level = first_level(self.db).unwrap_or(inner.opt.max_levels);
        }
        if self.prev_level == 0 {
            return Err(Error::InvalidRequest)
                .context("StreamWriter: no level above the existing data");
        }
        Ok(())
    }

    /// Writes a batch of KVs (Go's `Write`). Within a stream id, keys must
    /// come in increasing order and versions of a key newest first; a KV
    /// with `stream_done` closes its stream.
    pub fn write(&mut self, list: &pb::KvList) -> Result<()> {
        assert!(
            self.paused.is_some(),
            "StreamWriter::write called before prepare"
        );
        if list.kv.is_empty() {
            return Ok(());
        }
        let mut closed = HashSet::new();
        let mut reqs: HashMap<u32, Vec<Entry>> = HashMap::new();
        for kv in &list.kv {
            if kv.stream_done {
                closed.insert(kv.stream_id);
                continue;
            }
            if closed.contains(&kv.stream_id)
                || matches!(self.writers.get(&kv.stream_id), Some(None))
            {
                return Err(Error::InvalidRequest).with_context(|| {
                    format!("write performed on closed stream: {}", kv.stream_id)
                });
            }
            self.max_version = self.max_version.max(kv.version);
            if self.prev_level == 0 {
                self.prev_level = self.db.inner.opt.max_levels;
            }
            reqs.entry(kv.stream_id).or_default().push(Entry {
                key: key_with_ts(&kv.key, kv.version),
                value: kv.value.clone(),
                user_meta: kv.user_meta.first().copied().unwrap_or(0),
                expires_at: kv.expires_at,
                meta: kv.meta.first().copied().unwrap_or(0),
                ..Default::default()
            });
        }

        // Big values go to the value log first.
        let mut ids: Vec<u32> = reqs.keys().copied().collect();
        ids.sort_unstable();
        let mut groups: Vec<Vec<Entry>> = ids.iter().map(|id| reqs.remove(id).unwrap()).collect();
        let ptrs = {
            let mut refs: Vec<&mut Vec<Entry>> = groups.iter_mut().collect();
            self.db
                .inner
                .vlog
                .write(&mut refs, self.db.inner.value_threshold())?
        };

        for &id in &ids {
            if !self.writers.contains_key(&id) {
                let w = SortedWriter::new(self.db, id, self.prev_level - 1)
                    .with_context(|| format!("failed to create writer with ID {id}"))?;
                self.writers.insert(id, Some(w));
            }
        }
        // Each stream's entries go to its own builder, in parallel.
        let mut jobs: Vec<(&mut SortedWriter<'db>, Vec<Entry>, Vec<ValuePointer>)> = Vec::new();
        let mut ptrs = ptrs.into_iter();
        let mut writers: HashMap<u32, &mut SortedWriter<'db>> = self
            .writers
            .iter_mut()
            .filter_map(|(id, w)| w.as_mut().map(|w| (*id, w)))
            .collect();
        for (id, entries) in ids.iter().zip(groups) {
            let w = writers.remove(id).expect("writer exists");
            jobs.push((w, entries, ptrs.next().unwrap_or_default()));
        }
        let results: Vec<Result<()>> = std::thread::scope(|s| {
            let handles: Vec<_> = jobs
                .into_iter()
                .map(|(w, entries, ptrs)| s.spawn(move || w.process(entries, &ptrs)))
                .collect();
            handles
                .into_iter()
                .map(|h| h.join().expect("stream writer thread panicked"))
                .collect()
        });
        results.into_iter().collect::<Result<()>>()?;

        let mut closed: Vec<u32> = closed.into_iter().collect();
        closed.sort_unstable();
        for id in closed {
            match self.writers.get_mut(&id) {
                Some(slot @ Some(_)) => slot.take().unwrap().done()?,
                Some(None) => {}
                None => {
                    log::warn!("Trying to close stream: {id}, but no sorted writer found for it")
                }
            }
        }
        Ok(())
    }

    /// Writes the remaining tables, makes the new versions visible and
    /// resumes writes and compactions (Go's `Flush`). Resumes them even on
    /// error.
    pub fn flush(mut self) -> Result<()> {
        assert!(
            self.paused.is_some(),
            "StreamWriter::flush called before prepare"
        );
        let mut ids: Vec<u32> = self.writers.keys().copied().collect();
        ids.sort_unstable();
        for id in ids {
            if let Some(w) = self.writers.get_mut(&id).and_then(Option::take) {
                w.done()?;
            }
        }
        let inner = &self.db.inner;
        if !inner.opt.managed_txns {
            inner.orc.advance_to(self.max_version);
        }
        for l in &inner.lc.levels {
            l.sort_tables();
        }
        if !inner.opt.in_memory {
            if inner.opt.value_dir != inner.opt.dir {
                sync_dir(&inner.opt.value_dir)?;
            }
            sync_dir(&inner.opt.dir)?;
        }
        inner.lc.validate()
    }
}

/// Builds the tables of one stream (Go's `sortedWriter`).
struct SortedWriter<'db> {
    db: &'db DB,
    opts: TableOptions,
    builder: TableBuilder,
    last_key: Vec<u8>,
    level: usize,
    stream_id: u32,
}

impl<'db> SortedWriter<'db> {
    fn new(db: &'db DB, stream_id: u32, level: usize) -> Result<Self> {
        let inner = &db.inner;
        let mut opts = inner.table_opts.clone();
        // Tables as big as those of the last level.
        for _ in 2..inner.opt.max_levels {
            opts.table_size *= inner.opt.table_size_multiplier as u64;
        }
        opts.data_key = inner.registry.latest_data_key()?;
        Ok(SortedWriter {
            db,
            builder: TableBuilder::new(opts.clone()),
            opts,
            last_key: Vec::new(),
            level,
            stream_id,
        })
    }

    /// Adds a request's entries; `ptrs` are their value pointers.
    fn process(&mut self, mut entries: Vec<Entry>, ptrs: &[ValuePointer]) -> Result<()> {
        let threshold = self.db.inner.value_threshold();
        for (i, e) in entries.iter_mut().enumerate() {
            let inline = e.skip_vlog_and_set_threshold(threshold) || ptrs.is_empty();
            let vp;
            let vs = if inline {
                ValueStruct {
                    value: &e.value,
                    meta: e.meta & !BIT_VALUE_POINTER,
                    user_meta: e.user_meta,
                    expires_at: e.expires_at,
                    version: 0,
                }
            } else {
                vp = ptrs[i].encode();
                ValueStruct {
                    value: &vp,
                    meta: e.meta | BIT_VALUE_POINTER,
                    user_meta: e.user_meta,
                    expires_at: e.expires_at,
                    version: 0,
                }
            };
            let vlen = if inline { 0 } else { ptrs[i].len };
            self.add(&e.key, vs, vlen)?;
        }
        Ok(())
    }

    fn add(&mut self, key: &[u8], vs: ValueStruct<'_>, vlen: u32) -> Result<()> {
        if !self.last_key.is_empty() && compare_keys(key, &self.last_key).is_le() {
            return Err(Error::InvalidRequest).with_context(|| {
                format!(
                    "keys not in sorted order (last key: {:?}, key: {:?})",
                    self.last_key, key
                )
            });
        }
        // A key's versions stay in one table.
        if !same_key(key, &self.last_key) && self.builder.reached_capacity() {
            let full = std::mem::replace(&mut self.builder, TableBuilder::new(self.opts.clone()));
            self.create_table(full)?;
        }
        self.last_key.clear();
        self.last_key.extend_from_slice(key);
        self.builder.add(key, vs, vlen);
        Ok(())
    }

    /// Writes the last table (Go's `Done`).
    fn done(mut self) -> Result<()> {
        if self.builder.is_empty() {
            return Ok(());
        }
        let last = std::mem::replace(&mut self.builder, TableBuilder::new(self.opts.clone()));
        self.create_table(last)
    }

    /// Writes `builder` as a table at this writer's level and records it in
    /// the manifest (Go's `createTable`).
    fn create_table(&self, builder: TableBuilder) -> Result<()> {
        if builder.is_empty() {
            return Ok(());
        }
        let inner = &self.db.inner;
        let id = inner.lc.reserve_file_id();
        let table: Arc<Table> = if inner.opt.in_memory {
            Table::open_in_memory(builder.finish()?, id, self.opts.clone())?
        } else {
            Table::create(new_filename(id, &inner.opt.dir), builder)?
        };
        let change = new_create_change(
            table.id(),
            self.level as u32,
            table.key_id(),
            table.compression_type(),
        );
        inner.manifest.add_changes(vec![change])?;
        log::info!(
            "Table created: {id} at level: {} for stream: {}. Size: {} bytes",
            self.level,
            self.stream_id,
            table.size()
        );
        inner.lc.levels[self.level].add_table(table);
        Ok(())
    }
}

#[cfg(all(test, not(miri)))]
mod tests;
