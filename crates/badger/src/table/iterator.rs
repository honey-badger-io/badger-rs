//! Table and concat iterators. Port of Go's `table/iterator.go`.

use std::sync::Arc;

use super::Table;
use super::block::{BlockIterator, IterError};
use crate::error::Error;
use crate::util::iterator::KvIterator;
use crate::util::keys::compare_keys;
use crate::util::value::ValueStruct;

/// Iterates over one table, block by block.
#[derive(Debug)]
pub struct TableIterator {
    table: Arc<Table>,
    /// Current block index (may be -1 or num_blocks at the ends).
    bpos: isize,
    bi: BlockIterator,
    err: Option<IterError>,
    reversed: bool,
    use_cache: bool,
}

impl TableIterator {
    pub(crate) fn new(table: Arc<Table>, reversed: bool, use_cache: bool) -> Self {
        TableIterator {
            table,
            bpos: 0,
            bi: BlockIterator::default(),
            // Unlike Go, a fresh iterator is not valid until positioned.
            err: Some(IterError::Eof),
            reversed,
            use_cache,
        }
    }

    pub fn table(&self) -> &Arc<Table> {
        &self.table
    }

    /// The error that stopped iteration, if it was not simply reaching the end.
    pub fn error(&self) -> Option<&Error> {
        match &self.err {
            Some(IterError::Err(e)) => Some(e),
            _ => None,
        }
    }

    fn num_blocks(&self) -> isize {
        self.table.num_blocks() as isize
    }

    /// Loads block `idx` into the block iterator. Returns false on error.
    fn load_block(&mut self, idx: isize) -> bool {
        match self.table.block(idx as usize, self.use_cache) {
            Ok(block) => {
                self.bi.set_block(block);
                true
            }
            Err(e) => {
                self.err = Some(e.into());
                false
            }
        }
    }

    pub(crate) fn seek_to_first(&mut self) {
        self.bpos = 0;
        if self.load_block(0) {
            self.bi.seek_to_first();
            self.err = self.bi.err.clone();
        }
    }

    pub(crate) fn seek_to_last(&mut self) {
        self.bpos = self.num_blocks() - 1;
        if self.load_block(self.bpos) {
            self.bi.seek_to_last();
            self.err = self.bi.err.clone();
        }
    }

    fn seek_helper(&mut self, block_idx: isize, key: &[u8]) {
        self.bpos = block_idx;
        if self.load_block(block_idx) {
            self.bi.seek(key, false);
            self.err = self.bi.err.clone();
        }
    }

    /// Moves to the first entry with key >= `key`.
    pub(crate) fn seek_forward(&mut self, key: &[u8]) {
        self.err = None;
        self.bpos = 0;
        // The first block whose first key is > key; the target is in the block before it.
        let idx = match self.table.search_blocks(key) {
            Ok(idx) => idx as isize,
            Err(e) => {
                self.err = Some(e.into());
                return;
            }
        };
        if idx == 0 {
            self.seek_helper(0, key);
            return;
        }
        self.seek_helper(idx - 1, key);
        if matches!(self.err, Some(IterError::Eof)) && idx != self.num_blocks() {
            // Every key in block idx-1 is smaller: the answer is block idx's first key.
            self.seek_helper(idx, key);
        }
    }

    /// Moves to the last entry with key <= `key`.
    pub(crate) fn seek_for_prev(&mut self, key: &[u8]) {
        self.seek_forward(key);
        if self.bi.key != key {
            self.prev();
        }
    }

    /// Go's `next`: moves forward, crossing into the next block when needed.
    pub(crate) fn next_forward(&mut self) {
        loop {
            self.err = None;
            if self.bpos >= self.num_blocks() {
                self.err = Some(IterError::Eof);
                return;
            }
            if self.bi.block.is_none() {
                if self.load_block(self.bpos) {
                    self.bi.seek_to_first();
                    self.err = self.bi.err.clone();
                }
                return;
            }
            self.bi.next();
            match &self.bi.err {
                None => return,
                Some(IterError::Err(e)) => {
                    // Go treats this like the end of the block; surface it instead.
                    self.err = Some(IterError::Err(Arc::clone(e)));
                    return;
                }
                Some(IterError::Eof) => {
                    self.bpos += 1;
                    self.bi.block = None;
                }
            }
        }
    }

    /// Go's `prev`: moves backward, crossing into the previous block when needed.
    pub(crate) fn prev(&mut self) {
        loop {
            self.err = None;
            if self.bpos < 0 {
                self.err = Some(IterError::Eof);
                return;
            }
            if self.bi.block.is_none() {
                if self.load_block(self.bpos) {
                    self.bi.seek_to_last();
                    self.err = self.bi.err.clone();
                }
                return;
            }
            self.bi.prev();
            match &self.bi.err {
                None => return,
                Some(IterError::Err(e)) => {
                    self.err = Some(IterError::Err(Arc::clone(e)));
                    return;
                }
                Some(IterError::Eof) => {
                    self.bpos -= 1;
                    self.bi.block = None;
                }
            }
        }
    }
}

impl KvIterator for TableIterator {
    fn next(&mut self) {
        if self.reversed {
            self.prev()
        } else {
            self.next_forward()
        }
    }

    fn rewind(&mut self) {
        if self.reversed {
            self.seek_to_last()
        } else {
            self.seek_to_first()
        }
    }

    fn seek(&mut self, key: &[u8]) {
        if self.reversed {
            self.seek_for_prev(key)
        } else {
            self.seek_forward(key)
        }
    }

    fn key(&self) -> &[u8] {
        &self.bi.key
    }

    fn value(&self) -> ValueStruct<'_> {
        self.bi.value()
    }

    fn valid(&self) -> bool {
        self.err.is_none()
    }
}

/// Iterates over tables with non-overlapping key ranges, in order, opening
/// each table's iterator lazily. Port of Go's `ConcatIterator`.
#[derive(Debug)]
pub struct ConcatIterator {
    /// Ascending by key range, regardless of direction.
    tables: Vec<Arc<Table>>,
    iters: Vec<Option<TableIterator>>,
    cur: Option<usize>,
    reversed: bool,
    use_cache: bool,
}

impl ConcatIterator {
    pub fn new(tables: Vec<Arc<Table>>, reversed: bool, use_cache: bool) -> Self {
        let iters = tables.iter().map(|_| None).collect();
        ConcatIterator {
            tables,
            iters,
            cur: None,
            reversed,
            use_cache,
        }
    }

    fn set_idx(&mut self, idx: isize) {
        if idx < 0 || idx as usize >= self.iters.len() {
            self.cur = None;
            return;
        }
        let idx = idx as usize;
        if self.iters[idx].is_none() {
            self.iters[idx] = Some(self.tables[idx].iter(self.reversed, self.use_cache));
        }
        self.cur = Some(idx);
    }

    fn cur(&self) -> Option<&TableIterator> {
        self.cur.and_then(|i| self.iters[i].as_ref())
    }

    fn cur_mut(&mut self) -> Option<&mut TableIterator> {
        self.cur.and_then(|i| self.iters[i].as_mut())
    }
}

impl KvIterator for ConcatIterator {
    fn rewind(&mut self) {
        if self.iters.is_empty() {
            return;
        }
        let start = if self.reversed {
            self.iters.len() as isize - 1
        } else {
            0
        };
        self.set_idx(start);
        if let Some(it) = self.cur_mut() {
            it.rewind();
        }
    }

    fn valid(&self) -> bool {
        self.cur().is_some_and(|it| it.valid())
    }

    fn key(&self) -> &[u8] {
        self.cur().map(|it| it.key()).unwrap_or_default()
    }

    fn value(&self) -> ValueStruct<'_> {
        self.cur().map(|it| it.value()).unwrap_or_default()
    }

    fn seek(&mut self, key: &[u8]) {
        let n = self.tables.len();
        let idx = if !self.reversed {
            // First table whose biggest key is >= key.
            self.tables
                .partition_point(|t| compare_keys(t.biggest(), key).is_lt()) as isize
        } else {
            // Last table whose smallest key is <= key.
            let from_end = self
                .tables
                .iter()
                .rev()
                .take_while(|t| compare_keys(t.smallest(), key).is_gt())
                .count();
            n as isize - 1 - from_end as isize
        };
        if idx < 0 || idx as usize >= n {
            self.set_idx(-1);
            return;
        }
        self.set_idx(idx);
        if let Some(it) = self.cur_mut() {
            it.seek(key);
        }
    }

    fn next(&mut self) {
        let Some(it) = self.cur_mut() else { return };
        it.next();
        if it.valid() {
            return;
        }
        // Move on to the next table, skipping empty ones.
        loop {
            let i = self.cur.expect("checked above") as isize;
            self.set_idx(if self.reversed { i - 1 } else { i + 1 });
            let Some(it) = self.cur_mut() else { return };
            it.rewind();
            if it.valid() {
                return;
            }
        }
    }
}
