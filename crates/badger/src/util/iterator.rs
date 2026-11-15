//! [`KvIterator`]: the internal iterator interface shared by table, concat,
//! merge (and later memtable) iterators. Port of Go's `y.Iterator`.
//!
//! Keys are internal keys (user key + version). Go's `Close` is `Drop` here.

use super::value::ValueStruct;

pub trait KvIterator {
    /// Moves to the next entry (the previous one for reversed iterators).
    fn next(&mut self);
    /// Moves to the first entry (the last one for reversed iterators).
    fn rewind(&mut self);
    /// Moves to the first entry at or after `key` (at or before it, if reversed).
    fn seek(&mut self, key: &[u8]);
    /// The current internal key. Only meaningful while [`KvIterator::valid`].
    fn key(&self) -> &[u8];
    /// The current value. Only meaningful while [`KvIterator::valid`].
    fn value(&self) -> ValueStruct<'_>;
    fn valid(&self) -> bool;
}

impl<I: KvIterator + ?Sized> KvIterator for Box<I> {
    fn next(&mut self) {
        (**self).next()
    }
    fn rewind(&mut self) {
        (**self).rewind()
    }
    fn seek(&mut self, key: &[u8]) {
        (**self).seek(key)
    }
    fn key(&self) -> &[u8] {
        (**self).key()
    }
    fn value(&self) -> ValueStruct<'_> {
        (**self).value()
    }
    fn valid(&self) -> bool {
        (**self).valid()
    }
}
