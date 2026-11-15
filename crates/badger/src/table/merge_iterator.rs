//! Merges sorted iterators into one. Port of Go's `table/merge_iterator.go`.
//!
//! N iterators become a binary tree of two-way merges. Each two-way merge
//! tracks which side holds the smaller current key. When both sides are at
//! the same internal key (same user key *and* version), the left side wins and
//! the right side skips it, so earlier iterators take precedence. The DB passes
//! memtables before tables, newest first.

use crate::util::iterator::KvIterator;
use crate::util::keys::compare_keys;
use crate::util::value::ValueStruct;

type Boxed<'a> = Box<dyn KvIterator + 'a>;

struct Node<'a> {
    valid: bool,
    iter: Boxed<'a>,
}

impl Node<'_> {
    fn set_valid(&mut self) {
        self.valid = self.iter.valid();
    }
    fn next(&mut self) {
        self.iter.next();
        self.set_valid();
    }
    fn rewind(&mut self) {
        self.iter.rewind();
        self.set_valid();
    }
    fn seek(&mut self, key: &[u8]) {
        self.iter.seek(key);
        self.set_valid();
    }
    fn key(&self) -> &[u8] {
        self.iter.key()
    }
}

/// A two-way merge of iterators. Build with [`MergeIterator::merge`].
pub struct MergeIterator<'a> {
    left: Node<'a>,
    right: Node<'a>,
    small_is_left: bool,
    /// Copy of the current key, to skip duplicates of it in `next`.
    cur_key: Vec<u8>,
    reverse: bool,
}

impl<'a> MergeIterator<'a> {
    /// Merges `iters` (all going in the same direction as `reverse`). Returns
    /// `None` for no iterators and the iterator itself for one.
    pub fn merge(mut iters: Vec<Boxed<'a>>, reverse: bool) -> Option<Boxed<'a>> {
        match iters.len() {
            0 => None,
            1 => iters.pop(),
            2 => {
                let right = iters.pop().unwrap();
                let left = iters.pop().unwrap();
                Some(Box::new(MergeIterator {
                    left: Node {
                        valid: false,
                        iter: left,
                    },
                    right: Node {
                        valid: false,
                        iter: right,
                    },
                    small_is_left: true,
                    cur_key: Vec::new(),
                    reverse,
                }))
            }
            n => {
                let right = iters.split_off(n / 2);
                let left = Self::merge(iters, reverse).unwrap();
                let right = Self::merge(right, reverse).unwrap();
                Self::merge(vec![left, right], reverse)
            }
        }
    }

    fn small(&self) -> &Node<'a> {
        if self.small_is_left {
            &self.left
        } else {
            &self.right
        }
    }

    fn small_mut(&mut self) -> &mut Node<'a> {
        if self.small_is_left {
            &mut self.left
        } else {
            &mut self.right
        }
    }

    fn bigger(&self) -> &Node<'a> {
        if self.small_is_left {
            &self.right
        } else {
            &self.left
        }
    }

    /// Re-establishes which side is smaller after one side moved.
    fn fix(&mut self) {
        if !self.bigger().valid {
            return;
        }
        if !self.small().valid {
            self.small_is_left = !self.small_is_left;
            return;
        }
        match compare_keys(self.small().key(), self.bigger().key()) {
            std::cmp::Ordering::Equal => {
                // Same key and version: the left one wins, skip the right one.
                self.right.next();
                if !self.small_is_left {
                    self.small_is_left = true;
                }
            }
            std::cmp::Ordering::Less => {
                if self.reverse {
                    self.small_is_left = !self.small_is_left;
                }
            }
            std::cmp::Ordering::Greater => {
                if !self.reverse {
                    self.small_is_left = !self.small_is_left;
                }
            }
        }
    }

    fn set_current(&mut self) {
        self.cur_key.clear();
        if self.small().valid {
            let key = self.small().key().to_vec();
            self.cur_key = key;
        }
    }
}

impl KvIterator for MergeIterator<'_> {
    fn next(&mut self) {
        while self.valid() && self.small().key() == self.cur_key.as_slice() {
            self.small_mut().next();
            self.fix();
        }
        self.set_current();
    }

    fn rewind(&mut self) {
        self.left.rewind();
        self.right.rewind();
        self.fix();
        self.set_current();
    }

    fn seek(&mut self, key: &[u8]) {
        self.left.seek(key);
        self.right.seek(key);
        self.fix();
        self.set_current();
    }

    fn key(&self) -> &[u8] {
        self.small().key()
    }

    fn value(&self) -> ValueStruct<'_> {
        self.small().iter.value()
    }

    fn valid(&self) -> bool {
        self.small().valid
    }
}
