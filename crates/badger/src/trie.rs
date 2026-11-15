//! A prefix trie mapping key prefixes (optionally with "holes") to subscriber
//! ids. Used by `Subscribe` to find which subscribers a written key matches.
//! Port of Go Badger's `trie/trie.go`.

use std::collections::{HashMap, HashSet};

use crate::error::{Error, Result};
use crate::pb::Match;

#[derive(Debug, Default)]
struct Node {
    children: HashMap<u8, Node>,
    /// Child for a position whose byte is ignored (matches any byte).
    ignore: Option<Box<Node>>,
    ids: Vec<u64>,
}

impl Node {
    fn is_empty(&self) -> bool {
        self.children.is_empty() && self.ids.is_empty() && self.ignore.is_none()
    }

    #[cfg(test)]
    fn num_nodes(&self) -> usize {
        1 + self.ignore.as_ref().map_or(0, |n| n.num_nodes())
            + self.children.values().map(Node::num_nodes).sum::<usize>()
    }
}

#[derive(Debug, Default)]
pub struct Trie {
    root: Node,
}

enum Op {
    Set,
    Del,
}

impl Trie {
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers `id` for keys starting with `prefix`.
    pub fn add(&mut self, prefix: &[u8], id: u64) {
        let m = Match {
            prefix: prefix.to_vec(),
            ignore_bytes: String::new(),
        };
        self.add_match(&m, id)
            .expect("an empty ignore list always parses");
    }

    /// Registers `id` for a prefix with holes. `m.ignore_bytes` lists positions
    /// that match any byte, e.g. `"3, 5-8, 10"`. With prefix `"aaaa"` and
    /// ignore bytes `"0, 2"`, the key `"baba..."` matches too.
    pub fn add_match(&mut self, m: &Match, id: u64) -> Result<()> {
        self.fix(m, id, Op::Set)
    }

    /// Ids of every registered prefix that `key` starts with.
    pub fn get(&self, key: &[u8]) -> HashSet<u64> {
        let mut out = HashSet::new();
        get(&self.root, key, &mut out);
        out
    }

    pub fn delete(&mut self, prefix: &[u8], id: u64) -> Result<()> {
        self.delete_match(
            &Match {
                prefix: prefix.to_vec(),
                ignore_bytes: String::new(),
            },
            id,
        )
    }

    pub fn delete_match(&mut self, m: &Match, id: u64) -> Result<()> {
        self.fix(m, id, Op::Del)?;
        // Prune empty nodes, but keep the root.
        remove_empty(&mut self.root);
        Ok(())
    }

    fn fix(&mut self, m: &Match, id: u64, op: Op) -> Result<()> {
        let mut ignore = parse_ignore_bytes(&m.ignore_bytes).map_err(|e| Error::Context {
            context: format!("while parsing ignore bytes: {}", m.ignore_bytes),
            source: Box::new(e),
        })?;
        if ignore.len() < m.prefix.len() {
            ignore.resize(m.prefix.len(), false);
        }

        let deleting = matches!(op, Op::Del);
        let mut cur = &mut self.root;
        for (idx, &byte) in m.prefix.iter().enumerate() {
            cur = if ignore[idx] {
                if deleting && cur.ignore.is_none() {
                    return Ok(()); // nothing to delete
                }
                cur.ignore.get_or_insert_with(Box::default).as_mut()
            } else {
                if deleting && !cur.children.contains_key(&byte) {
                    return Ok(());
                }
                cur.children.entry(byte).or_default()
            };
        }

        match op {
            Op::Set => cur.ids.push(id),
            Op::Del => cur.ids.retain(|&cid| cid != id),
        }
        Ok(())
    }

    #[cfg(test)]
    fn num_nodes(&self) -> usize {
        self.root.num_nodes()
    }
}

fn get(node: &Node, key: &[u8], out: &mut HashSet<u64>) {
    // Every node on the key's path contributes its ids (this also matches the
    // empty prefix at the root).
    out.extend(&node.ids);
    let Some((&first, rest)) = key.split_first() else {
        return;
    };
    if let Some(ignore) = &node.ignore {
        get(ignore, rest, out);
    }
    if let Some(child) = node.children.get(&first) {
        get(child, rest, out);
    }
}

/// Removes empty descendants; returns whether `node` itself is now empty.
fn remove_empty(node: &mut Node) -> bool {
    if node.ignore.as_deref_mut().is_some_and(remove_empty) {
        node.ignore = None;
    }
    node.children.retain(|_, child| !remove_empty(child));
    node.is_empty()
}

/// Parses `"0, 3-5, 7"` into `[true, false, false, true, true, true, false, true]`.
fn parse_ignore_bytes(ig: &str) -> Result<Vec<bool>> {
    let mut out = Vec::new();
    if ig.is_empty() {
        return Ok(out);
    }
    let invalid = |part: &str| Error::InvalidOptions(format!("Invalid range: {part}"));
    let number = |s: &str| {
        s.trim()
            .parse::<usize>()
            .map_err(|e| Error::InvalidOptions(e.to_string()))
    };

    for part in ig.trim().split(',') {
        let bounds: Vec<&str> = part.trim().split('-').collect();
        let (start, end) = match bounds.as_slice() {
            [single] => (number(single)?, None),
            [start, end] => (number(start)?, Some(number(end)?)),
            _ => return Err(invalid(part)),
        };
        let last = end.unwrap_or(start).max(start);
        if last >= out.len() {
            out.resize(last + 1, false);
        }
        match end {
            None => out[start] = true,
            Some(end) => out
                .iter_mut()
                .take(end + 1)
                .skip(start)
                .for_each(|b| *b = true),
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn set(ids: &[u64]) -> HashSet<u64> {
        ids.iter().copied().collect()
    }

    // Ports of Go's trie_test.go.
    #[test]
    fn get_ids() {
        let mut t = Trie::new();
        t.add(b"hello", 1);
        t.add(b"hello", 3);
        t.add(b"hello", 4);
        t.add(b"hel", 20);
        t.add(b"he", 20);
        t.add(b"badger", 30);

        t.add(b"", 10);
        assert_eq!(t.get(b"A"), set(&[10]));
        assert_eq!(t.get(b"hel"), set(&[10, 20]));
        assert_eq!(t.get(b"badger"), set(&[10, 30]));
        assert_eq!(t.get(b"hello"), set(&[10, 1, 3, 4, 20]));

        t.add(&[], 11);
        assert_eq!(t.get(b"A"), set(&[10, 11]));
    }

    #[test]
    fn delete() {
        let mut t = Trie::new();
        assert_eq!(t.num_nodes(), 1);
        t.add(b"hello", 1);
        t.add(b"hello", 3);
        t.add(b"hello", 4);
        t.add(b"", 5);

        t.delete(b"hello", 4).unwrap();
        assert_eq!(t.get(b"hello"), set(&[5, 1, 3]));
        t.delete(b"", 5).unwrap();
        assert_eq!(t.get(b"hello"), set(&[1, 3]));

        for id in [1, 3, 4, 5, 6] {
            t.delete(b"hello", id).unwrap();
        }
        assert_eq!(t.num_nodes(), 1);
        assert!(t.root.is_empty());
        assert_eq!(t.get(b"hello"), set(&[]));
    }

    #[test]
    fn parse_ignore() {
        assert_eq!(parse_ignore_bytes("1").unwrap(), [false, true]);
        assert_eq!(parse_ignore_bytes("0").unwrap(), [true]);
        assert_eq!(
            parse_ignore_bytes("0, 3 - 5, 7").unwrap(),
            [true, false, false, true, true, true, false, true]
        );
        assert!(parse_ignore_bytes("1-2-3").is_err());
        assert!(parse_ignore_bytes("x").is_err());
    }

    #[test]
    fn prefix_match_with_holes() {
        let mut t = Trie::new();
        let m = |prefix: &str, ignore: &str| Match {
            prefix: prefix.as_bytes().to_vec(),
            ignore_bytes: ignore.to_string(),
        };
        t.add_match(&m("", ""), 1).unwrap();
        t.add_match(&m("aaaa", ""), 2).unwrap();
        t.add_match(&m("aaaaaa", "2-10"), 3).unwrap();
        t.add_match(&m("aaaaaaaaa", "0, 4 - 6, 8"), 4).unwrap();

        let get = |t: &Trie, k: &str| {
            let mut ids: Vec<u64> = t.get(k.as_bytes()).into_iter().collect();
            ids.sort();
            ids
        };
        assert_eq!(get(&t, ""), [1]);
        assert_eq!(get(&t, "aax"), [1]);
        assert_eq!(get(&t, "aaaaa"), [1, 2], "too short to match 3");
        assert_eq!(get(&t, "aabbbbbbbb"), [1, 3]);
        assert_eq!(get(&t, "baaabbbabba"), [1, 4]);
        assert_eq!(get(&t, "aaaabbbabba"), [1, 2, 3, 4]);

        t.delete_match(&m("aaaaaaaaa", "0, 4 - 6, 8"), 5).unwrap();
        t.delete_match(&m("aaaaaaaaa", "0, 4 - 6, 8"), 4).unwrap();
        t.delete_match(&m("aaaaaa", "2-10"), 3).unwrap();
        t.delete_match(&m("aaaa", ""), 2).unwrap();
        t.delete_match(&m("", ""), 1).unwrap();
        t.delete_match(&m("abracadabra", ""), 4).unwrap();
        assert_eq!(t.num_nodes(), 1);
    }
}
