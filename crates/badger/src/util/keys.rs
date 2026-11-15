//! Internal keys: a user key followed by an 8-byte version (timestamp).
//!
//! The version is stored as big-endian `u64::MAX - version`, so for the same
//! user key, newer versions sort first under plain byte comparison of the
//! suffix. Port of `KeyWithTs`, `ParseTs`, `ParseKey`, `CompareKeys` and
//! `SameKey` from Go's `y/y.go`.

use std::cmp::Ordering;

/// Length of the version suffix of an internal key.
pub const TS_SIZE: usize = 8;

/// Appends the encoded version to `key`, returning a new internal key.
pub fn key_with_ts(key: &[u8], ts: u64) -> Vec<u8> {
    let mut out = Vec::with_capacity(key.len() + TS_SIZE);
    append_key_with_ts(&mut out, key, ts);
    out
}

/// Like [`key_with_ts`], but appends to an existing buffer.
pub fn append_key_with_ts(out: &mut Vec<u8>, key: &[u8], ts: u64) {
    out.extend_from_slice(key);
    out.extend_from_slice(&(u64::MAX - ts).to_be_bytes());
}

/// The version of an internal key, or 0 if the key is too short to have one.
///
/// Go returns 0 when `len(key) <= 8`, so a bare 8-byte suffix with an empty user
/// key also parses as 0. This keeps that behaviour.
pub fn parse_ts(key: &[u8]) -> u64 {
    if key.len() <= TS_SIZE {
        return 0;
    }
    let suffix: [u8; TS_SIZE] = key[key.len() - TS_SIZE..].try_into().unwrap();
    u64::MAX - u64::from_be_bytes(suffix)
}

/// The user key part of an internal key (empty if `key` is shorter than 8 bytes).
pub fn parse_key(key: &[u8]) -> &[u8] {
    if key.len() < TS_SIZE {
        return &[];
    }
    &key[..key.len() - TS_SIZE]
}

/// Orders internal keys by user key, then by version (newest first).
///
/// Comparing whole keys bytewise would be wrong: `a<ts>` must sort before
/// `aa<ts>`, but its suffix bytes can be larger than the second `a`.
///
/// # Panics
/// If either key is shorter than 8 bytes (Go panics too).
pub fn compare_keys(a: &[u8], b: &[u8]) -> Ordering {
    let (ak, at) = a.split_at(a.len() - TS_SIZE);
    let (bk, bt) = b.split_at(b.len() - TS_SIZE);
    ak.cmp(bk).then_with(|| at.cmp(bt))
}

/// Whether two internal keys have the same user key, ignoring versions.
pub fn same_key(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && parse_key(a) == parse_key(b)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip() {
        let k = key_with_ts(b"key", 42);
        assert_eq!(parse_key(&k), b"key");
        assert_eq!(parse_ts(&k), 42);
        assert_eq!(parse_ts(b"short"), 0);
        assert_eq!(parse_key(b"short"), b"");
    }

    #[test]
    fn newer_versions_sort_first() {
        let newer = key_with_ts(b"a", 2);
        let older = key_with_ts(b"a", 1);
        assert_eq!(compare_keys(&newer, &older), Ordering::Less);
        assert_eq!(
            compare_keys(&key_with_ts(b"a", 1), &key_with_ts(b"aa", 9)),
            Ordering::Less
        );
        assert!(same_key(&newer, &older));
        assert!(!same_key(&newer, &key_with_ts(b"b", 2)));
    }
}
