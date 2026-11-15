//! Value pointers, value-log entry headers, entry meta bits and [`Entry`].
//! Port of Go Badger's `structs.go` (and the meta bits from `value.go`).

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::error::{Error, Result};
use crate::util::varint::{put_uvarint, uvarint};

// Bits of the internal meta byte (Go: value.go).
/// The key has been deleted.
pub const BIT_DELETE: u8 = 1 << 0;
/// The value is a [`ValuePointer`] into the value log, not the value itself.
pub const BIT_VALUE_POINTER: u8 = 1 << 1;
/// Earlier versions of the key can be discarded by compaction.
pub const BIT_DISCARD_EARLIER_VERSIONS: u8 = 1 << 2;
/// The value is a merge operand (merge operator).
pub const BIT_MERGE_ENTRY: u8 = 1 << 3;
/// The entry is part of a transaction (value log).
pub const BIT_TXN: u8 = 1 << 6;
/// The entry marks the end of a transaction (value log).
pub const BIT_FIN_TXN: u8 = 1 << 7;

/// Location of a value in the value log.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct ValuePointer {
    pub fid: u32,
    pub len: u32,
    pub offset: u32,
}

impl ValuePointer {
    /// Encoded size in bytes.
    pub const SIZE: usize = 12;

    /// Go copies the struct's memory, so the fields are written in declaration
    /// order (fid, len, offset) in the machine's byte order. Badger only runs on
    /// little-endian machines in practice, so the format is little-endian.
    pub fn encode(&self) -> [u8; Self::SIZE] {
        let mut b = [0u8; Self::SIZE];
        b[0..4].copy_from_slice(&self.fid.to_le_bytes());
        b[4..8].copy_from_slice(&self.len.to_le_bytes());
        b[8..12].copy_from_slice(&self.offset.to_le_bytes());
        b
    }

    pub fn decode(b: &[u8]) -> Result<Self> {
        if b.len() < Self::SIZE {
            return Err(Error::corruption(format!(
                "value pointer too short: {} bytes",
                b.len()
            )));
        }
        let u32_at = |i: usize| u32::from_le_bytes(b[i..i + 4].try_into().unwrap());
        Ok(ValuePointer {
            fid: u32_at(0),
            len: u32_at(4),
            offset: u32_at(8),
        })
    }

    pub fn is_zero(&self) -> bool {
        self.fid == 0 && self.offset == 0 && self.len == 0
    }
}

impl PartialOrd for ValuePointer {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// Orders by fid, then offset, then len (Go's `Less`).
impl Ord for ValuePointer {
    fn cmp(&self, o: &Self) -> std::cmp::Ordering {
        (self.fid, self.offset, self.len).cmp(&(o.fid, o.offset, o.len))
    }
}

/// Header written before each entry in the value log and the memtable WAL.
///
/// ```text
/// +------+----------+------------+--------------+-----------+
/// | Meta | UserMeta | Key Length | Value Length | ExpiresAt |
/// +------+----------+------------+--------------+-----------+
///   1 B      1 B       uvarint        uvarint       uvarint
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Header {
    pub klen: u32,
    pub vlen: u32,
    pub expires_at: u64,
    pub meta: u8,
    pub user_meta: u8,
}

impl Header {
    /// Maximum encoded size: 2 bytes + two 5-byte and one 10-byte varint.
    pub const MAX_SIZE: usize = 22;

    /// Encodes into `out` (at least [`Header::MAX_SIZE`] bytes to be safe) and
    /// returns the number of bytes written.
    pub fn encode(&self, out: &mut [u8]) -> usize {
        out[0] = self.meta;
        out[1] = self.user_meta;
        let mut i = 2;
        i += put_uvarint(&mut out[i..], self.klen as u64);
        i += put_uvarint(&mut out[i..], self.vlen as u64);
        i += put_uvarint(&mut out[i..], self.expires_at);
        i
    }

    /// Decodes a header from the start of `buf`, returning it and its length.
    pub fn decode(buf: &[u8]) -> Result<(Header, usize)> {
        let bad = || Error::corruption("truncated or invalid entry header");
        if buf.len() < 2 {
            return Err(bad());
        }
        let mut i = 2;
        let next = |i: &mut usize| -> Result<u64> {
            let (v, n) = uvarint(&buf[*i..]).ok_or_else(bad)?;
            *i += n;
            Ok(v)
        };
        // Go truncates klen/vlen to uint32 the same way.
        let klen = next(&mut i)? as u32;
        let vlen = next(&mut i)? as u32;
        let expires_at = next(&mut i)?;
        Ok((
            Header {
                klen,
                vlen,
                expires_at,
                meta: buf[0],
                user_meta: buf[1],
            },
            i,
        ))
    }
}

/// A key-value pair to write, with optional metadata.
///
/// Build one with [`Entry::new`] and the `with_*` methods, then pass it to a
/// transaction. Unlike Go, the entry owns its key and value.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Entry {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    /// Unix time in seconds after which the entry expires; 0 = never.
    pub expires_at: u64,
    /// Opaque byte owned by the application.
    pub user_meta: u8,
    pub(crate) meta: u8,
    pub(crate) version: u64,
    /// Offset in the value log (internal).
    pub(crate) offset: u32,
    /// Length of the encoded header (internal).
    pub(crate) hlen: usize,
    pub(crate) val_threshold: i64,
}

impl Entry {
    pub fn new(key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) -> Self {
        Entry {
            key: key.into(),
            value: value.into(),
            ..Default::default()
        }
    }

    /// Sets the user meta byte. (Go names this `WithMeta`, though it sets `UserMeta`.)
    pub fn with_meta(mut self, user_meta: u8) -> Self {
        self.user_meta = user_meta;
        self
    }

    /// Marks earlier versions of this key as eligible for garbage collection.
    /// Only useful when `num_versions_to_keep` is above 1.
    pub fn with_discard(mut self) -> Self {
        self.meta = BIT_DISCARD_EARLIER_VERSIONS;
        self
    }

    /// Expires the entry `ttl` from now.
    pub fn with_ttl(mut self, ttl: Duration) -> Self {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default();
        self.expires_at = (now + ttl).as_secs();
        self
    }

    #[allow(dead_code)] // used by the write path (Phase 5)
    pub(crate) fn with_merge_bit(mut self) -> Self {
        self.meta = BIT_MERGE_ENTRY;
        self
    }

    #[allow(dead_code)] // used by the write path (Phase 5)
    pub(crate) fn is_zero(&self) -> bool {
        self.key.is_empty()
    }

    /// Approximate size of the entry in the LSM tree: the value itself if it
    /// stays inline, else a 12-byte value pointer.
    #[allow(dead_code)] // used by the write path (Phase 5)
    pub(crate) fn estimate_size_and_set_threshold(&mut self, threshold: i64) -> i64 {
        if self.val_threshold == 0 {
            self.val_threshold = threshold;
        }
        let k = self.key.len() as i64;
        let v = self.value.len() as i64;
        if v < self.val_threshold {
            k + v + 2 // meta, user meta
        } else {
            k + ValuePointer::SIZE as i64 + 2
        }
    }

    /// Whether the value is small enough to stay inline (not in the value log).
    #[allow(dead_code)] // used by the write path (Phase 5)
    pub(crate) fn skip_vlog_and_set_threshold(&mut self, threshold: i64) -> bool {
        if self.val_threshold == 0 {
            self.val_threshold = threshold;
        }
        (self.value.len() as i64) < self.val_threshold
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn value_pointer_round_trip_and_order() {
        let p = ValuePointer {
            fid: 1,
            len: 0x0203,
            offset: 0x04050607,
        };
        let b = p.encode();
        assert_eq!(b, [1, 0, 0, 0, 3, 2, 0, 0, 7, 6, 5, 4]);
        assert_eq!(ValuePointer::decode(&b).unwrap(), p);
        assert!(ValuePointer::default().is_zero());
        let q = ValuePointer {
            fid: 1,
            len: 1,
            offset: 0x04050608,
        };
        assert!(p < q, "offset is compared before len");
    }

    #[test]
    fn header_round_trip() {
        let h = Header {
            klen: u32::MAX,
            vlen: u32::MAX,
            expires_at: u64::MAX,
            meta: 0x42,
            user_meta: 7,
        };
        let mut buf = [0u8; Header::MAX_SIZE];
        let n = h.encode(&mut buf);
        assert_eq!(n, Header::MAX_SIZE, "worst case uses every byte");
        assert_eq!(Header::decode(&buf[..n]).unwrap(), (h, n));
        assert!(Header::decode(&buf[..n - 1]).is_err());
    }

    #[test]
    fn entry_builders() {
        let e = Entry::new("k", "v").with_meta(3).with_discard();
        assert_eq!((e.user_meta, e.meta), (3, BIT_DISCARD_EARLIER_VERSIONS));
        assert!(
            Entry::new("k", "v")
                .with_ttl(Duration::from_secs(60))
                .expires_at
                > 0
        );

        let mut big = Entry::new("key", vec![0; 100]);
        assert_eq!(big.estimate_size_and_set_threshold(64), 3 + 12 + 2);
        assert!(
            !big.skip_vlog_and_set_threshold(1000),
            "threshold is set once"
        );
    }
}
