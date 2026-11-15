//! [`ValueStruct`]: the value half of an LSM entry, as stored in memtables and
//! SSTables. Port of `y.ValueStruct` from Go's `y/iterator.go`.
//!
//! Encoded layout:
//! ```text
//! +------+----------+---------------------+-------------+
//! | meta | userMeta | expiresAt (uvarint) | value bytes |
//! +------+----------+---------------------+-------------+
//! ```
//! The value has no length prefix: it runs to the end of the slice, so the
//! caller must know where the encoded value ends.

use super::varint::{MAX_VARINT_LEN64, put_uvarint, size_uvarint, uvarint};
use crate::error::{Error, Result};

/// A value plus its metadata. `value` borrows from wherever it was decoded
/// (a block, the skiplist arena, ...), like Go's slice aliasing.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ValueStruct<'a> {
    /// Internal bits (`BIT_DELETE`, `BIT_VALUE_POINTER`, ...).
    pub meta: u8,
    /// Opaque byte owned by the application.
    pub user_meta: u8,
    /// Unix time in seconds after which the entry is expired; 0 = never.
    pub expires_at: u64,
    pub value: &'a [u8],
    /// Version of the key. Not encoded; filled in by readers.
    pub version: u64,
}

impl<'a> ValueStruct<'a> {
    /// Size in bytes of [`ValueStruct::encode`]'s output.
    pub fn encoded_size(&self) -> u32 {
        (2 + size_uvarint(self.expires_at) + self.value.len()) as u32
    }

    /// Encodes into `buf`, which must hold at least `encoded_size()` bytes.
    /// Returns the number of bytes written.
    pub fn encode(&self, buf: &mut [u8]) -> u32 {
        buf[0] = self.meta;
        buf[1] = self.user_meta;
        let n = put_uvarint(&mut buf[2..], self.expires_at);
        buf[2 + n..2 + n + self.value.len()].copy_from_slice(self.value);
        (2 + n + self.value.len()) as u32
    }

    /// Appends the encoding to `out` (Go's `EncodeTo`).
    pub fn encode_to(&self, out: &mut Vec<u8>) {
        out.push(self.meta);
        out.push(self.user_meta);
        let mut enc = [0u8; MAX_VARINT_LEN64];
        let n = put_uvarint(&mut enc, self.expires_at);
        out.extend_from_slice(&enc[..n]);
        out.extend_from_slice(self.value);
    }

    /// Decodes `buf`; the value is everything after the expiry varint.
    pub fn decode(buf: &'a [u8]) -> Result<Self> {
        if buf.len() < 3 {
            return Err(Error::corruption(format!(
                "value struct too short: {} bytes",
                buf.len()
            )));
        }
        let (expires_at, n) = uvarint(&buf[2..])
            .ok_or_else(|| Error::corruption("value struct: invalid expiresAt varint"))?;
        Ok(ValueStruct {
            meta: buf[0],
            user_meta: buf[1],
            expires_at,
            value: &buf[2 + n..],
            version: 0,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encoded_size_matches_encoding() {
        // Port of Go's TestEncodedSize.
        let value = b"abcdefghij";
        for expires_at in [0, 1, 127, 128, 1 << 32, u64::MAX] {
            let v = ValueStruct {
                meta: 1,
                user_meta: 2,
                expires_at,
                value,
                version: 0,
            };
            let mut buf = vec![0u8; v.encoded_size() as usize];
            assert_eq!(v.encode(&mut buf), v.encoded_size());
            let mut via_vec = Vec::new();
            v.encode_to(&mut via_vec);
            assert_eq!(buf, via_vec);
            assert_eq!(ValueStruct::decode(&buf).unwrap(), v);
        }
    }

    #[test]
    fn decode_rejects_short_input() {
        assert!(ValueStruct::decode(&[1, 2]).is_err());
        assert!(ValueStruct::decode(&[1, 2, 0x80]).is_err());
    }
}
