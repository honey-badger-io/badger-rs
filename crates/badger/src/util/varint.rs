//! Unsigned LEB128 varints, byte-compatible with Go's `encoding/binary`
//! `PutUvarint` / `Uvarint`.

/// Maximum encoded length of a `u64`.
pub const MAX_VARINT_LEN64: usize = 10;

/// Number of bytes needed to encode `x` (Go's `sizeVarint` in `y/iterator.go`).
pub fn size_uvarint(mut x: u64) -> usize {
    let mut n = 1;
    while x >= 0x80 {
        x >>= 7;
        n += 1;
    }
    n
}

/// Encodes `x` into `buf`, returning the number of bytes written.
///
/// # Panics
/// If `buf` is too small (Go panics too).
pub fn put_uvarint(buf: &mut [u8], mut x: u64) -> usize {
    let mut i = 0;
    while x >= 0x80 {
        buf[i] = x as u8 | 0x80;
        x >>= 7;
        i += 1;
    }
    buf[i] = x as u8;
    i + 1
}

/// Appends the encoding of `x` to `out`.
pub fn append_uvarint(out: &mut Vec<u8>, x: u64) {
    let mut buf = [0u8; MAX_VARINT_LEN64];
    let n = put_uvarint(&mut buf, x);
    out.extend_from_slice(&buf[..n]);
}

/// Decodes a varint from the start of `buf`, returning the value and the
/// number of bytes read. Returns `None` if `buf` ends mid-varint or the value
/// overflows 64 bits (where Go's `Uvarint` returns `n <= 0`).
pub fn uvarint(buf: &[u8]) -> Option<(u64, usize)> {
    let mut x: u64 = 0;
    let mut shift = 0u32;
    for (i, &b) in buf.iter().enumerate() {
        if i == MAX_VARINT_LEN64 {
            return None; // overflow
        }
        if b < 0x80 {
            if i == MAX_VARINT_LEN64 - 1 && b > 1 {
                return None; // overflow
            }
            return Some((x | (b as u64) << shift, i + 1));
        }
        x |= ((b & 0x7f) as u64) << shift;
        shift += 7;
    }
    None // buffer too small
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_and_sizes() {
        for x in [0, 1, 127, 128, 300, 16383, 16384, 1 << 32, u64::MAX] {
            let mut v = Vec::new();
            append_uvarint(&mut v, x);
            assert_eq!(v.len(), size_uvarint(x), "{x}");
            assert_eq!(uvarint(&v), Some((x, v.len())));
        }
        assert_eq!(size_uvarint(0), 1); // Go: TestSizeVarintForZero
    }

    #[test]
    fn rejects_truncated_and_overflow() {
        assert_eq!(uvarint(&[]), None);
        assert_eq!(uvarint(&[0x80]), None);
        let mut too_long = [0xffu8; 10];
        too_long[9] = 0x02;
        assert_eq!(uvarint(&too_long), None);
        assert_eq!(uvarint(&[0xff; 11]), None);
    }
}
