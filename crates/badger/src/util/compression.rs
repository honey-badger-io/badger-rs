//! Block compression: Snappy (raw block format) and Zstandard (frames).
//!
//! Go Badger compresses with klauspost's `s2.EncodeSnappy` and `zstd`. The
//! compressed bytes from Rust's `snap` / `zstd` crates differ, but the formats
//! are standard, so each side reads the other's blocks.

use crate::error::{Error, Result};
use crate::options::CompressionType;

/// Compresses `data`. `None` returns a copy.
pub fn compress(ctype: CompressionType, data: &[u8], zstd_level: i32) -> Result<Vec<u8>> {
    match ctype {
        CompressionType::None => Ok(data.to_vec()),
        CompressionType::Snappy => snap::raw::Encoder::new()
            .compress_vec(data)
            .map_err(|e| Error::corruption(format!("snappy compress: {e}"))),
        CompressionType::Zstd => Ok(zstd::bulk::compress(data, zstd_level)?),
    }
}

/// Decompresses `data` that was compressed with `ctype`.
pub fn decompress(ctype: CompressionType, data: &[u8]) -> Result<Vec<u8>> {
    match ctype {
        CompressionType::None => Ok(data.to_vec()),
        CompressionType::Snappy => snap::raw::Decoder::new()
            .decompress_vec(data)
            .map_err(|e| Error::corruption(format!("failed to decompress (snappy): {e}"))),
        CompressionType::Zstd => zstd::stream::decode_all(data)
            .map_err(|e| Error::corruption(format!("failed to decompress (zstd): {e}"))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[cfg_attr(miri, ignore)] // zstd is C code
    fn round_trip() {
        let data: Vec<u8> = b"badger ".iter().copied().cycle().take(10_000).collect();
        for ctype in [
            CompressionType::None,
            CompressionType::Snappy,
            CompressionType::Zstd,
        ] {
            let c = compress(ctype, &data, 1).unwrap();
            if ctype != CompressionType::None {
                assert!(
                    c.len() < data.len() / 10,
                    "{ctype:?} should compress repetitive data"
                );
            }
            assert_eq!(decompress(ctype, &c).unwrap(), data);
        }
    }

    #[test]
    #[cfg_attr(miri, ignore)] // zstd is C code
    fn wrong_algorithm_is_an_error() {
        let zstd = compress(CompressionType::Zstd, b"hello hello hello", 1).unwrap();
        assert!(decompress(CompressionType::Snappy, &zstd).is_err());
        assert!(decompress(CompressionType::Zstd, b"not zstd").is_err());
    }
}
