//! Block and index checksums (CRC32C or XXHash64). Port of Go's `y/checksum.go`.

use crate::error::{Error, Result};
use crate::pb::{Checksum, checksum::Algorithm};

/// Computes the checksum of `data` with `algo`.
pub fn calculate_checksum(data: &[u8], algo: Algorithm) -> u64 {
    match algo {
        Algorithm::Crc32c => crc32c::crc32c(data) as u64,
        Algorithm::XxHash64 => xxhash_rust::xxh64::xxh64(data, 0),
    }
}

/// Builds a [`Checksum`] message for `data`.
pub fn checksum_of(data: &[u8], algo: Algorithm) -> Checksum {
    Checksum {
        algo: algo as i32,
        sum: calculate_checksum(data, algo),
    }
}

/// Checks `data` against `expected`.
///
/// Go panics on an unknown algorithm; here that is an [`Error::Corruption`],
/// since the value comes from disk.
pub fn verify_checksum(data: &[u8], expected: &Checksum) -> Result<()> {
    let algo = Algorithm::try_from(expected.algo)
        .map_err(|_| Error::corruption(format!("unknown checksum algorithm {}", expected.algo)))?;
    let actual = calculate_checksum(data, algo);
    if actual != expected.sum {
        return Err(Error::ChecksumMismatch {
            actual,
            expected: expected.sum,
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    // Ports of Go's checksum_test.go.
    #[test]
    #[cfg_attr(miri, ignore)] // crc32c uses CPU intrinsics Miri cannot run
    fn crc32c_known_value() {
        // "123456789" is the standard CRC check input.
        assert_eq!(
            calculate_checksum(b"123456789", Algorithm::Crc32c),
            0xe3069283
        );
    }

    #[test]
    #[cfg_attr(miri, ignore)] // crc32c uses CPU intrinsics Miri cannot run
    fn verify_success_and_mismatch() {
        let data = b"badger checksum";
        for algo in [Algorithm::Crc32c, Algorithm::XxHash64] {
            let mut sum = checksum_of(data, algo);
            verify_checksum(data, &sum).unwrap();
            sum.sum ^= 1;
            assert!(matches!(
                verify_checksum(data, &sum),
                Err(Error::ChecksumMismatch { .. })
            ));
        }
    }

    #[test]
    fn unknown_algorithm_is_an_error() {
        let sum = Checksum { algo: 99, sum: 0 };
        assert!(matches!(
            verify_checksum(b"x", &sum),
            Err(Error::Corruption(_))
        ));
    }
}
