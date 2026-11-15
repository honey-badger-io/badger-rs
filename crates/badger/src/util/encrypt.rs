//! AES in CTR mode, used to encrypt blocks, vlog entries, WAL entries and data
//! keys. Port of Go's `y/encrypt.go`.
//!
//! Go's `cipher.NewCTR` treats the whole 16-byte IV as a big-endian counter,
//! which is `ctr::Ctr128BE`. Encryption and decryption are the same XOR.

use std::io::Write;

use aes::{Aes128, Aes192, Aes256};
use ctr::Ctr128BE;
use ctr::cipher::{KeyIvInit, StreamCipher};

use crate::error::{Error, Result};

/// AES block size, and so the IV size.
pub const BLOCK_SIZE: usize = 16;

enum Ctr {
    Aes128(Ctr128BE<Aes128>),
    Aes192(Ctr128BE<Aes192>),
    Aes256(Ctr128BE<Aes256>),
}

impl Ctr {
    fn new(key: &[u8], iv: &[u8]) -> Result<Self> {
        if iv.len() != BLOCK_SIZE {
            return Err(Error::InvalidOptions(format!(
                "IV must be {BLOCK_SIZE} bytes, got {}",
                iv.len()
            )));
        }
        // The lengths are checked above / by the match, so new_from_slices cannot fail.
        Ok(match key.len() {
            16 => Ctr::Aes128(Ctr128BE::new_from_slices(key, iv).unwrap()),
            24 => Ctr::Aes192(Ctr128BE::new_from_slices(key, iv).unwrap()),
            32 => Ctr::Aes256(Ctr128BE::new_from_slices(key, iv).unwrap()),
            _ => return Err(Error::InvalidEncryptionKey),
        })
    }

    fn apply(&mut self, buf: &mut [u8]) {
        match self {
            Ctr::Aes128(c) => c.apply_keystream(buf),
            Ctr::Aes192(c) => c.apply_keystream(buf),
            Ctr::Aes256(c) => c.apply_keystream(buf),
        }
    }
}

/// Encrypts or decrypts `buf` in place.
pub fn xor_block_in_place(buf: &mut [u8], key: &[u8], iv: &[u8]) -> Result<()> {
    Ctr::new(key, iv)?.apply(buf);
    Ok(())
}

/// Encrypts or decrypts `src` into `dst` (Go's `XORBlock`).
///
/// # Panics
/// If `dst` and `src` differ in length.
pub fn xor_block(dst: &mut [u8], src: &[u8], key: &[u8], iv: &[u8]) -> Result<()> {
    assert_eq!(
        dst.len(),
        src.len(),
        "xor_block: dst and src lengths differ"
    );
    let mut ctr = Ctr::new(key, iv)?;
    dst.copy_from_slice(src);
    ctr.apply(dst);
    Ok(())
}

/// Encrypts or decrypts `src` into a new buffer (Go's `XORBlockAllocate`).
pub fn xor_block_alloc(src: &[u8], key: &[u8], iv: &[u8]) -> Result<Vec<u8>> {
    let mut out = src.to_vec();
    xor_block_in_place(&mut out, key, iv)?;
    Ok(out)
}

/// Encrypts or decrypts `src` and writes the result to `w` (Go's `XORBlockStream`).
pub fn xor_block_stream(w: &mut impl Write, src: &[u8], key: &[u8], iv: &[u8]) -> Result<()> {
    let mut ctr = Ctr::new(key, iv)?;
    let mut chunk = [0u8; 4096];
    for piece in src.chunks(chunk.len()) {
        let buf = &mut chunk[..piece.len()];
        buf.copy_from_slice(piece);
        ctr.apply(buf);
        w.write_all(buf)?;
    }
    Ok(())
}

/// A random IV from the OS's secure random number generator.
pub fn generate_iv() -> Result<[u8; BLOCK_SIZE]> {
    let mut iv = [0u8; BLOCK_SIZE];
    getrandom::fill(&mut iv).map_err(|e| Error::Io(std::io::Error::other(e)))?;
    Ok(iv)
}

#[cfg(test)]
mod tests {
    use super::*;

    // Port of Go's TestXORBlock: round trip, and the stream form agrees.
    #[test]
    #[cfg_attr(miri, ignore)] // AES uses CPU intrinsics Miri cannot run
    fn xor_block_round_trip() {
        let key = [7u8; 32];
        let iv = generate_iv().unwrap();
        let src: Vec<u8> = (0..10_000u32).map(|i| (i * 31) as u8).collect();

        let mut enc = vec![0u8; src.len()];
        xor_block(&mut enc, &src, &key, &iv).unwrap();
        assert_ne!(enc, src);
        assert_eq!(xor_block_alloc(&enc, &key, &iv).unwrap(), src);

        let mut streamed = Vec::new();
        xor_block_stream(&mut streamed, &src, &key, &iv).unwrap();
        assert_eq!(streamed, enc);
    }

    #[test]
    #[cfg_attr(miri, ignore)] // AES uses CPU intrinsics Miri cannot run
    fn rejects_bad_key_and_iv() {
        let mut buf = [0u8; 4];
        assert!(matches!(
            xor_block_in_place(&mut buf, &[0; 15], &[0; 16]),
            Err(Error::InvalidEncryptionKey)
        ));
        assert!(xor_block_in_place(&mut buf, &[0; 16], &[0; 8]).is_err());
    }
}
