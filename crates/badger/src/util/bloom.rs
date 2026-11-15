//! Bloom filters for SSTables. Port of Go's `y/bloom.go`, which in turn follows
//! LevelDB's `bloom.cc`.
//!
//! Filter layout: `nBytes` of bit array followed by one byte `k`, the number of
//! probes. Probe `j` tests bit `(h + j*delta) mod nBits`, with
//! `delta = rotate_right(h, 17)`.

/// The hash used for bloom filters: a Murmur-like hash with LevelDB's seed.
/// This must match Go exactly, because filters are stored on disk.
pub fn hash(b: &[u8]) -> u32 {
    const SEED: u32 = 0xbc9f1d34;
    const M: u32 = 0xc6a4a793;

    let mut h = SEED ^ (b.len() as u32).wrapping_mul(M);
    let mut chunks = b.chunks_exact(4);
    for c in &mut chunks {
        h = h.wrapping_add(u32::from_le_bytes([c[0], c[1], c[2], c[3]]));
        h = h.wrapping_mul(M);
        h ^= h >> 16;
    }
    let rest = chunks.remainder();
    // Go's switch with fallthrough: 3 adds b[2]<<16, then b[1]<<8, then b[0].
    if rest.len() == 3 {
        h = h.wrapping_add((rest[2] as u32) << 16);
    }
    if rest.len() >= 2 {
        h = h.wrapping_add((rest[1] as u32) << 8);
    }
    if !rest.is_empty() {
        h = h.wrapping_add(rest[0] as u32);
        h = h.wrapping_mul(M);
        h ^= h >> 24;
    }
    h
}

/// Whether `filter` may contain a key with hash `h`. False positives are possible.
pub fn may_contain(filter: &[u8], mut h: u32) -> bool {
    if filter.len() < 2 {
        return false;
    }
    let k = filter[filter.len() - 1];
    if k > 30 {
        // Reserved for potential new encodings of short filters: treat as a match.
        return true;
    }
    let n_bits = (8 * (filter.len() - 1)) as u32;
    let delta = h.rotate_right(17);
    for _ in 0..k {
        let bit_pos = h % n_bits;
        if filter[(bit_pos / 8) as usize] & (1 << (bit_pos % 8)) == 0 {
            return false;
        }
        h = h.wrapping_add(delta);
    }
    true
}

/// [`may_contain`] for a raw key.
pub fn may_contain_key(filter: &[u8], key: &[u8]) -> bool {
    may_contain(filter, hash(key))
}

/// Bits per key needed for false-positive rate `fp` with `num_entries` keys.
pub fn bloom_bits_per_key(num_entries: usize, fp: f64) -> i32 {
    // Go's literal, not the exact ln 2: changing it could change results.
    #[allow(clippy::approx_constant)]
    const LN2: f64 = 0.69314718056;
    let n = num_entries as f64;
    // Go: math.Pow(ln2, 2). For an exponent of 2, Go's Pow does a single
    // multiplication, so `LN2 * LN2` gives the identical result.
    let size = -n * fp.ln() / (LN2 * LN2);
    (LN2 * size / n).ceil() as i32
}

/// Builds a filter over the given key hashes.
pub fn new_filter(hashes: &[u32], bits_per_key: i32) -> Vec<u8> {
    let bits_per_key = bits_per_key.max(0) as usize;
    // 0.69 is approximately ln(2).
    let k = ((bits_per_key as f64 * 0.69) as u32).clamp(1, 30);

    // Small filters get a minimum size, or the false positive rate explodes.
    let n_bytes = (hashes.len() * bits_per_key).max(64).div_ceil(8);
    let n_bits = (n_bytes * 8) as u32;

    let mut filter = vec![0u8; n_bytes + 1];
    for &h0 in hashes {
        let mut h = h0;
        let delta = h.rotate_right(17);
        for _ in 0..k {
            let bit_pos = h % n_bits;
            filter[(bit_pos / 8) as usize] |= 1 << (bit_pos % 8);
            h = h.wrapping_add(delta);
        }
    }
    filter[n_bytes] = k as u8;
    filter
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bits(f: &[u8]) -> String {
        f.iter()
            .flat_map(|x| (0..8).map(move |j| if x & (1 << j) != 0 { '1' } else { '.' }))
            .collect()
    }

    // Port of Go's TestSmallBloomFilter (expected bits from LevelDB's bloom_test.cc).
    #[test]
    fn small_bloom_filter() {
        let f = new_filter(&[hash(b"hello"), hash(b"world")], 10);
        assert_eq!(
            bits(&f),
            "1...1.........1.........1.....1...1...1.....1.........1.....1....11....."
        );
        assert!(may_contain_key(&f, b"hello"));
        assert!(may_contain_key(&f, b"world"));
        assert!(!may_contain_key(&f, b"x"));
        assert!(!may_contain_key(&f, b"foo"));
    }

    // Port of Go's TestBloomFilter: sizes, no false negatives, FP rate.
    #[test]
    #[cfg_attr(miri, ignore)] // too slow under Miri; no unsafe code involved
    fn bloom_filter_sizes_and_false_positive_rate() {
        let next_length = |x: usize| match x {
            0..10 => x + 1,
            10..100 => x + 10,
            100..1000 => x + 100,
            _ => x + 1000,
        };
        let le32 = |i: u32| i.to_le_bytes();

        let (mut mediocre, mut good) = (0, 0);
        let mut length = 1;
        while length <= 10_000 {
            let hashes: Vec<u32> = (0..length as u32).map(|i| hash(&le32(i))).collect();
            let f = new_filter(&hashes, 10);
            assert!(
                f.len() <= length * 10 / 8 + 40,
                "length={length}: len(f)={}",
                f.len()
            );
            for i in 0..length as u32 {
                assert!(
                    may_contain_key(&f, &le32(i)),
                    "length={length}: missing key {i}"
                );
            }
            let false_positives = (0..10_000)
                .filter(|i| may_contain_key(&f, &le32(1_000_000_000 + i)))
                .count();
            assert!(
                false_positives <= 200,
                "length={length}: {false_positives} FPs"
            );
            if false_positives > 125 {
                mediocre += 1;
            } else {
                good += 1;
            }
            length = next_length(length);
        }
        assert!(
            mediocre <= good / 5,
            "{mediocre} mediocre vs {good} good filters"
        );
    }

    // Port of Go's TestHash (expected values from LevelDB's hash.cc).
    #[test]
    fn hash_matches_leveldb() {
        for (s, want) in [
            ("", 0xbc9f1d34),
            ("g", 0xd04a8bda),
            ("go", 0x3e0b0745),
            ("gop", 0x0c326610),
            ("goph", 0x8c9d6390),
            ("gophe", 0x9bfd4b0a),
            ("gopher", 0xa78edc7c),
            ("I had a dream it would end this way.", 0xe14a9db9),
        ] {
            assert_eq!(hash(s.as_bytes()), want, "{s:?}");
        }
    }
}
