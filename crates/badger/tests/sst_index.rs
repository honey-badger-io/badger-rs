//! Reads the index of every Go-written SST fixture with the Phase 1 building
//! blocks: footer parsing, checksum, decryption, flatbuffers and bloom filter.
//!
//! SSTable tail layout (Go's `table.initIndex`):
//! ```text
//! | blocks ... | index | index len (u32 BE) | checksum (pb) | checksum len (u32 BE) |
//! ```
//! For encrypted tables the index is AES-CTR encrypted with its 16-byte IV
//! appended, and the checksum covers the encrypted bytes.

use std::fs;

use badger::fb;
use badger::pb;
use badger::util::{bloom, checksum, encrypt, keys};
use badger_fixtures::{Fixtures, SstExpected};
use prost::Message;

fn read_u32_be(buf: &[u8], pos: usize) -> usize {
    u32::from_be_bytes(buf[pos..pos + 4].try_into().unwrap()) as usize
}

/// Returns (decrypted index bytes, offset where the index starts).
fn read_index(table: &[u8], exp: &SstExpected) -> (Vec<u8>, usize) {
    let mut pos = table.len() - 4;
    let checksum_len = read_u32_be(table, pos);
    pos -= checksum_len;
    let sum = pb::Checksum::decode(&table[pos..pos + checksum_len]).unwrap();
    pos -= 4;
    let index_len = read_u32_be(table, pos);
    pos -= index_len;
    let stored = &table[pos..pos + index_len];
    checksum::verify_checksum(stored, &sum).unwrap();

    let index = match &exp.data_key {
        None => stored.to_vec(),
        Some(dk) => {
            let (data, iv) = stored.split_at(stored.len() - encrypt::BLOCK_SIZE);
            encrypt::xor_block_alloc(data, &dk.data, iv).unwrap()
        }
    };
    (index, pos)
}

#[test]
fn go_sst_indexes_decode() {
    let fx = Fixtures::load().expect("fixtures missing: run `make fixtures`");
    for sst in fx.ssts() {
        let exp = fx.sst_expected(sst).unwrap();
        let table = fs::read(fx.path(sst)).unwrap();
        let (index_bytes, index_start) = read_index(&table, &exp);
        let index = fb::root_as_table_index(&index_bytes).unwrap();
        let name = &sst.name;

        assert_eq!(
            index.key_count() as usize,
            exp.entries.len(),
            "{name}: key_count"
        );
        assert_eq!(index.max_version(), exp.max_version, "{name}: max_version");
        // Go's onDiskSize = block bytes + plain (unencrypted) index length +
        // the vlog bytes referenced by value pointers (Builder.Add's valueLen).
        let vlog_bytes: usize = exp.entries.iter().map(|e| e.value_len as usize).sum();
        assert_eq!(
            index.on_disk_size() as usize,
            index_start + index_bytes.len() + vlog_bytes,
            "{name}: on_disk_size"
        );

        // Blocks are contiguous from offset 0 up to the index.
        let offsets = index.offsets().unwrap();
        assert!(!offsets.is_empty());
        let mut next = 0;
        for bo in offsets.iter() {
            assert_eq!(
                bo.offset() as usize,
                next,
                "{name}: block offsets are contiguous"
            );
            next += bo.len() as usize;
        }
        assert_eq!(
            next, index_start,
            "{name}: last block ends where the index starts"
        );
        assert_eq!(
            offsets.get(0).key().unwrap().bytes(),
            &exp.smallest[..],
            "{name}: first key"
        );
        if exp.block_size == 256 {
            assert!(offsets.len() > 50, "{name}: small blocks give many blocks");
        }

        // The bloom filter is built over hashes of user keys (no version).
        let filter = index.bloom_filter().unwrap().bytes();
        for e in &exp.entries {
            assert!(
                bloom::may_contain_key(filter, &e.key),
                "{name}: bloom misses a key"
            );
        }
        let absent =
            (0..1000).filter(|i| bloom::may_contain_key(filter, format!("absent-{i}").as_bytes()));
        assert!(
            absent.count() < 50,
            "{name}: bloom false positive rate too high"
        );
        assert_eq!(keys::parse_key(&exp.smallest), &exp.entries[0].key[..]);
    }
}
