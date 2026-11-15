//! Bit-for-bit comparisons with Go Badger, using values recorded by
//! `go-fixtures golden` in `compat/golden/phase1.json`.

use std::cmp::Ordering;
use std::fs;
use std::path::Path;

use badger::fb::{self, BlockOffsetBuilder, TableIndexBuilder};
use badger::pb::{self, checksum::Algorithm, manifest_change::Operation};
use badger::util::{bloom, checksum, encrypt, keys, value::ValueStruct, varint};
use flatbuffers::FlatBufferBuilder;
use prost::Message;
use serde::Deserialize;

#[derive(Deserialize)]
struct Golden {
    hash: Vec<HashCase>,
    bloom: Vec<BloomCase>,
    bloom_bits_per_key: Vec<BitsPerKeyCase>,
    checksum: Vec<ChecksumCase>,
    aes_ctr: Vec<AesCase>,
    value_struct: Vec<ValueStructCase>,
    key_with_ts: Vec<KeyWithTsCase>,
    uvarint: Vec<UvarintCase>,
    compare_keys: Vec<CompareKeysCase>,
    proto: Vec<ProtoCase>,
    table_index: TableIndexCase,
}

#[derive(Deserialize)]
struct HashCase {
    input: Hex,
    hash: u32,
}
#[derive(Deserialize)]
struct BloomCase {
    keys: Vec<Hex>,
    bits_per_key: i32,
    filter: Hex,
    probes: Vec<Probe>,
}
#[derive(Deserialize)]
struct Probe {
    key: Hex,
    may_contain: bool,
}
#[derive(Deserialize)]
struct BitsPerKeyCase {
    num_entries: usize,
    fp: f64,
    bits: i32,
}
#[derive(Deserialize)]
struct ChecksumCase {
    data: Hex,
    crc32c: u64,
    xxhash64: u64,
}
#[derive(Deserialize)]
struct AesCase {
    key: Hex,
    iv: Hex,
    plaintext: Hex,
    ciphertext: Hex,
}
#[derive(Deserialize)]
struct ValueStructCase {
    meta: u8,
    user_meta: u8,
    expires_at: u64,
    value: Hex,
    encoded: Hex,
}
#[derive(Deserialize)]
struct KeyWithTsCase {
    key: Hex,
    version: u64,
    encoded: Hex,
}
#[derive(Deserialize)]
struct UvarintCase {
    value: u64,
    encoded: Hex,
}
#[derive(Deserialize)]
struct CompareKeysCase {
    a: Hex,
    b: Hex,
    cmp: i32,
}
#[derive(Deserialize)]
struct ProtoCase {
    name: String,
    encoded: Hex,
}
#[derive(Deserialize)]
struct TableIndexCase {
    offsets: Vec<BlockOffsetCase>,
    bloom_filter: Hex,
    max_version: u64,
    key_count: u32,
    uncompressed_size: u32,
    on_disk_size: u32,
    stale_data_size: u32,
    encoded: Hex,
}
#[derive(Deserialize)]
struct BlockOffsetCase {
    key: Hex,
    offset: u32,
    len: u32,
}

/// Bytes stored as a hex string.
#[derive(Deserialize)]
#[serde(try_from = "String")]
struct Hex(Vec<u8>);

impl TryFrom<String> for Hex {
    type Error = hex::FromHexError;
    fn try_from(s: String) -> Result<Self, Self::Error> {
        hex::decode(s).map(Hex)
    }
}

impl std::ops::Deref for Hex {
    type Target = Vec<u8>;
    fn deref(&self) -> &Vec<u8> {
        &self.0
    }
}

fn golden() -> Golden {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../compat/golden/phase1.json");
    let bytes = fs::read(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
    serde_json::from_slice(&bytes).unwrap()
}

#[test]
fn hash_matches_go() {
    let g = golden();
    assert!(g.hash.len() > 40);
    for c in &g.hash {
        assert_eq!(
            bloom::hash(&c.input),
            c.hash,
            "input {}",
            hex::encode(&*c.input)
        );
    }
}

#[test]
fn bloom_filters_match_go() {
    for c in golden().bloom {
        let hashes: Vec<u32> = c.keys.iter().map(|k| bloom::hash(k)).collect();
        let filter = bloom::new_filter(&hashes, c.bits_per_key);
        assert_eq!(
            filter,
            *c.filter,
            "{} keys, {} bits/key",
            c.keys.len(),
            c.bits_per_key
        );
        for p in &c.probes {
            assert_eq!(bloom::may_contain_key(&filter, &p.key), p.may_contain);
        }
    }
}

#[test]
fn bloom_bits_per_key_matches_go() {
    for c in golden().bloom_bits_per_key {
        assert_eq!(
            bloom::bloom_bits_per_key(c.num_entries, c.fp),
            c.bits,
            "n={} fp={}",
            c.num_entries,
            c.fp
        );
    }
}

#[test]
fn checksums_match_go() {
    for c in golden().checksum {
        assert_eq!(
            checksum::calculate_checksum(&c.data, Algorithm::Crc32c),
            c.crc32c
        );
        assert_eq!(
            checksum::calculate_checksum(&c.data, Algorithm::XxHash64),
            c.xxhash64
        );
    }
}

#[test]
fn aes_ctr_matches_go() {
    for c in golden().aes_ctr {
        let ct = encrypt::xor_block_alloc(&c.plaintext, &c.key, &c.iv).unwrap();
        assert_eq!(
            ct,
            *c.ciphertext,
            "key {} bytes, {} bytes",
            c.key.len(),
            c.plaintext.len()
        );
        assert_eq!(
            encrypt::xor_block_alloc(&ct, &c.key, &c.iv).unwrap(),
            *c.plaintext
        );
    }
}

#[test]
fn value_struct_encoding_matches_go() {
    for c in golden().value_struct {
        let v = ValueStruct {
            meta: c.meta,
            user_meta: c.user_meta,
            expires_at: c.expires_at,
            value: &c.value,
            version: 0,
        };
        let mut out = Vec::new();
        v.encode_to(&mut out);
        assert_eq!(out, *c.encoded);
        assert_eq!(v.encoded_size() as usize, c.encoded.len());
        assert_eq!(ValueStruct::decode(&c.encoded).unwrap(), v);
    }
}

#[test]
fn keys_and_varints_match_go() {
    let g = golden();
    for c in g.key_with_ts {
        let k = keys::key_with_ts(&c.key, c.version);
        assert_eq!(k, *c.encoded);
        if !c.key.is_empty() {
            assert_eq!(keys::parse_ts(&k), c.version);
        }
        assert_eq!(keys::parse_key(&k), &c.key[..]);
    }
    for c in g.uvarint {
        let mut out = Vec::new();
        varint::append_uvarint(&mut out, c.value);
        assert_eq!(out, *c.encoded);
        assert_eq!(
            varint::uvarint(&c.encoded),
            Some((c.value, c.encoded.len()))
        );
    }
    for c in g.compare_keys {
        let want = c.cmp.cmp(&0);
        assert_eq!(keys::compare_keys(&c.a, &c.b), want);
        assert_eq!(keys::compare_keys(&c.b, &c.a), want.reverse());
    }
}

/// Must construct the same messages as `goldenProtoMessages` in golden.go.
fn proto_message(name: &str) -> Vec<u8> {
    let seq = |start: u8, n: u8| (0..n).map(|i| start + i).collect::<Vec<u8>>();
    match name {
        "checksum_crc32c" => pb::Checksum {
            algo: Algorithm::Crc32c as i32,
            sum: 0xdeadbeef,
        }
        .encode_to_vec(),
        "checksum_xxhash64" => pb::Checksum {
            algo: Algorithm::XxHash64 as i32,
            sum: (1 << 63) + 5,
        }
        .encode_to_vec(),
        "data_key" => pb::DataKey {
            key_id: 7,
            data: seq(0x20, 32),
            iv: seq(0x80, 16),
            created_at: 1_700_000_000,
        }
        .encode_to_vec(),
        "data_key_negative_created_at" => pb::DataKey {
            key_id: 1,
            created_at: -1,
            ..Default::default()
        }
        .encode_to_vec(),
        "manifest_change_set" => pb::ManifestChangeSet {
            changes: vec![
                pb::ManifestChange {
                    id: 1,
                    op: Operation::Create as i32,
                    compression: 1,
                    ..Default::default()
                },
                pb::ManifestChange {
                    id: 2,
                    op: Operation::Delete as i32,
                    ..Default::default()
                },
                pb::ManifestChange {
                    id: 3,
                    op: Operation::Create as i32,
                    level: 6,
                    key_id: 9,
                    encryption_algo: pb::EncryptionAlgo::Aes as i32,
                    compression: 2,
                },
            ],
        }
        .encode_to_vec(),
        "kv_list" => pb::KvList {
            kv: vec![
                pb::Kv {
                    key: b"k1".to_vec(),
                    value: b"v1".to_vec(),
                    user_meta: vec![1],
                    version: 5,
                    expires_at: 100,
                    meta: vec![2],
                    stream_id: 3,
                    stream_done: false,
                },
                pb::Kv {
                    key: b"k2".to_vec(),
                    stream_done: true,
                    ..Default::default()
                },
            ],
            alloc_ref: 77,
        }
        .encode_to_vec(),
        "match" => pb::Match {
            prefix: b"abc".to_vec(),
            ignore_bytes: "1, 3-4".into(),
        }
        .encode_to_vec(),
        "empty_kv" => pb::Kv::default().encode_to_vec(),
        other => panic!("unknown golden proto message {other}"),
    }
}

#[test]
fn protobuf_encoding_matches_go() {
    let g = golden();
    assert!(!g.proto.is_empty());
    for c in g.proto {
        assert_eq!(proto_message(&c.name), *c.encoded, "{}", c.name);
    }
    // Decoding Go's bytes and re-encoding is stable too.
    let set = pb::ManifestChangeSet::decode(&proto_message("manifest_change_set")[..]).unwrap();
    assert_eq!(set.changes[1].op(), Operation::Delete);
}

#[test]
fn table_index_matches_go() {
    let c = golden().table_index;

    // Reading Go's bytes.
    let index = fb::root_as_table_index(&c.encoded).unwrap();
    assert_eq!(index.max_version(), c.max_version);
    assert_eq!(index.key_count(), c.key_count);
    assert_eq!(index.uncompressed_size(), c.uncompressed_size);
    assert_eq!(
        index.on_disk_size(),
        c.on_disk_size + c.encoded.len() as u32
    );
    assert_eq!(index.stale_data_size(), c.stale_data_size);
    assert_eq!(index.bloom_filter().unwrap().bytes(), &c.bloom_filter[..]);
    let offsets = index.offsets().unwrap();
    assert_eq!(offsets.len(), c.offsets.len());
    for (got, want) in offsets.iter().zip(&c.offsets) {
        assert_eq!(got.key().unwrap().bytes(), &want.key[..]);
        assert_eq!((got.offset(), got.len()), (want.offset, want.len));
    }

    // Building the same index in the same order as Go's buildIndex.
    let mut fbb = FlatBufferBuilder::with_capacity(3 << 20);
    let mut uoffs = Vec::new();
    for o in &c.offsets {
        let key = fbb.create_vector(&o.key[..]);
        let mut b = BlockOffsetBuilder::new(&mut fbb);
        b.add_key(key);
        b.add_offset(o.offset);
        b.add_len(o.len);
        uoffs.push(b.finish());
    }
    let offsets = fbb.create_vector(&uoffs);
    let bloom = fbb.create_vector(&c.bloom_filter[..]);
    let mut b = TableIndexBuilder::new(&mut fbb);
    b.add_offsets(offsets);
    b.add_bloom_filter(bloom);
    b.add_max_version(c.max_version);
    b.add_uncompressed_size(c.uncompressed_size);
    b.add_key_count(c.key_count);
    b.add_on_disk_size(c.on_disk_size);
    b.add_stale_data_size(c.stale_data_size);
    let root = b.finish();
    fbb.finish(root, None);
    let mut built = fbb.finished_data().to_vec();
    let len = built.len() as u32;
    assert!(fb::mutate_on_disk_size(&mut built, c.on_disk_size + len));
    assert_eq!(
        hex::encode(&built),
        hex::encode(&*c.encoded),
        "Rust-built index differs from Go's"
    );
}

#[test]
fn compare_keys_orders_versions_newest_first() {
    let a = keys::key_with_ts(b"k", 2);
    let b = keys::key_with_ts(b"k", 1);
    assert_eq!(keys::compare_keys(&a, &b), Ordering::Less);
}
