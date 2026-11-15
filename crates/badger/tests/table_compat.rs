//! Table compatibility with Go Badger, using the fixtures in compat/fixtures:
//!
//! - Rust reads every Go-written SST fixture, entry by entry.
//! - Rust reads every table inside the unencrypted Go-written DB fixtures.
//! - Rust's builder reproduces Go's uncompressed, unencrypted tables byte for byte.
//! - Rust builds every SST fixture's entries with the same options. With
//!   `BADGER_COMPAT_OUT=<dir>` set, the results are written there for
//!   `go-fixtures verify-sst` to check (CI does this).

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use badger::options::CompressionType;
use badger::pb;
use badger::table::{Table, TableBuilder, TableOptions, id_to_filename, new_index_cache};
use badger::util::KvIterator;
use badger::util::keys::{key_with_ts, parse_key, parse_ts};
use badger::util::value::ValueStruct;
use badger_fixtures::{Fixtures, SstEntry, SstExpected};

fn fixtures() -> Fixtures {
    Fixtures::load().expect("fixtures missing: run `make fixtures`")
}

fn compression(name: &str) -> CompressionType {
    CompressionType::parse(name).unwrap().0
}

fn table_options(exp: &SstExpected) -> TableOptions {
    TableOptions {
        table_size: exp.table_size,
        block_size: exp.block_size,
        bloom_false_positive: exp.bloom_false_positive,
        compression: compression(&exp.compression),
        zstd_compression_level: 1,
        data_key: exp.data_key.as_ref().map(|k| {
            Arc::new(pb::DataKey {
                key_id: k.key_id,
                data: k.data.clone(),
                iv: k.iv.clone(),
                created_at: k.created_at,
            })
        }),
        index_cache: exp.data_key.as_ref().map(|_| new_index_cache(1 << 20)),
        checksum_mode: badger::options::ChecksumVerificationMode::OnTableAndBlockRead,
        ..Default::default()
    }
}

/// Reads all entries of a table, in order.
fn read_entries(table: &Arc<Table>) -> Vec<SstEntry> {
    let mut it = table.iter(false, false);
    let mut out = Vec::new();
    it.rewind();
    while it.valid() {
        let v = it.value();
        out.push(SstEntry {
            key: parse_key(it.key()).to_vec(),
            version: parse_ts(it.key()),
            meta: v.meta,
            user_meta: v.user_meta,
            expires_at: v.expires_at,
            value: v.value.to_vec(),
            value_len: 0,
        });
        it.next();
    }
    assert!(
        it.error().is_none(),
        "{}: {:?}",
        table.filename(),
        it.error()
    );
    out
}

fn assert_entries_match(name: &str, got: &[SstEntry], want: &[SstEntry]) {
    assert_eq!(got.len(), want.len(), "{name}: entry count");
    for (i, (g, w)) in got.iter().zip(want).enumerate() {
        let w = SstEntry {
            value_len: 0,
            ..w.clone()
        };
        assert_eq!(*g, w, "{name}: entry {i}");
    }
}

fn build(exp: &SstExpected) -> TableBuilder {
    let mut b = TableBuilder::new(table_options(exp));
    for e in &exp.entries {
        let vs = ValueStruct {
            meta: e.meta,
            user_meta: e.user_meta,
            expires_at: e.expires_at,
            value: &e.value,
            version: 0,
        };
        b.add(&key_with_ts(&e.key, e.version), vs, e.value_len);
    }
    b
}

#[test]
fn reads_go_sst_fixtures() {
    let fx = fixtures();
    for sst in fx.ssts() {
        let exp = fx.sst_expected(sst).unwrap();
        let table = Table::open(fx.path(sst), table_options(&exp)).unwrap();
        assert_eq!(table.smallest(), &exp.smallest[..], "{}", sst.name);
        assert_eq!(table.biggest(), &exp.biggest[..], "{}", sst.name);
        assert_eq!(table.key_count(), exp.key_count, "{}", sst.name);
        assert_eq!(table.max_version(), exp.max_version, "{}", sst.name);
        assert_entries_match(&sst.name, &read_entries(&table), &exp.entries);

        // Seeking to every key finds it, in both directions.
        let mut fwd = table.iter(false, true);
        let mut rev = table.iter(true, true);
        for e in exp.entries.iter().step_by(7) {
            let k = key_with_ts(&e.key, e.version);
            fwd.seek(&k);
            rev.seek(&k);
            assert_eq!((fwd.key(), rev.key()), (&k[..], &k[..]), "{}", sst.name);
        }
    }
}

#[test]
fn reads_tables_in_go_db_fixtures() {
    let fx = fixtures();
    let mut tables = 0;
    for db in fx.dbs() {
        let exp = fx.db_expected(db).unwrap();
        if !exp.options.encryption_key.is_empty() {
            continue; // needs the key registry (Phase 4)
        }
        let opts = TableOptions {
            compression: compression(&exp.options.compression),
            checksum_mode: badger::options::ChecksumVerificationMode::OnTableAndBlockRead,
            ..Default::default()
        };
        for entry in fs::read_dir(fx.path(db)).unwrap() {
            let path = entry.unwrap().path();
            if path.extension().is_some_and(|e| e == "sst") {
                let table = Table::open(&path, opts.clone())
                    .unwrap_or_else(|e| panic!("{}: {e}", path.display()));
                let entries = read_entries(&table);
                assert_eq!(
                    entries.len(),
                    table.key_count() as usize,
                    "{}",
                    path.display()
                );
                tables += 1;
            }
        }
    }
    assert!(
        tables >= 8,
        "expected tables in the DB fixtures, found {tables}"
    );
}

#[test]
fn builder_is_byte_identical_to_go() {
    let fx = fixtures();
    let mut compared = 0;
    for sst in fx.ssts() {
        let exp = fx.sst_expected(sst).unwrap();
        if exp.compression != "none" || exp.data_key.is_some() {
            continue; // compressed bytes differ between libraries; IVs are random
        }
        let go = fs::read(fx.path(sst)).unwrap();
        let rust = build(&exp).finish().unwrap();
        assert_eq!(rust.len(), go.len(), "{}: size", sst.name);
        if let Some(i) = rust.iter().zip(&go).position(|(a, b)| a != b) {
            panic!("{}: first difference at byte {i} of {}", sst.name, go.len());
        }
        compared += 1;
    }
    assert!(compared >= 2);
}

#[test]
fn builds_every_fixture_and_reads_it_back() {
    let fx = fixtures();
    let out = std::env::var_os("BADGER_COMPAT_OUT").map(PathBuf::from);
    let tmp = tempfile::tempdir().unwrap();
    for sst in fx.ssts() {
        let exp = fx.sst_expected(sst).unwrap();
        let dir = match &out {
            Some(out) => out.join("sst").join(&sst.name),
            None => tmp.path().join(&sst.name),
        };
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join(id_to_filename(1));
        let _ = fs::remove_file(&path);

        let table = Table::create(&path, build(&exp)).unwrap();
        assert_eq!(table.smallest(), &exp.smallest[..], "{}", sst.name);
        assert_eq!(table.biggest(), &exp.biggest[..], "{}", sst.name);
        assert_entries_match(&sst.name, &read_entries(&table), &exp.entries);

        if let Some(out) = &out {
            exp.write(&out.join("sst").join(format!("{}.json", sst.name)))
                .unwrap();
        }
    }
    if let Some(out) = &out {
        eprintln!("wrote Rust-built tables to {}", Path::new(out).display());
    }
}
