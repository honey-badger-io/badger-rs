//! Phase 7: backups move between Go and Rust.
//!
//! - Backups Go wrote of fixture DBs (`compat/fixtures/backup`, made by
//!   `go-fixtures backup`) load into a Rust DB and read like the source.
//! - Rust backs up the same fixture DBs and loads the result itself. With
//!   `BADGER_COMPAT_OUT=<dir>` set, the backups and the source's expected
//!   JSON go to `<dir>/backup`, where `go-fixtures verify-backup` loads them
//!   with Go (`make compat`, CI).

use std::path::{Path, PathBuf};

use badger::options::CompressionType;
use badger::{DB, IteratorOptions, Options};
use badger_fixtures::{DbEntry, DbExpected, Fixtures};

/// The fixture DBs that have a Go backup.
const NAMES: [&str; 4] = ["basic", "vlog", "versions", "managed"];

fn fixtures() -> Fixtures {
    Fixtures::load().expect("fixtures missing: run `make fixtures`")
}

/// The newest version of each key unless deleted or expired: what a
/// latest-version iteration returns (Go's `liveEntries`).
fn live(entries: &[DbEntry]) -> Vec<DbEntry> {
    let mut out: Vec<DbEntry> = Vec::new();
    let mut prev: Option<&[u8]> = None;
    for e in entries {
        if prev == Some(e.key.as_slice()) {
            continue;
        }
        prev = Some(&e.key);
        if !e.deleted_or_expired {
            out.push(e.clone());
        }
    }
    out
}

fn latest(db: &DB) -> Vec<DbEntry> {
    db.view(|txn| {
        let mut it = txn.iter(IteratorOptions::default());
        it.rewind();
        let mut out = Vec::new();
        while it.valid() {
            let item = it.item();
            out.push(DbEntry {
                key: item.key().to_vec(),
                version: item.version(),
                value: item.value_copy()?,
                user_meta: item.user_meta(),
                expires_at: item.expires_at(),
                deleted_or_expired: false,
                discard_earlier_versions: item.discard_earlier_versions(),
            });
            it.next();
        }
        Ok(out)
    })
    .unwrap()
}

/// Loads `bak` into a fresh DB and checks it against the source's dump.
fn check_load(name: &str, bak: &[u8], exp: &DbExpected) {
    let dir = tempfile::tempdir().unwrap();
    let db = DB::open(Options::new(dir.path())).unwrap();
    db.load(bak, 16)
        .unwrap_or_else(|e| panic!("{name}: load: {e}"));
    let want = live(&exp.entries);
    let got = latest(&db);
    assert_eq!(got.len(), want.len(), "{name}: live keys");
    for (g, w) in got.iter().zip(&want) {
        assert_eq!(g, w, "{name}");
    }
    // New commits come after every loaded version.
    let max = want.iter().map(|e| e.version).max().unwrap_or(0);
    db.update(|txn| txn.set("after-load", "x")).unwrap();
    let v = db
        .view(|txn| Ok(txn.get(b"after-load")?.version()))
        .unwrap();
    assert!(
        v > max,
        "{name}: commit at {v} after loading versions up to {max}"
    );
    db.close().unwrap();
}

#[test]
fn go_backups_load_in_rust() {
    let fx = fixtures();
    for name in NAMES {
        let entry = fx.get(badger_fixtures::Kind::Db, name).unwrap();
        let exp = fx.db_expected(entry).unwrap();
        let bak_path = fx.root().join("backup").join(format!("{name}.bak"));
        let bak =
            std::fs::read(&bak_path).unwrap_or_else(|e| panic!("{}: {e}", bak_path.display()));
        check_load(name, &bak, &exp);
    }
}

fn open_fixture(dir: &Path, exp: &DbExpected) -> DB {
    let o = &exp.options;
    let mut opt = Options::new(dir)
        .with_read_only(true)
        .with_compression(CompressionType::parse(&o.compression).unwrap().0)
        .with_value_threshold(o.value_threshold as u64)
        .with_mem_table_size(o.mem_table_size as u64)
        .with_value_log_file_size(o.value_log_file_size as u64)
        .with_base_table_size(o.base_table_size as u64)
        .with_num_versions_to_keep(o.num_versions_to_keep as usize);
    if !o.encryption_key.is_empty() {
        opt = opt
            .with_encryption_key(o.encryption_key.clone())
            .with_index_cache_size(1 << 20);
    }
    if o.managed {
        DB::open_managed(opt).unwrap()
    } else {
        DB::open(opt).unwrap()
    }
}

fn out_dir() -> (Option<tempfile::TempDir>, PathBuf) {
    match std::env::var_os("BADGER_COMPAT_OUT") {
        Some(out) => (None, PathBuf::from(out).join("backup")),
        None => {
            let tmp = tempfile::tempdir().unwrap();
            let p = tmp.path().to_path_buf();
            (Some(tmp), p)
        }
    }
}

#[test]
fn rust_backups_for_go() {
    let fx = fixtures();
    let (_tmp, out) = out_dir();
    std::fs::create_dir_all(&out).unwrap();
    for name in NAMES {
        let entry = fx.get(badger_fixtures::Kind::Db, name).unwrap();
        let exp = fx.db_expected(entry).unwrap();
        let db = open_fixture(&fx.path(entry), &exp);
        let mut bak = Vec::new();
        let max_version = if exp.options.managed {
            db.new_stream_at(u64::MAX).backup(&mut bak, 0).unwrap()
        } else {
            db.backup(&mut bak, 0).unwrap()
        };
        assert_eq!(max_version, exp.max_version, "{name}");
        db.close().unwrap();

        check_load(name, &bak, &exp);
        std::fs::write(out.join(format!("rust-{name}.bak")), &bak).unwrap();
        exp.write(&out.join(format!("rust-{name}.json"))).unwrap();
    }
}
