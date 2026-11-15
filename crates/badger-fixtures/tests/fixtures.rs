//! Sanity checks on the Go-generated fixtures, so later phases can rely on them.

use std::fs;

use badger_fixtures::{Fixtures, Kind, Result, key_with_ts};

fn fixtures() -> Fixtures {
    Fixtures::load().expect(
        "fixtures missing: run `make fixtures` (or set BADGER_FIXTURES to a generated directory)",
    )
}

#[test]
fn index_lists_all_fixtures() {
    let fx = fixtures();
    assert!(fx.dbs().count() >= 10, "expected at least 10 DB fixtures");
    assert!(fx.ssts().count() >= 6, "expected at least 6 SST fixtures");
}

#[test]
fn db_fixtures_are_complete_and_ordered() -> Result<()> {
    let fx = fixtures();
    for db in fx.dbs() {
        let dir = fx.path(db);
        let exp = fx.db_expected(db)?;
        assert_eq!(exp.name, db.name);
        for required in ["MANIFEST", "KEYREGISTRY"] {
            assert!(
                dir.join(required).is_file(),
                "{}: missing {required}",
                db.name
            );
        }
        assert!(!exp.entries.is_empty(), "{}: no entries", db.name);

        // Key ascending, then version descending, as Badger iterates.
        for pair in exp.entries.windows(2) {
            let (a, b) = (&pair[0], &pair[1]);
            assert!(
                a.key < b.key || (a.key == b.key && a.version > b.version),
                "{}: entries out of order at {}@{}",
                db.name,
                hex(&a.key),
                a.version
            );
        }
        let newest = exp.entries.iter().map(|e| e.version).max().unwrap();
        assert!(
            newest <= exp.max_version,
            "{}: version above max_version",
            db.name
        );
        if !exp.options.encryption_key.is_empty() {
            assert_eq!(
                exp.options.encryption_key.len(),
                32,
                "{}: AES-256 key",
                db.name
            );
        }
    }
    Ok(())
}

#[test]
fn crash_snapshot_has_unflushed_wal() -> Result<()> {
    let fx = fixtures();
    let db = fx.get(Kind::Db, "crash-wal")?;
    assert!(fx.db_expected(db)?.crash_snapshot);
    let has_mem = fs::read_dir(fx.path(db))?
        .filter_map(|e| e.ok())
        .any(|e| e.path().extension().is_some_and(|x| x == "mem"));
    assert!(has_mem, "crash-wal should contain a .mem file");
    Ok(())
}

#[test]
fn sst_fixtures_match_their_entries() -> Result<()> {
    let fx = fixtures();
    for sst in fx.ssts() {
        let exp = fx.sst_expected(sst)?;
        assert!(fx.path(sst).is_file(), "{}: missing table file", sst.name);
        assert_eq!(
            exp.key_count as usize,
            exp.entries.len(),
            "{}: key_count",
            sst.name
        );

        let first = exp.entries.first().unwrap();
        let last = exp.entries.last().unwrap();
        assert_eq!(
            exp.smallest,
            key_with_ts(&first.key, first.version),
            "{}",
            sst.name
        );
        assert_eq!(
            exp.biggest,
            key_with_ts(&last.key, last.version),
            "{}",
            sst.name
        );

        let max = exp.entries.iter().map(|e| e.version).max().unwrap();
        assert_eq!(exp.max_version, max, "{}: max_version", sst.name);
        if let Some(key) = &exp.data_key {
            assert_eq!(
                (key.data.len(), key.iv.len()),
                (32, 16),
                "{}: data key",
                sst.name
            );
        }
    }
    Ok(())
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}
