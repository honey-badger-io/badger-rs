//! Ports of Go's `levels_test.go` (compaction), plus L0 stall tests.

use std::sync::Arc;
use std::time::{Duration, SystemTime};

use super::*;
use crate::compaction::{CompactDef, CompactionPriority, KeyRange, contains_prefix};
use crate::manifest::new_create_change;
use crate::options::CompressionType;
use crate::structs::{BIT_DELETE, BIT_DISCARD_EARLIER_VERSIONS};
use crate::table::{Table, TableBuilder, TableOptions, new_filename};
use crate::util::keys::key_with_ts;
use crate::util::value::ValueStruct;

/// Go's `keyValVersion`: key, value, version, meta.
type Kvv = (&'static str, &'static str, u64, u8);

const D: u8 = BIT_DELETE;
const DISCARD: u8 = BIT_DISCARD_EARLIER_VERSIONS;

fn opts(dir: &std::path::Path) -> Options {
    Options::new(dir)
        .with_num_compactors(0)
        .with_num_versions_to_keep(1)
}

/// Go's runBadgerTest with `managedTxns = true`.
fn run_managed(opt: impl FnOnce(&std::path::Path) -> Options, f: impl FnOnce(&DB)) {
    let dir = tempfile::tempdir().unwrap();
    let db = DB::open_managed(opt(dir.path())).unwrap();
    f(&db);
    db.close().unwrap();
}

fn lc(db: &DB) -> &LevelsController {
    &db.inner.lc
}

fn tables(db: &DB, level: usize) -> Vec<Arc<Table>> {
    lc(db).levels[level].read().tables.clone()
}

fn set_size(db: &DB, level: usize, size: u64) {
    lc(db).levels[level].write().total_size = size;
}

/// Go's `buildTable` helpers: a table file with the next table id.
fn build_table(db: &DB, add: impl FnOnce(&mut TableBuilder)) -> Arc<Table> {
    let topt = TableOptions {
        block_size: db.inner.opt.block_size,
        bloom_false_positive: db.inner.opt.bloom_false_positive,
        ..Default::default()
    };
    let mut b = TableBuilder::new(topt);
    add(&mut b);
    let id = lc(db).reserve_file_id();
    Table::create(new_filename(id, &db.inner.opt.dir), b).unwrap()
}

/// Records `t` in the manifest at `level` and adds it there.
fn add_to_level(db: &DB, t: Arc<Table>, level: usize) {
    db.inner
        .manifest
        .add_changes(vec![new_create_change(
            t.id(),
            level as u32,
            0,
            t.compression_type(),
        )])
        .unwrap();
    let h = &lc(db).levels[level];
    h.add_table(t);
    h.sort_tables();
}

/// Go's `createAndOpen`.
fn create_and_open(db: &DB, td: &[Kvv], level: usize) {
    let t = build_table(db, |b| {
        for &(k, v, version, meta) in td {
            let vs = ValueStruct {
                value: v.as_bytes(),
                meta,
                ..Default::default()
            };
            b.add(&key_with_ts(k.as_bytes(), version), vs, 0);
        }
    });
    add_to_level(db, t, level);
}

/// Go's `getAllAndCheck`: every version, deleted ones included.
fn get_all_and_check(db: &DB, expected: &[Kvv]) {
    let got: Vec<(String, String, u64, u8)> = db
        .view(|txn| {
            let mut it = txn.iter(IteratorOptions {
                all_versions: true,
                internal_access: true,
                ..Default::default()
            });
            let mut out = Vec::new();
            it.rewind();
            while it.valid() {
                let item = it.item();
                out.push((
                    String::from_utf8(item.key().to_vec()).unwrap(),
                    String::from_utf8(item.value_copy()?).unwrap(),
                    item.version(),
                    item.meta,
                ));
                it.next();
            }
            Ok(out)
        })
        .unwrap();
    let want: Vec<(String, String, u64, u8)> = expected
        .iter()
        .map(|&(k, v, ver, m)| (k.to_string(), v.to_string(), ver, m))
        .collect();
    assert_eq!(got, want);
}

/// A compaction of all tables of `this` into all tables of `next`, with
/// `next` as the base level.
fn cdef(db: &DB, this: usize, next: usize) -> CompactDef {
    let mut t = lc(db).level_targets(&db.inner.opt);
    t.base_level = next;
    CompactDef {
        compactor_id: -1,
        this_level: this,
        next_level: next,
        top: tables(db, this),
        bot: tables(db, next),
        t,
        ..Default::default()
    }
}

fn run_cdef(db: &DB, mut cd: CompactDef) -> Result<()> {
    let l = cd.this_level;
    lc(db).run_compact_def(&db.inner, -1, l, &mut cd)
}

fn compact(db: &DB, this: usize, next: usize) {
    run_cdef(db, cdef(db, this, next)).unwrap();
}

#[test]
fn check_overlap() {
    // Same keys.
    run_managed(opts, |db| {
        create_and_open(db, &[("foo", "bar", 3, 0)], 0);
        create_and_open(db, &[("foo", "bar", 2, 0)], 1);
        let (l0, l1) = (tables(db, 0), tables(db, 1));
        assert!(lc(db).check_overlap(&l0, 0));
        assert!(lc(db).check_overlap(&l0, 1));
        assert!(!lc(db).check_overlap(&l0, 2));
        assert!(!lc(db).check_overlap(&l1, 2));
        assert!(!lc(db).check_overlap(&l0, 3));
        assert!(!lc(db).check_overlap(&l1, 3));
    });
    // Overlapping keys.
    run_managed(opts, |db| {
        create_and_open(
            db,
            &[("a", "x", 1, 0), ("b", "x", 1, 0), ("foo", "bar", 3, 0)],
            0,
        );
        create_and_open(db, &[("foo", "bar", 2, 0)], 1);
        let (l0, l1) = (tables(db, 0), tables(db, 1));
        assert!(lc(db).check_overlap(&l0, 0));
        assert!(lc(db).check_overlap(&l1, 1));
        assert!(lc(db).check_overlap(&l0, 1));
        assert!(!lc(db).check_overlap(&l0, 2));
        assert!(!lc(db).check_overlap(&l0, 3));
    });
    // Non-overlapping.
    run_managed(opts, |db| {
        create_and_open(
            db,
            &[("a", "x", 1, 0), ("b", "x", 1, 0), ("c", "bar", 3, 0)],
            0,
        );
        create_and_open(db, &[("foo", "bar", 2, 0)], 1);
        let l0 = tables(db, 0);
        assert!(!lc(db).check_overlap(&l0, 1));
        assert!(!lc(db).check_overlap(&l0, 2));
        assert!(!lc(db).check_overlap(&l0, 3));
    });
}

#[test]
fn compaction_level0_to_level1() {
    run_managed(opts, |db| {
        create_and_open(db, &[("foo", "bar", 3, 0), ("fooz", "baz", 1, 0)], 0);
        create_and_open(db, &[("foo", "bar", 2, 0)], 0);
        create_and_open(db, &[("foo", "bar", 1, 0)], 1);
        db.set_discard_ts(10);
        get_all_and_check(
            db,
            &[
                ("foo", "bar", 3, 0),
                ("foo", "bar", 2, 0),
                ("foo", "bar", 1, 0),
                ("fooz", "baz", 1, 0),
            ],
        );
        compact(db, 0, 1);
        get_all_and_check(db, &[("foo", "bar", 3, 0), ("fooz", "baz", 1, 0)]);
    });
}

#[test]
fn compaction_level0_to_level1_with_duplicates() {
    run_managed(opts, |db| {
        // foo@3 is in L0 again because GC rewrote it.
        create_and_open(db, &[("foo", "barNew", 3, 0), ("fooz", "baz", 1, 0)], 0);
        create_and_open(db, &[("foo", "bar", 4, 0)], 0);
        create_and_open(db, &[("foo", "bar", 3, 0)], 1);
        db.set_discard_ts(10);
        get_all_and_check(
            db,
            &[
                ("foo", "bar", 4, 0),
                ("foo", "barNew", 3, 0),
                ("fooz", "baz", 1, 0),
            ],
        );
        compact(db, 0, 1);
        get_all_and_check(db, &[("foo", "bar", 4, 0), ("fooz", "baz", 1, 0)]);
    });
}

#[test]
fn compaction_level0_to_level1_with_lower_overlap() {
    run_managed(opts, |db| {
        create_and_open(db, &[("foo", "bar", 4, 0), ("fooz", "baz", 1, 0)], 0);
        create_and_open(db, &[("foo", "bar", 3, 0)], 0);
        create_and_open(db, &[("foo", "bar", 2, 0)], 1);
        create_and_open(db, &[("foo", "bar", 1, 0)], 2);
        db.set_discard_ts(10);
        compact(db, 0, 1);
        get_all_and_check(
            db,
            &[
                ("foo", "bar", 4, 0),
                ("foo", "bar", 1, 0),
                ("fooz", "baz", 1, 0),
            ],
        );
    });
}

#[test]
fn compaction_level1_to_level2() {
    run_managed(opts, |db| {
        create_and_open(db, &[("foo", "bar", 3, 0), ("fooz", "baz", 1, 0)], 1);
        create_and_open(db, &[("foo", "bar", 2, 0)], 2);
        db.set_discard_ts(10);
        compact(db, 1, 2);
        get_all_and_check(db, &[("foo", "bar", 3, 0), ("fooz", "baz", 1, 0)]);
    });
}

#[test]
fn compaction_delete_with_overlap() {
    run_managed(opts, |db| {
        create_and_open(db, &[("foo", "bar", 3, D), ("fooz", "baz", 1, D)], 1);
        create_and_open(db, &[("foo", "bar", 2, 0)], 2);
        create_and_open(db, &[("foo", "bar", 1, 0)], 3);
        db.set_discard_ts(10);
        get_all_and_check(
            db,
            &[
                ("foo", "bar", 3, D),
                ("foo", "bar", 2, 0),
                ("foo", "bar", 1, 0),
                ("fooz", "baz", 1, D),
            ],
        );
        compact(db, 1, 2);
        // The deletes stay: L3 overlaps (checked once for the compaction).
        get_all_and_check(
            db,
            &[
                ("foo", "bar", 3, D),
                ("foo", "bar", 1, 0),
                ("fooz", "baz", 1, D),
            ],
        );
        compact(db, 2, 3);
        get_all_and_check(db, &[]);
    });
}

#[test]
fn compaction_delete_with_bottom_overlap() {
    run_managed(opts, |db| {
        create_and_open(db, &[("foo", "bar", 3, D)], 1);
        create_and_open(db, &[("foo", "bar", 2, 0), ("fooz", "baz", 2, D)], 2);
        create_and_open(db, &[("fooz", "baz", 1, 0)], 3);
        db.set_discard_ts(10);
        compact(db, 1, 2);
        // The bottom table overlaps L3, so the deletes stay.
        get_all_and_check(
            db,
            &[
                ("foo", "bar", 3, D),
                ("fooz", "baz", 2, D),
                ("fooz", "baz", 1, 0),
            ],
        );
    });
}

#[test]
fn compaction_delete_without_overlap() {
    run_managed(opts, |db| {
        create_and_open(db, &[("foo", "bar", 3, D), ("fooz", "baz", 1, D)], 1);
        create_and_open(db, &[("fooo", "barr", 2, 0)], 2);
        db.set_discard_ts(10);
        compact(db, 1, 2);
        get_all_and_check(db, &[("fooo", "barr", 2, 0)]);
    });
}

#[test]
fn compaction_delete_with_splits() {
    run_managed(opts, |db| {
        create_and_open(db, &[("C", "bar", 3, D)], 1);
        for k in ["A", "B", "C", "D"] {
            create_and_open(db, &[(k, "bar", 2, 0)], 2);
        }
        create_and_open(db, &[("fooz", "baz", 1, 0)], 3);
        db.set_discard_ts(10);
        get_all_and_check(
            db,
            &[
                ("A", "bar", 2, 0),
                ("B", "bar", 2, 0),
                ("C", "bar", 3, D),
                ("C", "bar", 2, 0),
                ("D", "bar", 2, 0),
                ("fooz", "baz", 1, 0),
            ],
        );
        compact(db, 1, 2);
        get_all_and_check(
            db,
            &[
                ("A", "bar", 2, 0),
                ("B", "bar", 2, 0),
                ("D", "bar", 2, 0),
                ("fooz", "baz", 1, 0),
            ],
        );
    });
}

#[test]
fn compaction_two_versions() {
    run_managed(
        |d| opts(d).with_num_versions_to_keep(2),
        |db| {
            create_and_open(db, &[("foo", "bar", 3, 0), ("fooz", "baz", 1, D)], 1);
            create_and_open(db, &[("foo", "bar", 2, 0)], 2);
            create_and_open(db, &[("foo", "bar", 1, 0)], 3);
            db.set_discard_ts(10);
            let all = [
                ("foo", "bar", 3, 0),
                ("foo", "bar", 2, 0),
                ("foo", "bar", 1, 0),
                ("fooz", "baz", 1, D),
            ];
            get_all_and_check(db, &all);
            compact(db, 1, 2);
            // Two versions are kept.
            get_all_and_check(db, &all);
            compact(db, 2, 3);
            get_all_and_check(db, &[("foo", "bar", 3, 0), ("foo", "bar", 2, 0)]);
        },
    );
}

#[test]
fn compaction_all_versions() {
    let all_versions = |d: &std::path::Path| opts(d).with_num_versions_to_keep(i32::MAX as usize);
    run_managed(all_versions, |db| {
        create_and_open(db, &[("foo", "bar", 3, 0), ("fooz", "baz", 1, D)], 1);
        create_and_open(db, &[("foo", "bar", 2, 0)], 2);
        create_and_open(db, &[("foo", "bar", 1, 0)], 3);
        db.set_discard_ts(10);
        let all = [
            ("foo", "bar", 3, 0),
            ("foo", "bar", 2, 0),
            ("foo", "bar", 1, 0),
            ("fooz", "baz", 1, D),
        ];
        compact(db, 1, 2);
        get_all_and_check(db, &all);
        compact(db, 2, 3);
        get_all_and_check(
            db,
            &[
                ("foo", "bar", 3, 0),
                ("foo", "bar", 2, 0),
                ("foo", "bar", 1, 0),
            ],
        );
    });
    run_managed(all_versions, |db| {
        create_and_open(db, &[("foo", "bar", 3, D), ("fooz", "baz", 1, D)], 1);
        create_and_open(db, &[("fooo", "barr", 2, 0)], 2);
        db.set_discard_ts(10);
        compact(db, 1, 2);
        get_all_and_check(db, &[("fooo", "barr", 2, 0)]);
    });
}

#[test]
fn discard_ts() {
    // All keys above the discard timestamp: nothing goes.
    run_managed(opts, |db| {
        create_and_open(db, &[("foo", "bar", 4, 0), ("fooz", "baz", 3, 0)], 0);
        create_and_open(db, &[("foo", "bar", 3, 0)], 0);
        create_and_open(db, &[("foo", "bar", 2, 0)], 1);
        db.set_discard_ts(1);
        let all = [
            ("foo", "bar", 4, 0),
            ("foo", "bar", 3, 0),
            ("foo", "bar", 2, 0),
            ("fooz", "baz", 3, 0),
        ];
        get_all_and_check(db, &all);
        compact(db, 0, 1);
        get_all_and_check(db, &all);
    });
    // Some keys above it.
    run_managed(opts, |db| {
        create_and_open(
            db,
            &[
                ("foo", "bar", 4, 0),
                ("foo", "bar", 3, 0),
                ("foo", "bar", 2, 0),
                ("fooz", "baz", 2, 0),
            ],
            0,
        );
        create_and_open(db, &[("foo", "bbb", 1, 0)], 1);
        db.set_discard_ts(3);
        compact(db, 0, 1);
        get_all_and_check(
            db,
            &[
                ("foo", "bar", 4, 0),
                ("foo", "bar", 3, 0),
                ("fooz", "baz", 2, 0),
            ],
        );
    });
    // All keys below it: one version of each is left.
    run_managed(opts, |db| {
        create_and_open(db, &[("foo", "bar", 4, 0), ("fooz", "baz", 3, 0)], 0);
        create_and_open(db, &[("foo", "bar", 3, 0)], 0);
        create_and_open(db, &[("foo", "bar", 2, 0)], 1);
        db.set_discard_ts(10);
        compact(db, 0, 1);
        get_all_and_check(db, &[("foo", "bar", 4, 0), ("fooz", "baz", 3, 0)]);
    });
}

/// The first discard_earlier_versions entry below the discard timestamp is
/// kept, even with unlimited versions.
#[test]
fn discard_first_version() {
    run_managed(
        |d| opts(d).with_num_versions_to_keep(i32::MAX as usize),
        |db| {
            for (version, meta) in [(1, 0), (2, DISCARD), (3, 0), (4, 0), (9, 0), (10, DISCARD)] {
                create_and_open(db, &[("foo", "bar", version, meta)], 0);
            }
            db.set_discard_ts(7);
            compact(db, 0, 1);
            get_all_and_check(
                db,
                &[
                    ("foo", "bar", 10, DISCARD),
                    ("foo", "bar", 9, 0),
                    ("foo", "bar", 4, 0),
                    ("foo", "bar", 3, 0),
                    ("foo", "bar", 2, DISCARD),
                ],
            );
        },
    );
}

#[test]
fn same_level() {
    run_managed(
        |d| {
            opts(d)
                .with_num_versions_to_keep(i32::MAX as usize)
                .with_lmax_compaction(true)
        },
        |db| {
            let rows = |k: &'static str, kfoo: &'static str| {
                [
                    (k, "bar", 4, DISCARD),
                    (k, "bar", 3, 0),
                    (k, "bar", 2, 0),
                    (kfoo, "baz", 2, 0),
                ]
            };
            let (a, b, c) = (rows("A", "Afoo"), rows("B", "Bfoo"), rows("C", "Cfoo"));
            for t in [&a, &b, &c] {
                create_and_open(db, t, 6);
            }
            lc(db).validate().unwrap();
            let all: Vec<Kvv> = a.iter().chain(&b).chain(&c).copied().collect();
            get_all_and_check(db, &all);

            let lmax = |db: &DB| {
                let mut cd = cdef(db, 6, 6);
                let t6 = tables(db, 6);
                cd.top = vec![Arc::clone(&t6[0])];
                cd.bot = t6[1..].to_vec();
                cd.t.base_level = 1;
                run_cdef(db, cd).unwrap();
            };
            db.set_discard_ts(3);
            lmax(db);
            get_all_and_check(db, &all);
            lc(db).validate().unwrap();

            db.set_discard_ts(7);
            lmax(db);
            get_all_and_check(
                db,
                &[
                    ("A", "bar", 4, DISCARD),
                    ("Afoo", "baz", 2, 0),
                    ("B", "bar", 4, DISCARD),
                    ("Bfoo", "baz", 2, 0),
                    ("C", "bar", 4, DISCARD),
                    ("Cfoo", "baz", 2, 0),
                ],
            );
            lc(db).validate().unwrap();
        },
    );
}

#[test]
fn table_contains_prefix() {
    run_managed(opts, |db| {
        let t = build_table(db, |b| {
            for k in ["key1", "key3", "key31", "key32", "key4"] {
                for version in [2, 1] {
                    let vs = ValueStruct {
                        value: b"value",
                        ..Default::default()
                    };
                    b.add(&key_with_ts(k.as_bytes(), version), vs, 0);
                }
            }
        });
        t.delete_on_drop();
        for p in ["key", "key1", "key3", "key32", "key4"] {
            assert!(contains_prefix(&t, p.as_bytes()), "{p}");
        }
        for p in ["key0", "key2", "key323", "key5"] {
            assert!(!contains_prefix(&t, p.as_bytes()), "{p}");
        }
    });
}

/// A table of 40,000 stale versions of one 100-byte key starting with
/// `prefix`, made to look 10 hours old.
fn stale_table(db: &DB, prefix: u8) -> Arc<Table> {
    let mut t = build_table(db, |b| {
        let mut key = vec![0u8; 100];
        key[0] = prefix;
        let val = vec![7u8; 500];
        for i in (1..=40_000u64).rev() {
            let vs = ValueStruct {
                value: &val,
                ..Default::default()
            };
            b.add_stale_key(&key_with_ts(&key, i), vs, 0);
        }
    });
    Arc::get_mut(&mut t)
        .unwrap()
        .set_created_at(SystemTime::now() - Duration::from_secs(10 * 3600));
    t
}

/// A compaction whose tables are being picked fails to pick them again
/// until it is unregistered.
#[test]
fn fill_table_cleanup() {
    run_managed(
        |d| Options::new(d).with_num_compactors(0),
        |db| {
            for level in [5, 6] {
                add_to_level(db, stale_table(db, 1), level);
                assert!(lc(db).levels[level].total_stale_size() > 0);
            }
            lc(db).validate().unwrap();
            db.set_discard_ts(1 << 30);
            let mut t = lc(db).level_targets(&db.inner.opt);
            t.file_sz[6] = 1 << 30;
            let mut p = CompactionPriority::new(6, 0.0);
            p.t = Some(t.clone());
            let mut cd = CompactDef {
                p,
                t,
                this_level: 5,
                next_level: 6,
                ..Default::default()
            };
            assert!(lc(db).fill_tables(&db.inner, &mut cd));
            assert!(!lc(db).fill_tables(&db.inner, &mut cd));
            lc(db).cstatus.lock().unwrap().delete(&cd);
            assert!(lc(db).fill_tables(&db.inner, &mut cd));
            lc(db).cstatus.lock().unwrap().delete(&cd);
        },
    );
}

/// A last-level compaction drops stale data.
#[test]
fn stale_data_cleanup() {
    run_managed(
        |d| {
            Options::new(d)
                .with_num_compactors(0)
                .with_lmax_compaction(true)
        },
        |db| {
            for prefix in 1..5 {
                add_to_level(db, stale_table(db, prefix), 6);
            }
            lc(db).validate().unwrap();
            assert!(lc(db).levels[6].total_stale_size() > 0);
            db.set_discard_ts(1 << 30);
            let mut t = lc(db).level_targets(&db.inner.opt);
            t.file_sz[6] = 1 << 30;
            let mut p = CompactionPriority::new(6, 0.0);
            p.t = Some(t);
            assert!(lc(db).do_compact(&db.inner, -1, p).unwrap());
            assert_eq!(lc(db).levels[6].total_stale_size(), 0);
        },
    );
}

/// Huge levels never make L0 compact into itself (Go's
/// TestBaseLevelZeroBySize).
#[test]
fn base_level_zero_by_size() {
    const GB: u64 = 1 << 30;
    const TB: u64 = 1 << 40;
    for lmax in [10 * GB, 100 * GB, 500 * GB, TB, 2 * TB] {
        run_managed(
            |d| Options::new(d).with_num_compactors(0),
            |db| {
                set_size(db, 6, lmax);
                set_size(db, 1, GB);
                set_size(db, 0, GB);
                let t = lc(db).level_targets(&db.inner.opt);
                assert_ne!(t.base_level, 0);
                let mut p = CompactionPriority::new(0, 0.0);
                p.t = Some(t);
                // L0 is empty: nothing to pick, and no panic.
                assert!(!lc(db).do_compact(&db.inner, 0, p).unwrap());
                // Undo the fake sizes before the tables are dropped.
                for l in [0, 1, 6] {
                    set_size(db, l, 0);
                }
            },
        );
    }
}

#[test]
fn level_targets() {
    let data: &[Kvv] = &[("foo", "bar", 3, 0), ("fooz", "baz", 1, 0)];
    let base = Options::new("").base_level_size;
    let mult = Options::new("").level_size_multiplier as u64;
    let max = Options::new("").max_levels;
    let base_level = |db: &DB| lc(db).level_targets(&db.inner.opt).base_level;
    // Restores the real sizes so closing does not trip over fake ones.
    let reset = |db: &DB| {
        for l in &lc(db).levels {
            let mut s = l.write();
            s.total_size = s.tables.iter().map(|t| t.size()).sum();
        }
    };

    // L0 → L6 when L5 is not empty fails.
    run_managed(opts, |db| {
        create_and_open(db, data, 0);
        create_and_open(db, data, max - 2);
        create_and_open(db, data, max - 1);
        set_size(db, max - 1, base * 2);
        set_size(db, max - 2, base / 4);
        let mut cd = cdef(db, 0, max - 1);
        cd.t.base_level = max - 1;
        let err = run_cdef(db, cd).unwrap_err();
        let want = format!(
            "there is a non-empty level {} above base level {}",
            max - 2,
            max - 1
        );
        assert!(err.to_string().contains(&want), "{err}");
        reset(db);
    });
    // L6 below base_level_size: L5 is the base.
    run_managed(opts, |db| {
        create_and_open(db, data, 0);
        create_and_open(db, data, max - 2);
        create_and_open(db, data, max - 1);
        set_size(db, max - 1, base / 2);
        set_size(db, max - 2, base / 4);
        assert_eq!(base_level(db), max - 2);
        reset(db);
    });
    // L5 and L6 below base_level_size, L4 not empty: L4.
    run_managed(opts, |db| {
        create_and_open(db, data, 0);
        for l in [max - 3, max - 2, max - 1] {
            create_and_open(db, data, l);
        }
        set_size(db, max - 1, base / 2);
        set_size(db, max - 2, base / 2);
        set_size(db, max - 3, base / 4);
        assert_eq!(base_level(db), max - 3);
        reset(db);
    });
    // Every level has data: L1.
    for size in [base / 2, base * 2] {
        run_managed(opts, |db| {
            create_and_open(db, data, 0);
            for l in 1..max {
                create_and_open(db, data, l);
                set_size(db, l, size);
            }
            assert_eq!(base_level(db), 1);
            reset(db);
        });
    }
    // L5 above base_level_size: L4.
    run_managed(opts, |db| {
        create_and_open(db, data, 0);
        create_and_open(db, data, max - 2);
        create_and_open(db, data, max - 1);
        set_size(db, max - 2, base * 2);
        set_size(db, max - 1, base * (mult * 2 + 1));
        assert_eq!(base_level(db), max - 3);
        reset(db);
    });
    // Only L0 has data: the last level.
    run_managed(opts, |db| {
        create_and_open(db, data, 0);
        assert_eq!(base_level(db), max - 1);
    });
    // L5 exactly at base_level_size: L5.
    run_managed(opts, |db| {
        create_and_open(db, data, 0);
        create_and_open(db, data, max - 2);
        create_and_open(db, data, max - 1);
        set_size(db, max - 2, base);
        set_size(db, max - 1, base * 2);
        assert_eq!(base_level(db), max - 2);
        reset(db);
    });
}

#[test]
fn level_targets_stray_levels() {
    let data: &[Kvv] = &[("foo", "bar", 3, 0), ("fooz", "baz", 1, 0)];
    let base = Options::new("").base_level_size;
    let max = Options::new("").max_levels;
    let base_level = |db: &DB| lc(db).level_targets(&db.inner.opt).base_level;
    let reset = |db: &DB| {
        for l in &lc(db).levels {
            let mut s = l.write();
            s.total_size = s.tables.iter().map(|t| t.size()).sum();
        }
    };
    // A stray at L2 clamps the base level to it.
    run_managed(opts, |db| {
        create_and_open(db, data, 0);
        create_and_open(db, data, 2);
        create_and_open(db, data, max - 1);
        set_size(db, 2, base / 4);
        set_size(db, max - 1, base * 100);
        assert_eq!(base_level(db), 2);
        reset(db);
    });
    // A collapsed tree keeps the base at the shallowest non-empty level.
    run_managed(opts, |db| {
        create_and_open(db, data, 0);
        for l in [1, 2, 3, max - 1] {
            create_and_open(db, data, l);
            set_size(db, l, base / 4);
        }
        set_size(db, max - 1, base * 100);
        assert_eq!(base_level(db), 1);
        reset(db);
    });
    // Once the strays are gone the base level recovers.
    run_managed(opts, |db| {
        create_and_open(db, data, 0);
        create_and_open(db, data, max - 1);
        set_size(db, max - 1, base * 100);
        assert_eq!(base_level(db), max - 2);
        reset(db);
    });
}

/// A deleted key stays deleted through compactions (Go's
/// TestDeletedKeyReappears, PR #2278).
#[test]
fn deleted_key_reappears() {
    let dir = tempfile::tempdir().unwrap();
    let open = |compact_on_close: bool| {
        DB::open_managed(
            Options::new(dir.path())
                .with_value_log_file_size(1 << 20)
                .with_num_compactors(0)
                .with_base_level_size(16 << 10)
                .with_base_table_size(8 << 10)
                .with_compression(CompressionType::None)
                .with_value_threshold(1 << 20)
                .with_compact_l0_on_close(compact_on_close),
        )
        .unwrap()
    };
    let pad = vec![b'p'; 20 << 10];
    let foo = vec![b'f'; 2 << 10];

    // An expired pad entry big enough that L6 makes L5 the base level.
    let db = open(true);
    let mut txn = db.new_transaction_at(0, true);
    let mut e = Entry::new(b"pad".to_vec(), pad);
    e.expires_at = 1;
    txn.set_entry(e).unwrap();
    txn.commit_at(10).unwrap();
    db.close().unwrap();

    let db = open(true);
    assert_eq!(lc(&db).level_targets(&db.inner.opt).base_level, 5);
    let mut txn = db.new_transaction_at(0, true);
    txn.set(b"foo".to_vec(), foo).unwrap();
    txn.commit_at(20).unwrap();
    db.close().unwrap();

    // Drop the pad from L6, which empties it.
    let db = open(false);
    db.set_discard_ts(15);
    let mut cd = cdef(&db, 6, 6);
    cd.bot.clear();
    run_cdef(&db, cd).unwrap();
    assert_eq!(lc(&db).levels[6].total_size(), 0);
    db.close().unwrap();

    let db = open(true);
    let mut txn = db.new_transaction_at(0, true);
    txn.delete(b"foo".to_vec()).unwrap();
    txn.commit_at(21).unwrap();
    db.close().unwrap();

    let db = open(false);
    db.set_discard_ts(25);
    if !tables(&db, 6).is_empty() {
        let mut cd = cdef(&db, 6, 6);
        cd.bot.clear();
        run_cdef(&db, cd).unwrap();
    }
    compact(&db, 5, 6);
    let err = db.view(|txn| txn.get(b"foo").map(|_| ())).unwrap_err();
    assert!(matches!(err, Error::KeyNotFound), "{err}");
    db.close().unwrap();
}

/// The flush thread waits while L0 is full and continues once compaction
/// (here: removing tables) makes room.
#[test]
fn l0_stall_and_unstall() {
    let dir = tempfile::tempdir().unwrap();
    let opt = Options::new(dir.path())
        .with_num_compactors(0)
        .with_num_level_zero_tables(1)
        .with_num_level_zero_tables_stall(2);
    let db = DB::open_managed(opt).unwrap();
    let data: &[Kvv] = &[("foo", "bar", 1, 0)];
    create_and_open(&db, data, 0);
    create_and_open(&db, data, 0);

    let (tx, rx) = crossbeam_channel::bounded(1);
    std::thread::scope(|s| {
        s.spawn(|| {
            let t = build_table(&db, |b| {
                b.add(&key_with_ts(b"foo", 2), ValueStruct::default(), 0)
            });
            lc(&db).add_level0_table(&db.inner, t).unwrap();
            tx.send(()).unwrap();
        });
        assert!(
            rx.recv_timeout(Duration::from_millis(300)).is_err(),
            "did not stall"
        );
        let first = tables(&db, 0).remove(0);
        lc(&db).levels[0].delete_tables(&[first]);
        lc(&db).signal_l0_drained();
        rx.recv_timeout(Duration::from_secs(5))
            .expect("did not resume");
    });
    assert_eq!(tables(&db, 0).len(), 2);
    db.close().unwrap();
}

/// Closing does not hang on a flush stalled on a full L0, with compaction
/// disabled (Go's TestL0StallCloseNoHang).
#[test]
fn l0_stall_close_no_hang() {
    let dir = tempfile::tempdir().unwrap();
    let opt = Options::new(dir.path())
        .with_mem_table_size(1 << 15)
        .with_value_log_file_size(1 << 20)
        .with_value_threshold(32)
        .with_num_compactors(0)
        .with_num_level_zero_tables(1)
        .with_num_level_zero_tables_stall(2);
    let db = DB::open(opt.clone()).unwrap();
    let val = vec![b'v'; 512];
    for i in 0..400 {
        db.update(|txn| txn.set(format!("key{i:05}"), val.clone()))
            .unwrap();
    }
    db.close().unwrap();
    // Everything is still there.
    let db = DB::open(opt).unwrap();
    db.view(|txn| {
        for i in 0..400 {
            txn.get(format!("key{i:05}").as_bytes())?;
        }
        Ok(())
    })
    .unwrap();
    db.close().unwrap();
}

/// Background compactors keep L0 small, and the tree stays readable.
#[test]
fn background_compaction() {
    let dir = tempfile::tempdir().unwrap();
    let opt = Options::new(dir.path())
        .with_mem_table_size(1 << 15)
        .with_base_table_size(1 << 13)
        .with_base_level_size(1 << 15)
        .with_value_log_file_size(1 << 20)
        .with_value_threshold(64)
        .with_num_level_zero_tables(2)
        .with_num_level_zero_tables_stall(4)
        .with_compression(CompressionType::None);
    let db = DB::open(opt.clone()).unwrap();
    for round in 0..20u32 {
        for chunk in 0..10u32 {
            db.update(|txn| {
                for i in chunk * 20..(chunk + 1) * 20 {
                    txn.set(format!("key{i:05}"), format!("value-{round}-{i}").repeat(4))?;
                }
                Ok(())
            })
            .unwrap();
        }
    }
    // Wait for compaction to move data below L0.
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    while db.tables().iter().all(|t| t.level == 0) {
        assert!(std::time::Instant::now() < deadline, "no compaction ran");
        std::thread::sleep(Duration::from_millis(50));
    }
    db.view(|txn| {
        for i in 0..200u32 {
            let v = txn.get(format!("key{i:05}").as_bytes())?.value_copy()?;
            assert_eq!(v, format!("value-19-{i}").repeat(4).into_bytes());
        }
        Ok(())
    })
    .unwrap();
    let levels = db.levels();
    assert_eq!(levels.len(), opt.max_levels);
    assert_eq!(levels.iter().filter(|l| l.is_base_level).count(), 1);
    db.close().unwrap();
}

#[test]
fn key_range_of_tables() {
    run_managed(opts, |db| {
        create_and_open(db, &[("b", "x", 3, 0), ("d", "x", 1, 0)], 1);
        create_and_open(db, &[("a", "x", 2, 0)], 0);
        let mut all = tables(db, 0);
        all.extend(tables(db, 1));
        let kr = KeyRange::of_tables(&all);
        assert_eq!(kr.left, key_with_ts(b"a", u64::MAX));
        assert_eq!(kr.right, key_with_ts(b"d", 0));
    });
}
