//! Port of Go's `backup_test.go`.

use std::collections::HashMap;
use std::path::Path;

use crate::iterator::IteratorOptions;
use crate::structs::{BIT_DELETE, BIT_DISCARD_EARLIER_VERSIONS};
use crate::{DB, Entry, Error, Options};

/// Go's getTestOptions.
fn test_options(dir: &Path) -> Options {
    Options::new(dir).with_sync_writes(false)
}

fn next_ts(db: &DB) -> u64 {
    db.inner.orc.next_ts()
}

/// Keys `key0..key{n-1}`, each set to `[1]` in one transaction (version 1).
fn populate(db: &DB, n: usize) -> Vec<Vec<u8>> {
    let keys: Vec<Vec<u8>> = (0..n).map(|i| format!("key{i}").into_bytes()).collect();
    db.update(|txn| {
        for k in &keys {
            txn.set(k.clone(), vec![1])?;
        }
        Ok(())
    })
    .unwrap();
    keys
}

/// The latest keys, checked against `keys`.
fn count_latest(db: &DB, keys: &[Vec<u8>]) -> usize {
    db.view(|txn| {
        let mut it = txn.iter(IteratorOptions::default());
        it.rewind();
        let mut count = 0;
        while it.valid() {
            let k = it.item().key();
            let idx: usize = std::str::from_utf8(&k[3..]).unwrap().parse().unwrap();
            assert_eq!(keys[idx], k);
            count += 1;
            it.next();
        }
        Ok(count)
    })
    .unwrap()
}

#[test]
fn backup_restore1() {
    let src_dir = tempfile::tempdir().unwrap();
    let db = DB::open(test_options(src_dir.path())).unwrap();
    let entries: [(&[u8], &[u8], u8, u64); 2] =
        [(b"answer1", b"42", 0, 1), (b"answer2", b"43", 1, 2)];
    for (k, v, m, _) in entries {
        db.update(|txn| txn.set_entry(Entry::new(k, v).with_meta(m)))
            .unwrap();
    }
    let mut bak = Vec::new();
    db.backup(&mut bak, 0).unwrap();
    db.close().unwrap();

    let dir = tempfile::tempdir().unwrap();
    let db = DB::open(test_options(dir.path())).unwrap();
    db.load(bak.as_slice(), 16).unwrap();
    db.view(|txn| {
        let mut it = txn.iter(IteratorOptions {
            all_versions: true,
            ..Default::default()
        });
        it.rewind();
        let mut count = 0;
        while it.valid() {
            let item = it.item();
            let (k, v, m, ver) = entries[count];
            assert_eq!(item.key(), k);
            assert_eq!(item.value_copy()?, v);
            assert_eq!(item.version(), ver);
            assert_eq!(item.user_meta(), m);
            count += 1;
            it.next();
        }
        assert_eq!(count, 2);
        Ok(())
    })
    .unwrap();
    assert_eq!(next_ts(&db), 3);
    db.close().unwrap();
}

#[test]
fn backup_restore2() {
    let tmp = tempfile::tempdir().unwrap();
    let open = |name: &str| DB::open(test_options(&tmp.path().join(name))).unwrap();
    let (key1, key2, raw) = (b"key1".to_vec(), b"key2".to_vec(), b"NotLongValue".to_vec());
    let k = |base: &[u8], i: u8| [base, &[i]].concat();
    let n = 251u8;
    let check = |db: &DB| {
        db.view(|txn| {
            for i in 1..n {
                let key = k(&key1, i);
                let item = txn
                    .get(&key)
                    .unwrap_or_else(|e| panic!("Key {key:?} has been not found, but was set: {e}"));
                assert_eq!(item.value_copy()?, raw);
            }
            Ok(())
        })
        .unwrap();
    };

    let db1 = open("test1");
    db1.update(|txn| {
        txn.set(key1.clone(), raw.clone())?;
        txn.set(key2.clone(), raw.clone())
    })
    .unwrap();
    for i in 1..n {
        db1.update(|txn| {
            txn.set(k(&key1, i), raw.clone())?;
            txn.set(k(&key2, i), raw.clone())
        })
        .unwrap();
    }
    let mut backup = Vec::new();
    db1.backup(&mut backup, 0).unwrap();

    let db2 = open("test2");
    db2.load(backup.as_slice(), 16).unwrap();
    assert_eq!(next_ts(&db1), next_ts(&db2));
    check(&db2);
    for i in 1..n {
        db2.update(|txn| {
            txn.set(k(&key1, i), raw.clone())?;
            txn.set(k(&key2, i), raw.clone())
        })
        .unwrap();
    }
    backup.clear();
    db2.backup(&mut backup, 0).unwrap();

    let db3 = open("test3");
    db3.load(backup.as_slice(), 16).unwrap();
    assert_eq!(next_ts(&db2), next_ts(&db3));
    check(&db3);
    for db in [db1, db2, db3] {
        db.close().unwrap();
    }
}

#[test]
fn backup() {
    let test = |db: &DB| {
        let keys = populate(db, 1000);
        let mut bb = Vec::new();
        db.backup(&mut bb, 0).unwrap();
        assert!(!bb.is_empty());
        assert_eq!(count_latest(db, &keys), 1000);
    };
    let dir = tempfile::tempdir().unwrap();
    let db = DB::open(Options::new(dir.path().join("backup0"))).unwrap();
    test(&db);
    db.close().unwrap();
    let db = DB::open(Options::new("").with_in_memory(true)).unwrap();
    test(&db);
    db.close().unwrap();
}

#[test]
fn backup_restore3() {
    let tmp = tempfile::tempdir().unwrap();
    let mut bb = Vec::new();
    let db1 = DB::open(Options::new(tmp.path().join("backup1"))).unwrap();
    let keys = populate(&db1, 1000);
    db1.backup(&mut bb, 0).unwrap();
    let db1_next = next_ts(&db1);
    db1.close().unwrap();
    assert!(!bb.is_empty());

    let db2 = DB::open(Options::new(tmp.path().join("restore1"))).unwrap();
    assert_ne!(db1_next, next_ts(&db2));
    db2.load(bb.as_slice(), 16).unwrap();
    assert_eq!(db1_next, next_ts(&db2));
    assert_eq!(count_latest(&db2, &keys), 1000);
    db2.close().unwrap();
}

#[test]
fn backup_load_incremental() {
    let tmp = tempfile::tempdir().unwrap();
    let n = 100;
    let mut updates: HashMap<usize, u8> = HashMap::new();
    let mut bb = Vec::new();
    let db1 = DB::open(Options::new(tmp.path().join("backup2"))).unwrap();
    let keys = populate(&db1, n);
    let mut since = db1.backup(&mut bb, 0).unwrap();
    // A fixed permutation (Go uses a random one).
    let ints: Vec<usize> = (0..n).map(|i| (i * 37 + 11) % n).collect();

    db1.update(|txn| {
        for &i in &ints[..10] {
            txn.delete(keys[i].clone())?;
            updates.insert(i, BIT_DELETE);
        }
        Ok(())
    })
    .unwrap();
    since = db1.backup(&mut bb, since).unwrap();

    let an_hour_ago = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
        - 3600;
    db1.update(|txn| {
        for &i in &ints[10..15] {
            let mut e = Entry::new(keys[i].clone(), vec![1]);
            e.expires_at = an_hour_ago;
            txn.set_entry(e)?;
            updates.insert(i, BIT_DELETE); // expired
        }
        Ok(())
    })
    .unwrap();
    since = db1.backup(&mut bb, since).unwrap();

    db1.update(|txn| {
        for &i in &ints[15..20] {
            txn.set_entry(Entry::new(keys[i].clone(), vec![1]).with_discard())?;
            updates.insert(i, BIT_DISCARD_EARLIER_VERSIONS);
        }
        Ok(())
    })
    .unwrap();
    db1.backup(&mut bb, since).unwrap();
    let db1_next = next_ts(&db1);
    db1.close().unwrap();

    let db2 = DB::open(test_options(&tmp.path().join("restore2"))).unwrap();
    assert_ne!(db1_next, next_ts(&db2));
    db2.load(bb.as_slice(), 16).unwrap();
    assert_eq!(db1_next, next_ts(&db2));

    // Every updated key shows a delete or expired version.
    let mut deleted = Vec::new();
    db2.view(|txn| {
        let mut it = txn.iter(IteratorOptions {
            all_versions: true,
            ..Default::default()
        });
        it.rewind();
        while it.valid() {
            let item = it.item();
            if item.is_deleted_or_expired() {
                let idx: usize = std::str::from_utf8(&item.key()[3..])
                    .unwrap()
                    .parse()
                    .unwrap();
                assert!(
                    updates.contains_key(&idx),
                    "key{idx} not expected to be updated"
                );
                deleted.push(idx);
            }
            it.next();
        }
        Ok(())
    })
    .unwrap();
    assert_eq!(deleted.len(), updates.len(), "{updates:?} {deleted:?}");
    // The keys written with "discard earlier versions" read as set.
    db2.view(|txn| {
        for &i in &ints[15..20] {
            assert_eq!(txn.get(&keys[i])?.value_copy()?, [1]);
        }
        for &i in &ints[..15] {
            assert!(matches!(txn.get(&keys[i]), Err(Error::KeyNotFound)));
        }
        Ok(())
    })
    .unwrap();
    db2.close().unwrap();
}

#[test]
fn backup_bit_clear() {
    // A value in the source's value log is inline in a DB with a bigger
    // threshold: the value pointer bit from the backup must be cleared.
    let src = tempfile::tempdir().unwrap();
    let db = DB::open(test_options(src.path()).with_value_threshold(10)).unwrap();
    let val = format!("{:0100}", 1).into_bytes();
    db.update(|txn| txn.set("foo", val.clone())).unwrap();
    let mut bak = Vec::new();
    db.backup(&mut bak, 0).unwrap();
    let old_next = next_ts(&db);
    db.close().unwrap();

    let dir = tempfile::tempdir().unwrap();
    let db = DB::open(test_options(dir.path()).with_value_threshold(200)).unwrap();
    db.load(bak.as_slice(), 16).unwrap();
    assert_eq!(old_next, next_ts(&db));
    let v = db.view(|txn| txn.get(b"foo")?.value_copy()).unwrap();
    assert_eq!(v, val);
    db.close().unwrap();
}

#[test]
fn load_rejects_truncated_backup() {
    let dir = tempfile::tempdir().unwrap();
    let db = DB::open(test_options(dir.path())).unwrap();
    populate(&db, 10);
    let mut bak = Vec::new();
    db.backup(&mut bak, 0).unwrap();
    let other = tempfile::tempdir().unwrap();
    let db2 = DB::open(test_options(other.path())).unwrap();
    // Cut inside the length prefix, then inside the list.
    assert!(db2.load(&bak[..4], 16).is_err());
    assert!(db2.load(&bak[..bak.len() - 1], 16).is_err());
    db2.close().unwrap();
    db.close().unwrap();
}

#[test]
fn failed_load_still_advances_the_oracle() {
    let dir = tempfile::tempdir().unwrap();
    let db = DB::open(test_options(dir.path())).unwrap();
    for i in 0..20 {
        db.update(|txn| txn.set(format!("key{i}"), vec![1]))
            .unwrap();
    }
    let mut bak = Vec::new();
    let max_version = db.backup(&mut bak, 0).unwrap();
    assert_eq!(max_version, 20);
    // A list that does not decode follows the good ones.
    bak.extend_from_slice(&5u64.to_le_bytes());
    bak.extend_from_slice(&[0xff; 5]);
    let other = tempfile::tempdir().unwrap();
    let db2 = DB::open(test_options(other.path())).unwrap();
    assert!(db2.load(bak.as_slice(), 16).is_err());
    // The KVs written before the error are below every new commit.
    assert!(next_ts(&db2) > max_version);
    db2.close().unwrap();
    db.close().unwrap();
}

#[test]
fn load_rejects_huge_length_prefix() {
    // Must fail cleanly, not try to allocate the claimed size.
    let dir = tempfile::tempdir().unwrap();
    let db = DB::open(test_options(dir.path())).unwrap();
    let mut bak = u64::MAX.to_le_bytes().to_vec();
    bak.extend_from_slice(&[0; 16]);
    assert!(db.load(bak.as_slice(), 16).is_err());
    db.close().unwrap();
}

#[test]
fn backup_fails_on_versions_below_since() {
    // Without since_ts, every key is older than `since`: the backup must fail,
    // not silently leave them out.
    let dir = tempfile::tempdir().unwrap();
    let db = DB::open(test_options(dir.path())).unwrap();
    populate(&db, 10);
    let mut stream = db.new_stream();
    let mut bak = Vec::new();
    let err = stream.backup(&mut bak, 5).unwrap_err();
    assert!(matches!(err.root(), Error::InvalidRequest), "{err}");
    drop(stream);
    db.close().unwrap();
}
