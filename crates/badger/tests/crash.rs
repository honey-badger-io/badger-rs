//! Crash recovery: a child process writes transactions and reports each
//! acknowledged commit on stdout; the parent kills it with SIGKILL at a
//! random point, reopens the DB (replaying WALs and truncating the value log)
//! and checks that:
//!
//! - every acknowledged transaction is fully present,
//! - every transaction is all-or-nothing,
//! - there are no gaps (commits are applied in order).
//!
//! This repeats on the same directory, so later rounds also recover from a
//! crash during or right after an earlier recovery. Compaction runs, and the
//! child rewrites older transactions with the same values and runs value log
//! GC now and then, so kills also land in the middle of those.

#![cfg(unix)]

use std::io::{BufRead, BufReader};
use std::path::Path;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use badger::{DB, Error, Options};

const CHILD_ENV: &str = "BADGER_CRASH_CHILD_DIR";
const KEYS_PER_TXN: u64 = 5;

fn options(dir: &Path) -> Options {
    Options::new(dir)
        .with_mem_table_size(64 << 10)
        .with_value_threshold(64)
        .with_value_log_file_size(1 << 20)
        .with_base_table_size(32 << 10)
        .with_base_level_size(128 << 10)
        .with_num_level_zero_tables(2)
        .with_num_compactors(2)
}

fn key(txn: u64, j: u64) -> Vec<u8> {
    format!("t{txn:08}-{j}").into_bytes()
}

/// Values vary in size so some go to the value log.
fn value(txn: u64, j: u64) -> Vec<u8> {
    let len = ((txn * 31 + j * 17) % 300) as usize;
    let mut v = format!("{txn}:{j}:").into_bytes();
    v.resize(v.len() + len, b'a' + (txn % 26) as u8);
    v
}

/// Finds the first transaction number not yet written (after recovery).
fn next_txn(db: &DB) -> u64 {
    db.view(|txn| {
        let mut n = 0;
        while txn.get(&key(n, 0)).is_ok() {
            n += 1;
        }
        Ok(n)
    })
    .unwrap()
}

/// The child: writes transactions until killed.
#[test]
#[ignore = "run by crash_recovery as a child process"]
fn crash_child() {
    let Some(dir) = std::env::var_os(CHILD_ENV) else {
        return;
    };
    let db = DB::open(options(Path::new(&dir))).unwrap();
    let mut n = next_txn(&db);
    println!("start {n}");
    loop {
        db.update(|txn| {
            for j in 0..KEYS_PER_TXN {
                txn.set(key(n, j), value(n, j))?;
            }
            Ok(())
        })
        .unwrap();
        println!("committed {n}");
        // Garbage for compaction and GC: write an older transaction again,
        // with the same values, so the checks do not change.
        if n.is_multiple_of(3) {
            let old = n / 2;
            db.update(|txn| {
                for j in 0..KEYS_PER_TXN {
                    txn.set(key(old, j), value(old, j))?;
                }
                Ok(())
            })
            .unwrap();
        }
        if n.is_multiple_of(200) {
            while db.run_value_log_gc(0.2).is_ok() {}
        }
        n += 1;
    }
}

/// Checks the invariants; returns the number of complete transactions.
fn verify(dir: &Path, acked: Option<u64>) -> u64 {
    let db = DB::open(options(dir)).unwrap();
    let n = next_txn(&db);
    db.view(|txn| {
        for i in 0..n + 5 {
            let present: Vec<bool> = (0..KEYS_PER_TXN)
                .map(|j| match txn.get(&key(i, j)) {
                    Ok(item) => {
                        assert_eq!(item.value_copy().unwrap(), value(i, j), "txn {i} key {j}");
                        true
                    }
                    Err(Error::KeyNotFound) => false,
                    Err(e) => panic!("txn {i}: {e}"),
                })
                .collect();
            let expected = i < n;
            assert!(
                present.iter().all(|&p| p == expected),
                "txn {i}: partially applied or out of order: {present:?} (complete up to {n})"
            );
        }
        Ok(())
    })
    .unwrap();
    if let Some(acked) = acked {
        assert!(n > acked, "acknowledged txn {acked} lost (have {n})");
    }
    db.close().unwrap();
    n
}

#[test]
fn crash_recovery() {
    let dir = tempfile::tempdir().unwrap();
    let exe = std::env::current_exe().unwrap();
    let mut seed = 0x5eed_u64;
    let mut total = 0;
    // More rounds locally: BADGER_CRASH_ROUNDS=100 cargo test --test crash
    let rounds: u32 = std::env::var("BADGER_CRASH_ROUNDS")
        .ok()
        .and_then(|r| r.parse().ok())
        .unwrap_or(8);
    for round in 0..rounds {
        seed = seed
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        let commits_before_kill = 20 + (seed >> 33) % 400;
        let extra_delay = Duration::from_micros((seed >> 20) % 3000);

        let mut child = Command::new(&exe)
            .args([
                "crash_child",
                "--exact",
                "--ignored",
                "--nocapture",
                "--test-threads=1",
            ])
            .env(CHILD_ENV, dir.path())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let stdout = child.stdout.take().unwrap();
        let mut acked = None;
        let mut seen = 0;
        let deadline = Instant::now() + Duration::from_secs(60);
        for line in BufReader::new(stdout).lines() {
            let line = line.unwrap();
            if let Some(n) = line.strip_prefix("committed ") {
                acked = Some(n.parse::<u64>().unwrap());
                seen += 1;
                if seen == commits_before_kill || Instant::now() > deadline {
                    break;
                }
            }
        }
        // Let it get into the middle of another write, then kill -9.
        std::thread::sleep(extra_delay);
        child.kill().unwrap();
        child.wait().unwrap();
        assert!(
            acked.is_some(),
            "round {round}: the child committed nothing"
        );

        let n = verify(dir.path(), acked);
        assert!(n >= total, "round {round}: went back from {total} to {n}");
        total = n;
    }
    assert!(total > 100, "only {total} transactions");
}
