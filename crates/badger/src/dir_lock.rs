//! Directory lock: keeps two processes from opening the same DB. Port of Go's
//! `dir_unix.go`.
//!
//! The directory itself is `flock`ed: shared for read-only opens, exclusive
//! otherwise. A read-write open also writes its pid to a `LOCK` file, purely
//! as information for humans. On non-Unix platforms there is no lock (Go
//! uses a different mechanism on Windows, not ported).

use std::fs::{self, File};
use std::path::{Path, PathBuf};

use crate::error::{Error, Result, ResultExt};

/// Name of the pid file.
pub const LOCK_FILE: &str = "LOCK";

/// Holds the lock until dropped.
#[derive(Debug)]
pub struct DirLockGuard {
    _dir: File,
    /// The pid file to remove on release, for read-write locks.
    pid_file: Option<PathBuf>,
}

impl DirLockGuard {
    pub fn acquire(dir: &Path, read_only: bool) -> Result<DirLockGuard> {
        let pid_path = std::path::absolute(dir.join(LOCK_FILE))
            .context("cannot get absolute path for pid lock file")?;
        let f = File::open(dir).with_context(|| format!("cannot open directory {dir:?}"))?;
        #[cfg(unix)]
        {
            let res = if read_only {
                f.try_lock_shared()
            } else {
                f.try_lock()
            };
            res.map_err(|e| Error::Io(std::io::Error::other(e)))
                .with_context(|| {
                    format!(
                        "Cannot acquire directory lock on {dir:?}.  \
                         Another process is using this Badger database."
                    )
                })?;
        }
        let pid_file = if read_only {
            None
        } else {
            fs::write(&pid_path, format!("{}\n", std::process::id()))
                .with_context(|| format!("Cannot write pid file {pid_path:?}"))?;
            Some(pid_path)
        };
        Ok(DirLockGuard { _dir: f, pid_file })
    }
}

impl Drop for DirLockGuard {
    fn drop(&mut self) {
        // Remove the pid file before the lock goes (with the file handle).
        if let Some(p) = &self.pid_file {
            let _ = fs::remove_file(p);
        }
    }
}

#[cfg(all(test, unix, not(miri)))]
mod tests {
    use super::*;

    #[test]
    fn exclusive_and_shared() {
        let dir = tempfile::tempdir().unwrap();
        let rw = DirLockGuard::acquire(dir.path(), false).unwrap();
        assert!(dir.path().join(LOCK_FILE).exists());
        assert!(DirLockGuard::acquire(dir.path(), false).is_err());
        assert!(DirLockGuard::acquire(dir.path(), true).is_err());
        drop(rw);
        assert!(!dir.path().join(LOCK_FILE).exists());

        let ro1 = DirLockGuard::acquire(dir.path(), true).unwrap();
        let ro2 = DirLockGuard::acquire(dir.path(), true).unwrap();
        assert!(DirLockGuard::acquire(dir.path(), false).is_err());
        drop((ro1, ro2));
        DirLockGuard::acquire(dir.path(), false).unwrap();
    }
}
