//! The value log: values too big for the LSM tree, stored in `<fid>.vlog`
//! files (same format as the WAL, see [`crate::log_file`]) and referenced by
//! [`ValuePointer`]s. Port of the read side of Go's `value.go`; writing and
//! garbage collection come in Phases 5 and 6.

use std::borrow::Cow;
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use crate::error::{Error, Result, ResultExt};
use crate::key_registry::KeyRegistry;
use crate::log_file::LogFile;
use crate::options::Options;
use crate::structs::{Header, ValuePointer};

/// Extension of value log files.
pub const VLOG_FILE_EXT: &str = ".vlog";

/// Path of value log file `fid` (Go's `vlogFilePath`).
pub fn vlog_file_path(dir: &Path, fid: u32) -> PathBuf {
    dir.join(format!("{fid:06}{VLOG_FILE_EXT}"))
}

pub(crate) struct ValueLog {
    dir: PathBuf,
    /// Open files by id. Each file has its own lock (Go's `logFile.lock`):
    /// readers share it while they use the mapped bytes.
    files: RwLock<BTreeMap<u32, Arc<RwLock<LogFile>>>>,
    verify_value_checksum: bool,
}

impl ValueLog {
    /// Opens the value log files in `opt.value_dir` (Go's `valueLog.open`).
    ///
    /// Only read-only mode is supported so far: Phase 5 adds truncating the
    /// last file and creating a new one for writes.
    pub fn open(opt: &Options, registry: &KeyRegistry) -> Result<ValueLog> {
        let vlog = ValueLog {
            dir: opt.value_dir.clone(),
            files: RwLock::new(BTreeMap::new()),
            verify_value_checksum: opt.verify_value_checksum,
        };
        if opt.in_memory {
            return Ok(vlog);
        }
        if !opt.read_only {
            return Err(Error::InvalidRequest)
                .context("opening the value log for writing is not implemented yet");
        }
        let mut files = BTreeMap::new();
        for entry in fs::read_dir(&vlog.dir)
            .with_context(|| format!("Unable to open log dir: {}", vlog.dir.display()))?
        {
            let name = entry?.file_name();
            let name = name.to_string_lossy();
            let Some(stem) = name.strip_suffix(VLOG_FILE_EXT) else {
                continue;
            };
            let fid: u32 = stem
                .parse()
                .map_err(|e| Error::corruption(format!("{e}")))
                .with_context(|| format!("{name}: Unable to parse log id."))?;
            let path = vlog_file_path(&vlog.dir, fid);
            let (lf, _) = LogFile::open(&path, fid, false, true, 0, registry)
                .with_context(|| format!("Open existing file: {}", path.display()))?;
            if files.insert(fid, Arc::new(RwLock::new(lf))).is_some() {
                return Err(Error::corruption(format!(
                    "{name}: Duplicate file found. Please delete one."
                )));
            }
        }
        *vlog.files.write().unwrap() = files;
        Ok(vlog)
    }

    /// Reads the value `vp` points at and passes it to `f` (Go's `Read`).
    /// Unencrypted values are passed straight from the mapped file.
    pub fn read<R>(&self, vp: ValuePointer, f: impl FnOnce(&[u8]) -> R) -> Result<R> {
        let lf = self
            .files
            .read()
            .unwrap()
            .get(&vp.fid)
            .cloned()
            .ok_or_else(|| Error::corruption(format!("file with ID: {} not found", vp.fid)))?;
        let lf = lf.read().unwrap();
        let buf = lf.read(vp)?;
        if buf.len() < 4 {
            return Err(Error::corruption(format!("Invalid read: vp: {vp:?}")));
        }
        if self.verify_value_checksum {
            let (data, crc) = buf.split_at(buf.len() - 4);
            let expected = u32::from_be_bytes(crc.try_into().unwrap());
            let actual = crc32c::crc32c(data);
            if actual != expected {
                return Err(Error::ChecksumMismatch {
                    actual: actual as u64,
                    expected: expected as u64,
                })
                .with_context(|| format!("value corrupted for vp: {vp:?}"));
            }
        }
        let (h, hlen) = Header::decode(buf)?;
        let kv = &buf[hlen..];
        let kv: Cow<'_, [u8]> = if lf.encryption_enabled() {
            Cow::Owned(lf.decrypt_kv(kv, vp.offset)?)
        } else {
            Cow::Borrowed(kv)
        };
        let (klen, vlen) = (h.klen as usize, h.vlen as usize);
        let Some(value) = kv.get(klen..klen + vlen) else {
            return Err(Error::corruption(format!(
                "Invalid read: Len: {} read at:[{klen}:{}]",
                kv.len(),
                klen + vlen
            )));
        };
        Ok(f(value))
    }

    /// Ids of the open files, ascending.
    #[allow(dead_code)] // used by GC and the write path (Phases 5–6)
    pub fn sorted_fids(&self) -> Vec<u32> {
        self.files.read().unwrap().keys().copied().collect()
    }
}
