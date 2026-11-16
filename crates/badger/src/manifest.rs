//! The manifest: which tables exist, at which level, with which data key and
//! compression. Port of Go's `manifest.go`.
//!
//! File layout (`MANIFEST`):
//!
//! ```text
//! +--------+------------------+-------------------+------------------+-----
//! | "Bdgr" | ext magic (u16)  | version (u16) = 8 | change set       | change set ...
//! +--------+------------------+-------------------+------------------+-----
//! change set = len (u32 BE) | crc32c (u32 BE) | pb::ManifestChangeSet
//! ```
//!
//! Replay applies change sets in order and stops at a truncated one (a crash
//! mid-append); the file is then truncated there (unless read-only). When
//! deletions dominate, the file is rewritten from the current state via
//! `MANIFEST-REWRITE` and a rename.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::{ErrorKind, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use prost::Message;

use crate::error::{Error, Result, ResultExt};
use crate::options::{CompressionType, Options};
use crate::pb::{self, manifest_change::Operation};
use crate::util::sync_dir;

pub const MANIFEST_FILENAME: &str = "MANIFEST";
pub const MANIFEST_REWRITE_FILENAME: &str = "MANIFEST-REWRITE";
/// Rewrite once there are more deletions than this...
const MANIFEST_DELETIONS_REWRITE_THRESHOLD: usize = 10_000;
/// ...and more than this many times the live tables.
const MANIFEST_DELETIONS_RATIO: usize = 10;

const MAGIC_TEXT: &[u8; 4] = b"Bdgr";
/// Bumped by Go Badger on incompatible format changes.
pub const BADGER_MAGIC_VERSION: u16 = 8;

/// The tables of the DB, as recorded by the manifest.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Manifest {
    /// Table ids per level. Levels only exist up to the highest one used.
    pub levels: Vec<BTreeSet<u64>>,
    pub tables: BTreeMap<u64, TableManifest>,
    /// Creations and deletions applied, to decide when to rewrite.
    pub creations: usize,
    pub deletions: usize,
}

/// What the manifest knows about one table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TableManifest {
    pub level: u8,
    /// Data key the table is encrypted with; 0 = not encrypted.
    pub key_id: u64,
    pub compression: CompressionType,
}

impl Manifest {
    /// Changes that recreate this manifest from scratch.
    pub fn as_changes(&self) -> Vec<pb::ManifestChange> {
        self.tables
            .iter()
            .map(|(&id, tm)| new_create_change(id, tm.level as u32, tm.key_id, tm.compression))
            .collect()
    }

    fn apply_change(&mut self, tc: &pb::ManifestChange) -> Result<()> {
        match Operation::try_from(tc.op) {
            Ok(Operation::Create) => {
                if self.tables.contains_key(&tc.id) {
                    return Err(Error::corruption(format!(
                        "MANIFEST invalid, table {} exists",
                        tc.id
                    )));
                }
                let level = u8::try_from(tc.level).map_err(|_| {
                    Error::corruption(format!("MANIFEST invalid, level {}", tc.level))
                })?;
                self.tables.insert(
                    tc.id,
                    TableManifest {
                        level,
                        key_id: tc.key_id,
                        compression: CompressionType::try_from(tc.compression)?,
                    },
                );
                while self.levels.len() <= level as usize {
                    self.levels.push(BTreeSet::new());
                }
                self.levels[level as usize].insert(tc.id);
                self.creations += 1;
            }
            Ok(Operation::Delete) => {
                match self.tables.remove(&tc.id) {
                    Some(tm) => {
                        self.levels[tm.level as usize].remove(&tc.id);
                    }
                    None => {
                        // Go only warns here.
                        for level in &mut self.levels {
                            level.remove(&tc.id);
                        }
                    }
                }
                self.deletions += 1;
            }
            Err(_) => {
                return Err(Error::corruption(
                    "MANIFEST file has invalid manifestChange op",
                ));
            }
        }
        Ok(())
    }

    /// Applies every change in `cs`, in order.
    pub fn apply_change_set(&mut self, cs: &pb::ManifestChangeSet) -> Result<()> {
        cs.changes.iter().try_for_each(|c| self.apply_change(c))
    }

    /// A copy rebuilt from [`Manifest::as_changes`], so with fresh counters.
    fn compacted(&self) -> Manifest {
        let mut m = Manifest::default();
        m.apply_change_set(&pb::ManifestChangeSet {
            changes: self.as_changes(),
        })
        .expect("a manifest's own changes apply cleanly");
        m
    }
}

/// A change that adds table `id` at `level`.
pub fn new_create_change(
    id: u64,
    level: u32,
    key_id: u64,
    compression: CompressionType,
) -> pb::ManifestChange {
    pb::ManifestChange {
        id,
        op: Operation::Create as i32,
        level,
        key_id,
        encryption_algo: pb::EncryptionAlgo::Aes as i32,
        compression: compression as u32,
    }
}

/// A change that removes table `id`.
pub fn new_delete_change(id: u64) -> pb::ManifestChange {
    pb::ManifestChange {
        id,
        op: Operation::Delete as i32,
        ..Default::default()
    }
}

/// The open manifest file, for appending changes.
#[derive(Debug)]
pub struct ManifestFile {
    inner: Mutex<Inner>,
    dir: PathBuf,
    external_magic: u16,
    deletions_rewrite_threshold: usize,
    in_memory: bool,
}

#[derive(Debug)]
struct Inner {
    /// `None` in in-memory or read-only mode.
    file: Option<File>,
    /// The state after all changes so far.
    manifest: Manifest,
}

impl ManifestFile {
    /// Opens the manifest in `opt.dir`, creating it if missing (unless
    /// read-only). Returns the file and the manifest it holds.
    pub fn open_or_create(opt: &Options) -> Result<(ManifestFile, Manifest)> {
        if opt.in_memory {
            let mf = ManifestFile {
                inner: Mutex::new(Inner {
                    file: None,
                    manifest: Manifest::default(),
                }),
                dir: PathBuf::new(),
                external_magic: opt.external_magic_version,
                deletions_rewrite_threshold: MANIFEST_DELETIONS_REWRITE_THRESHOLD,
                in_memory: true,
            };
            return Ok((mf, Manifest::default()));
        }
        Self::help_open_or_create(
            &opt.dir,
            opt.read_only,
            opt.external_magic_version,
            MANIFEST_DELETIONS_REWRITE_THRESHOLD,
        )
    }

    fn help_open_or_create(
        dir: &Path,
        read_only: bool,
        ext_magic: u16,
        deletions_threshold: usize,
    ) -> Result<(ManifestFile, Manifest)> {
        let path = dir.join(MANIFEST_FILENAME);
        let new = |file: Option<File>, manifest: &Manifest| ManifestFile {
            inner: Mutex::new(Inner {
                file,
                manifest: manifest.compacted(),
            }),
            dir: dir.to_path_buf(),
            external_magic: ext_magic,
            deletions_rewrite_threshold: deletions_threshold,
            in_memory: false,
        };
        let mut file = match OpenOptions::new().read(true).write(!read_only).open(&path) {
            Ok(f) => f,
            Err(e) if e.kind() == ErrorKind::NotFound => {
                if read_only {
                    return Err(Error::InvalidRequest)
                        .context("no manifest found, required for read-only db");
                }
                let m = Manifest::default();
                let (file, net_creations) = help_rewrite(dir, &m, ext_magic)?;
                debug_assert_eq!(net_creations, 0);
                return Ok((new(Some(file), &m), m));
            }
            Err(e) => return Err(e.into()),
        };
        let (manifest, trunc_offset) = replay_manifest_file(&mut file, ext_magic)?;
        if read_only {
            return Ok((new(None, &manifest), manifest));
        }
        file.set_len(trunc_offset)?;
        file.seek(SeekFrom::End(0))?;
        Ok((new(Some(file), &manifest), manifest))
    }

    /// Appends `changes` as one change set and syncs (Go's `addChanges`).
    pub fn add_changes(&self, changes: Vec<pb::ManifestChange>) -> Result<()> {
        if self.in_memory {
            return Ok(());
        }
        let cs = pb::ManifestChangeSet { changes };
        let buf = cs.encode_to_vec();
        let mut inner = self.inner.lock().unwrap();
        if inner.file.is_none() {
            return Err(Error::InvalidRequest).context("manifest is read-only");
        }
        inner.manifest.apply_change_set(&cs)?;
        let m = &inner.manifest;
        if m.deletions > self.deletions_rewrite_threshold
            && m.deletions > MANIFEST_DELETIONS_RATIO * (m.creations.saturating_sub(m.deletions))
        {
            self.rewrite(&mut inner)?;
        } else {
            let file = inner.file.as_mut().unwrap();
            let mut rec = Vec::with_capacity(8 + buf.len());
            rec.extend_from_slice(&(buf.len() as u32).to_be_bytes());
            rec.extend_from_slice(&crc32c::crc32c(&buf).to_be_bytes());
            rec.extend_from_slice(&buf);
            file.write_all(&rec)?;
        }
        inner.file.as_ref().unwrap().sync_all()?;
        Ok(())
    }

    fn rewrite(&self, inner: &mut Inner) -> Result<()> {
        inner.file = None; // close before the rename (Windows)
        let (file, net_creations) = help_rewrite(&self.dir, &inner.manifest, self.external_magic)?;
        inner.file = Some(file);
        inner.manifest.creations = net_creations;
        inner.manifest.deletions = 0;
        Ok(())
    }

    /// A copy of the current manifest.
    pub fn manifest(&self) -> Manifest {
        self.inner.lock().unwrap().manifest.clone()
    }
}

/// Writes a fresh manifest holding `m` and renames it into place. Returns the
/// file open for appending and the number of tables written.
fn help_rewrite(dir: &Path, m: &Manifest, ext_magic: u16) -> Result<(File, usize)> {
    let rewrite_path = dir.join(MANIFEST_REWRITE_FILENAME);
    let mut buf = Vec::with_capacity(64);
    buf.extend_from_slice(MAGIC_TEXT);
    buf.extend_from_slice(&ext_magic.to_be_bytes());
    buf.extend_from_slice(&BADGER_MAGIC_VERSION.to_be_bytes());
    let changes = pb::ManifestChangeSet {
        changes: m.as_changes(),
    }
    .encode_to_vec();
    buf.extend_from_slice(&(changes.len() as u32).to_be_bytes());
    buf.extend_from_slice(&crc32c::crc32c(&changes).to_be_bytes());
    buf.extend_from_slice(&changes);

    let mut f = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&rewrite_path)?;
    f.write_all(&buf)?;
    f.sync_all()?;
    drop(f);
    let manifest_path = dir.join(MANIFEST_FILENAME);
    fs::rename(&rewrite_path, &manifest_path)?;
    let mut f = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&manifest_path)?;
    f.seek(SeekFrom::End(0))?;
    sync_dir(dir)?;
    Ok((f, m.tables.len()))
}

/// Reads a manifest file (Go's `ReplayManifestFile`). Returns the manifest
/// and the offset after the last complete change set.
pub fn replay_manifest_file(file: &mut File, ext_magic: u16) -> Result<(Manifest, u64)> {
    let mut data = Vec::new();
    file.seek(SeekFrom::Start(0))?;
    file.read_to_end(&mut data)?;
    replay_manifest(&data, ext_magic)
}

fn replay_manifest(data: &[u8], ext_magic: u16) -> Result<(Manifest, u64)> {
    let bad_magic = || Error::corruption("manifest has bad magic");
    if data.len() < 8 || &data[0..4] != MAGIC_TEXT {
        return Err(bad_magic());
    }
    let ext_version = u16::from_be_bytes([data[4], data[5]]);
    let version = u16::from_be_bytes([data[6], data[7]]);
    if version != BADGER_MAGIC_VERSION {
        return Err(Error::corruption(format!(
            "manifest has unsupported version: {version} (we support {BADGER_MAGIC_VERSION})"
        )));
    }
    if ext_version != ext_magic {
        return Err(Error::InvalidRequest).context(format!(
            "cannot open DB because the external magic number doesn't match, \
             expected: {ext_magic}, version present in manifest: {ext_version}"
        ));
    }

    let mut build = Manifest::default();
    let mut offset = 8usize;
    // A truncated length/CRC header ends the replay (a crash mid-append).
    while let Some(len_crc) = data.get(offset..offset + 8) {
        let len = u32::from_be_bytes(len_crc[0..4].try_into().unwrap());
        let crc = u32::from_be_bytes(len_crc[4..8].try_into().unwrap());
        if len as usize > data.len() {
            return Err(Error::corruption(format!(
                "Buffer length: {len} greater than file size: {}. Manifest file might be corrupted",
                data.len()
            )));
        }
        let Some(buf) = data.get(offset + 8..offset + 8 + len as usize) else {
            break; // truncated
        };
        if crc32c::crc32c(buf) != crc {
            return Err(Error::corruption("manifest has checksum mismatch"));
        }
        let cs = pb::ManifestChangeSet::decode(buf)?;
        build.apply_change_set(&cs)?;
        offset += 8 + len as usize;
    }
    Ok((build, offset as u64))
}

// Miri cannot fsync directories.
#[cfg(all(test, not(miri)))]
mod tests {
    // Ports of Go's manifest_test.go that do not need a writable DB.
    use super::*;

    fn open(dir: &Path, threshold: usize) -> (ManifestFile, Manifest) {
        ManifestFile::help_open_or_create(dir, false, 0, threshold).unwrap()
    }

    #[test]
    fn create_replay_and_truncate() {
        let dir = tempfile::tempdir().unwrap();
        let (mf, m) = open(dir.path(), 10_000);
        assert_eq!(m, Manifest::default());
        mf.add_changes(vec![
            new_create_change(1, 0, 0, CompressionType::None),
            new_create_change(2, 6, 3, CompressionType::Zstd),
        ])
        .unwrap();
        mf.add_changes(vec![new_delete_change(1)]).unwrap();
        drop(mf);

        // A torn append is cut off on open.
        let path = dir.path().join(MANIFEST_FILENAME);
        let good_len = fs::metadata(&path).unwrap().len();
        let mut f = OpenOptions::new().append(true).open(&path).unwrap();
        f.write_all(&[0, 0, 0, 9, 1, 2]).unwrap();
        drop(f);

        let (mf, m) = open(dir.path(), 10_000);
        assert_eq!(fs::metadata(&path).unwrap().len(), good_len);
        assert_eq!(
            m.tables,
            BTreeMap::from([(
                2,
                TableManifest {
                    level: 6,
                    key_id: 3,
                    compression: CompressionType::Zstd
                }
            )])
        );
        assert_eq!((m.creations, m.deletions), (2, 1));
        assert_eq!(m.levels.len(), 7);
        assert_eq!(
            mf.manifest().creations,
            1,
            "the in-memory copy is compacted"
        );
    }

    fn corrupt_at(offset: usize, want: &str) {
        let dir = tempfile::tempdir().unwrap();
        drop(open(dir.path(), 10_000));
        let path = dir.path().join(MANIFEST_FILENAME);
        let mut bytes = fs::read(&path).unwrap();
        bytes[offset] ^= 1;
        fs::write(&path, bytes).unwrap();
        let err = ManifestFile::help_open_or_create(dir.path(), false, 0, 10_000)
            .err()
            .unwrap();
        assert!(err.to_string().contains(want), "{err}");
    }

    #[test]
    fn manifest_magic() {
        corrupt_at(3, "bad magic");
    }

    #[test]
    fn manifest_version() {
        corrupt_at(6, "unsupported version");
    }

    #[test]
    fn manifest_checksum() {
        let dir = tempfile::tempdir().unwrap();
        let (mf, _) = open(dir.path(), 10_000);
        mf.add_changes(vec![new_create_change(1, 0, 0, CompressionType::None)])
            .unwrap();
        drop(mf);
        let path = dir.path().join(MANIFEST_FILENAME);
        let mut bytes = fs::read(&path).unwrap();
        let last = bytes.len() - 1;
        bytes[last] ^= 1;
        fs::write(&path, bytes).unwrap();
        let err = ManifestFile::help_open_or_create(dir.path(), false, 0, 10_000)
            .err()
            .unwrap();
        assert!(err.to_string().contains("checksum mismatch"), "{err}");
    }

    #[test]
    fn external_magic_mismatch() {
        let dir = tempfile::tempdir().unwrap();
        drop(open(dir.path(), 10_000));
        let err = ManifestFile::help_open_or_create(dir.path(), true, 7, 10_000)
            .err()
            .unwrap();
        assert!(err.to_string().contains("external magic"), "{err}");
    }

    #[test]
    fn manifest_rewrite() {
        let dir = tempfile::tempdir().unwrap();
        let threshold = 10;
        let (mf, m) = open(dir.path(), threshold);
        assert_eq!((m.creations, m.deletions), (0, 0));
        mf.add_changes(vec![new_create_change(0, 0, 0, CompressionType::None)])
            .unwrap();
        for i in 0..(threshold as u64 * 3) {
            mf.add_changes(vec![
                new_create_change(i + 1, 0, 0, CompressionType::None),
                new_delete_change(i),
            ])
            .unwrap();
        }
        drop(mf);
        let (_, m) = open(dir.path(), threshold);
        assert_eq!(
            m.tables,
            BTreeMap::from([(
                threshold as u64 * 3,
                TableManifest {
                    level: 0,
                    key_id: 0,
                    compression: CompressionType::None
                }
            )])
        );
        assert!(m.deletions < threshold, "the file was rewritten");
    }

    #[test]
    fn concurrent_manifest_compaction() {
        let dir = tempfile::tempdir().unwrap();
        let (mf, _) = open(dir.path(), 0);
        let changes: Vec<_> = (0..1000)
            .flat_map(|i| {
                [
                    new_create_change(i, 0, 0, CompressionType::None),
                    new_delete_change(i),
                ]
            })
            .collect();
        std::thread::scope(|s| {
            for _ in 0..2 {
                s.spawn(|| mf.add_changes(changes.clone()).unwrap());
            }
        });
        drop(mf);
        let (_, m) = open(dir.path(), 0);
        assert!(m.tables.is_empty());
    }

    #[test]
    fn read_only_needs_a_manifest() {
        let dir = tempfile::tempdir().unwrap();
        assert!(ManifestFile::help_open_or_create(dir.path(), true, 0, 10).is_err());
        assert!(!dir.path().join(MANIFEST_FILENAME).exists());
    }
}
