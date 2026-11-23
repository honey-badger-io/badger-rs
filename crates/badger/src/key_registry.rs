//! The key registry: data keys used to encrypt tables, vlog files and WALs,
//! stored encrypted with the user's master key. Port of Go's `key_registry.go`.
//!
//! File layout (`KEYREGISTRY`):
//!
//! ```text
//! +---------+------------------------+-----------+-----------+-----
//! | IV (16) | sanity text (12, enc.) | DataKey 1 | DataKey 2 | ...
//! +---------+------------------------+-----------+-----------+-----
//! DataKey = len (u32 BE) | crc32c of proto (u32 BE) | pb::DataKey proto
//! ```
//!
//! The sanity text, `"Hello Badger"`, is encrypted with the master key and the
//! IV, so a wrong master key is detected on open. Each data key's `data` is
//! encrypted with the master key and the key's own IV.
//!
//! Differences from Go: the registry is opened read-write without `O_DSYNC`;
//! new keys are flushed with `sync_data` instead. Data keys are shared as
//! `Arc<pb::DataKey>`.

use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{ErrorKind, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use prost::Message;

use crate::error::{Error, Result, ResultExt};
use crate::pb;
use crate::util::encrypt::{BLOCK_SIZE, generate_iv, xor_block_alloc};
use crate::util::sync_dir;
use crc32c::crc32c;

pub const KEY_REGISTRY_FILE_NAME: &str = "KEYREGISTRY";
pub const KEY_REGISTRY_REWRITE_FILE_NAME: &str = "REWRITE-KEYREGISTRY";

/// Encrypted with the master key to check that the key is right.
const SANITY_TEXT: &[u8] = b"Hello Badger";

#[derive(Debug, Clone, Default)]
pub struct KeyRegistryOptions {
    pub dir: PathBuf,
    pub read_only: bool,
    /// Master key: 16, 24 or 32 bytes, or empty for no encryption.
    pub encryption_key: Vec<u8>,
    pub encryption_key_rotation_duration: Duration,
    pub in_memory: bool,
}

impl KeyRegistryOptions {
    pub fn from_options(opt: &crate::Options) -> Self {
        KeyRegistryOptions {
            dir: opt.dir.clone(),
            read_only: opt.read_only,
            encryption_key: opt.encryption_key.clone(),
            encryption_key_rotation_duration: opt.encryption_key_rotation_duration,
            in_memory: opt.in_memory,
        }
    }
}

/// See the module docs.
#[derive(Debug)]
pub struct KeyRegistry {
    inner: RwLock<Inner>,
    opt: KeyRegistryOptions,
}

#[derive(Debug, Default)]
struct Inner {
    data_keys: HashMap<u64, Arc<pb::DataKey>>,
    /// Unix seconds when the newest data key was created.
    last_created: i64,
    /// The newest key id; the next key gets this plus one.
    next_key_id: u64,
    /// Open for appending new keys (not in read-only or in-memory mode).
    file: Option<File>,
}

impl KeyRegistry {
    fn empty(opt: KeyRegistryOptions) -> Self {
        KeyRegistry {
            inner: RwLock::new(Inner::default()),
            opt,
        }
    }

    /// Opens the registry in `opt.dir`, creating it if missing (unless read-only).
    pub fn open(opt: KeyRegistryOptions) -> Result<Self> {
        if !matches!(opt.encryption_key.len(), 0 | 16 | 24 | 32) {
            return Err(Error::InvalidEncryptionKey).context("During OpenKeyRegistry");
        }
        if opt.in_memory {
            return Ok(Self::empty(opt));
        }
        let path = opt.dir.join(KEY_REGISTRY_FILE_NAME);
        let open = |path: &Path| {
            OpenOptions::new()
                .read(true)
                .append(!opt.read_only)
                .open(path)
        };
        let mut file = match open(&path) {
            Ok(f) => f,
            Err(e) if e.kind() == ErrorKind::NotFound => {
                if opt.read_only {
                    return Ok(Self::empty(opt));
                }
                write_key_registry(&HashMap::new(), &opt)
                    .context("Error while writing key registry.")?;
                open(&path).context("Error while opening newly created key registry.")?
            }
            Err(e) => return Err(e).context("Error while opening key registry."),
        };
        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;
        let mut inner = read_key_registry(&buf, &opt.encryption_key)?;
        if !opt.read_only {
            inner.file = Some(file);
        }
        Ok(KeyRegistry {
            inner: RwLock::new(inner),
            opt,
        })
    }

    /// The data key with `id`. Id 0 means "not encrypted" and gives `None`.
    pub fn data_key(&self, id: u64) -> Result<Option<Arc<pb::DataKey>>> {
        if id == 0 {
            return Ok(None);
        }
        let inner = self.inner.read().unwrap();
        match inner.data_keys.get(&id) {
            Some(dk) => Ok(Some(Arc::clone(dk))),
            None => {
                Err(Error::InvalidDataKeyId).with_context(|| format!("Error for the KEY ID {id}"))
            }
        }
    }

    /// The data key to encrypt new files with, or `None` without encryption.
    /// Creates (and persists) a new key when the newest one is older than the
    /// rotation duration.
    pub fn latest_data_key(&self) -> Result<Option<Arc<pb::DataKey>>> {
        if self.opt.encryption_key.is_empty() {
            return Ok(None);
        }
        // The newest key is valid while younger than the rotation period. A
        // key "from the future" (the clock moved back) is valid, as in Go.
        let now = SystemTime::now();
        let valid = |inner: &Inner| -> Option<Arc<pb::DataKey>> {
            let created = UNIX_EPOCH + Duration::from_secs(inner.last_created.max(0) as u64);
            let fresh = now
                .duration_since(created)
                .is_ok_and(|age| age < self.opt.encryption_key_rotation_duration)
                || now < created;
            fresh
                .then(|| inner.data_keys.get(&inner.next_key_id).cloned())
                .flatten()
        };
        if let Some(k) = valid(&self.inner.read().unwrap()) {
            return Ok(Some(k));
        }
        let mut inner = self.inner.write().unwrap();
        // Another thread may have rotated the key meanwhile.
        if let Some(k) = valid(&inner) {
            return Ok(Some(k));
        }

        let mut data = vec![0u8; self.opt.encryption_key.len()];
        getrandom::fill(&mut data).map_err(|e| Error::Io(std::io::Error::other(e)))?;
        let dk = pb::DataKey {
            key_id: inner.next_key_id + 1,
            data,
            iv: generate_iv()?.to_vec(),
            created_at: unix_now(),
        };
        if !self.opt.in_memory {
            let mut buf = Vec::new();
            store_data_key(&mut buf, &self.opt.encryption_key, &dk)?;
            let file = inner
                .file
                .as_mut()
                .ok_or(Error::InvalidRequest)
                .context("key registry is read-only")?;
            file.write_all(&buf)?;
            file.sync_data()?;
        }
        inner.next_key_id = dk.key_id;
        inner.last_created = dk.created_at;
        let dk = Arc::new(dk);
        inner.data_keys.insert(dk.key_id, Arc::clone(&dk));
        Ok(Some(dk))
    }

    /// All data keys, by id.
    pub fn data_keys(&self) -> HashMap<u64, Arc<pb::DataKey>> {
        self.inner.read().unwrap().data_keys.clone()
    }

    /// Rewrites this registry's file with its current keys (Go's `WriteKeyRegistry`
    /// called on an open registry).
    pub fn rewrite(&self, opt: &KeyRegistryOptions) -> Result<()> {
        write_key_registry(&self.data_keys(), opt)
    }
}

/// Re-encrypts the key registry in `dir` under a new master key (Go's
/// `badger rotate`): the data keys stay the same, so tables and value log
/// files need no rewriting. An empty `old_key` means the registry is not
/// encrypted yet, an empty `new_key` stores the data keys unencrypted.
/// Fails with [`Error::EncryptionKeyMismatch`] if `old_key` is wrong.
///
/// Unlike Go, it takes the directory lock: the DB must not be open, and it
/// fails if `dir` has no key registry.
pub fn rotate_master_key(dir: &Path, old_key: &[u8], new_key: &[u8]) -> Result<()> {
    // Opening would create an empty registry: a mistyped directory must not
    // look like a successful rotation.
    if !dir.join(KEY_REGISTRY_FILE_NAME).is_file() {
        return Err(Error::InvalidRequest)
            .with_context(|| format!("no {KEY_REGISTRY_FILE_NAME} in {}", dir.display()));
    }
    let _lock = crate::dir_lock::DirLockGuard::acquire(dir, false)?;
    let mut opt = KeyRegistryOptions {
        dir: dir.to_path_buf(),
        read_only: true,
        encryption_key: old_key.to_vec(),
        // Go's value; nothing is created here, so it does not matter.
        encryption_key_rotation_duration: Duration::from_secs(10 * 24 * 3600),
        in_memory: false,
    };
    let registry = KeyRegistry::open(opt.clone())?;
    if !matches!(new_key.len(), 0 | 16 | 24 | 32) {
        return Err(Error::InvalidEncryptionKey).context("rotating the master key");
    }
    opt.encryption_key = new_key.to_vec();
    registry.rewrite(&opt)
}

fn unix_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// Parses a whole registry file.
fn read_key_registry(buf: &[u8], encryption_key: &[u8]) -> Result<Inner> {
    // Header: IV and the (encrypted) sanity text.
    let header = BLOCK_SIZE + SANITY_TEXT.len();
    if buf.len() < header {
        return Err(Error::corruption("key registry too short"))
            .context("Error while reading sanity text.");
    }
    let (iv, sanity) = (&buf[..BLOCK_SIZE], &buf[BLOCK_SIZE..header]);
    let sanity = if encryption_key.is_empty() {
        sanity.to_vec()
    } else {
        xor_block_alloc(sanity, encryption_key, iv).context("During validRegistry")?
    };
    if sanity != SANITY_TEXT {
        return Err(Error::EncryptionKeyMismatch);
    }

    let mut inner = Inner::default();
    let mut rest = &buf[header..];
    while !rest.is_empty() {
        let short = || Error::corruption("truncated data key in key registry");
        if rest.len() < 8 {
            return Err(short());
        }
        let len = u32::from_be_bytes(rest[0..4].try_into().unwrap()) as usize;
        let crc = u32::from_be_bytes(rest[4..8].try_into().unwrap());
        let Some(data) = rest.get(8..8 + len) else {
            return Err(short());
        };
        rest = &rest[8 + len..];
        let actual = crc32c(data);
        if actual != crc {
            return Err(Error::ChecksumMismatch {
                actual: actual as u64,
                expected: crc as u64,
            })
            .context("Error while checking checksum for data key.");
        }
        let mut dk = pb::DataKey::decode(data)
            .context("While unmarshal of datakey in keyRegistryIterator.next")?;
        if !encryption_key.is_empty() {
            dk.data = xor_block_alloc(&dk.data, encryption_key, &dk.iv)
                .context("While decrypting datakey in keyRegistryIterator.next")?;
        }
        inner.next_key_id = inner.next_key_id.max(dk.key_id);
        inner.last_created = inner.last_created.max(dk.created_at);
        inner.data_keys.insert(dk.key_id, Arc::new(dk));
    }
    Ok(inner)
}

/// Appends `dk` to `buf` in file format, encrypting its data with `storage_key`.
fn store_data_key(buf: &mut Vec<u8>, storage_key: &[u8], dk: &pb::DataKey) -> Result<()> {
    let mut stored = dk.clone();
    if !storage_key.is_empty() {
        stored.data = xor_block_alloc(&dk.data, storage_key, &dk.iv)
            .context("Error while encrypting datakey in storeDataKey")?;
    }
    let data = stored.encode_to_vec();
    buf.extend_from_slice(&(data.len() as u32).to_be_bytes());
    buf.extend_from_slice(&crc32c(&data).to_be_bytes());
    buf.extend_from_slice(&data);
    Ok(())
}

/// Writes a fresh registry file holding `keys`, atomically via a rename
/// (Go's `WriteKeyRegistry`).
pub fn write_key_registry(
    keys: &HashMap<u64, Arc<pb::DataKey>>,
    opt: &KeyRegistryOptions,
) -> Result<()> {
    let iv = generate_iv()?;
    let sanity = if opt.encryption_key.is_empty() {
        SANITY_TEXT.to_vec()
    } else {
        xor_block_alloc(SANITY_TEXT, &opt.encryption_key, &iv)
            .context("Error while encrypting sanity text in WriteKeyRegistry")?
    };
    let mut buf = Vec::with_capacity(64);
    buf.extend_from_slice(&iv);
    buf.extend_from_slice(&sanity);
    // Go iterates a map, so its order is random; sort for reproducibility.
    let mut ids: Vec<_> = keys.keys().copied().collect();
    ids.sort_unstable();
    for id in ids {
        store_data_key(&mut buf, &opt.encryption_key, &keys[&id])
            .context("Error while storing datakey in WriteKeyRegistry")?;
    }

    let tmp = opt.dir.join(KEY_REGISTRY_REWRITE_FILE_NAME);
    let mut f = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&tmp)
        .context("Error while opening tmp file in WriteKeyRegistry")?;
    f.write_all(&buf)
        .context("Error while writing buf in WriteKeyRegistry")?;
    f.sync_all()?;
    drop(f);
    fs::rename(&tmp, opt.dir.join(KEY_REGISTRY_FILE_NAME))
        .context("Error while renaming file in WriteKeyRegistry")?;
    sync_dir(&opt.dir)
}

#[cfg(test)]
mod tests {
    // Port of Go's key_registry_test.go.
    use super::*;

    fn opts(dir: &Path, key: &[u8]) -> KeyRegistryOptions {
        KeyRegistryOptions {
            dir: dir.to_path_buf(),
            encryption_key: key.to_vec(),
            encryption_key_rotation_duration: Duration::from_secs(10 * 24 * 3600),
            ..Default::default()
        }
    }

    #[test]
    #[cfg_attr(miri, ignore)] // AES intrinsics
    fn build_registry() {
        let dir = tempfile::tempdir().unwrap();
        let key = [7u8; 32];
        let kr = KeyRegistry::open(opts(dir.path(), &key)).unwrap();
        let dk = kr.latest_data_key().unwrap().unwrap();
        // The same key comes back until rotation.
        let dk1 = kr.latest_data_key().unwrap().unwrap();
        assert_eq!(dk, dk1);
        assert_eq!(dk.key_id, 1);
        drop(kr);

        let kr = KeyRegistry::open(opts(dir.path(), &key)).unwrap();
        assert_eq!(kr.data_key(1).unwrap().unwrap(), dk);
        assert!(kr.data_key(0).unwrap().is_none());
        assert!(matches!(
            kr.data_key(2).unwrap_err().root(),
            Error::InvalidDataKeyId
        ));
    }

    #[test]
    #[cfg_attr(miri, ignore)] // AES intrinsics
    fn rewrite_and_rotation() {
        let dir = tempfile::tempdir().unwrap();
        let key = [1u8; 16];
        let mut o = opts(dir.path(), &key);
        o.encryption_key_rotation_duration = Duration::ZERO; // every call rotates
        let kr = KeyRegistry::open(o.clone()).unwrap();
        let a = kr.latest_data_key().unwrap().unwrap();
        let b = kr.latest_data_key().unwrap().unwrap();
        assert_eq!((a.key_id, b.key_id), (1, 2));
        kr.rewrite(&o).unwrap();
        drop(kr);

        let kr = KeyRegistry::open(o.clone()).unwrap();
        assert_eq!(kr.data_keys().len(), 2);
        assert_eq!(kr.data_key(2).unwrap().unwrap(), b);
        let c = kr.latest_data_key().unwrap().unwrap();
        assert_eq!(c.key_id, 3);
    }

    #[test]
    #[cfg_attr(miri, ignore)] // AES intrinsics
    fn mismatch_and_read_only() {
        let dir = tempfile::tempdir().unwrap();
        KeyRegistry::open(opts(dir.path(), &[1u8; 16])).unwrap();
        let err = KeyRegistry::open(opts(dir.path(), &[2u8; 16])).unwrap_err();
        assert!(matches!(err.root(), Error::EncryptionKeyMismatch));
        assert!(matches!(
            KeyRegistry::open(opts(dir.path(), &[2u8; 15]))
                .unwrap_err()
                .root(),
            Error::InvalidEncryptionKey
        ));

        let empty = tempfile::tempdir().unwrap();
        let mut o = opts(empty.path(), &[]);
        o.read_only = true;
        let kr = KeyRegistry::open(o).unwrap();
        assert!(kr.latest_data_key().unwrap().is_none());
        assert!(!empty.path().join(KEY_REGISTRY_FILE_NAME).exists());
    }

    #[test]
    #[cfg_attr(miri, ignore)] // Miri cannot fsync directories
    fn plain_registry_has_no_keys() {
        let dir = tempfile::tempdir().unwrap();
        let kr = KeyRegistry::open(opts(dir.path(), &[])).unwrap();
        assert!(kr.latest_data_key().unwrap().is_none());
        let bytes = fs::read(dir.path().join(KEY_REGISTRY_FILE_NAME)).unwrap();
        assert_eq!(bytes.len(), 28);
        assert_eq!(&bytes[16..], SANITY_TEXT);
    }
}
