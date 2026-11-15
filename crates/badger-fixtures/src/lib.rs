//! Test support for the badger port: loads the fixtures that `compat/go-fixtures`
//! generates with Go Badger, and writes expected-contents files that Go can verify.
//!
//! The JSON layout mirrors `compat/go-fixtures/model.go`; see `compat/README.md`.
//!
//! ```no_run
//! let fx = badger_fixtures::Fixtures::load().unwrap();
//! for db in fx.dbs() {
//!     let expected = fx.db_expected(db).unwrap();
//!     println!("{}: {} entries in {}", db.name, expected.entries.len(), fx.path(db).display());
//! }
//! ```

use std::fs;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

/// The fixture JSON layout version this crate understands.
pub const FORMAT_VERSION: u32 = 1;

/// Environment variable that overrides the fixtures directory.
pub const FIXTURES_ENV: &str = "BADGER_FIXTURES";

/// Boxed error type, so callers can use `?` freely in tests.
pub type Error = Box<dyn std::error::Error + Send + Sync>;
pub type Result<T> = std::result::Result<T, Error>;

/// `fixtures/index.json`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Index {
    pub format_version: u32,
    pub badger_version: String,
    pub fixtures: Vec<IndexEntry>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Kind {
    /// A whole database directory.
    Db,
    /// A standalone SSTable built with Go's `table.Builder`.
    Sst,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexEntry {
    pub name: String,
    pub kind: Kind,
    pub description: String,
    /// Relative to the index: the DB directory, or the `.sst` file.
    pub path: String,
    /// Relative to the index: the expected-contents JSON.
    pub expected: String,
}

/// Options needed to open (or that were used to write) a fixture DB.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DbOptions {
    /// `none`, `snappy` or `zstd`.
    pub compression: String,
    /// AES master key; empty when unencrypted.
    #[serde(with = "hex_bytes")]
    pub encryption_key: Vec<u8>,
    pub value_threshold: i64,
    pub mem_table_size: i64,
    pub value_log_file_size: i64,
    pub base_table_size: i64,
    pub num_versions_to_keep: i32,
    pub managed: bool,
}

/// Expected contents of a DB: every version of every key, as returned by an
/// iterator with `AllVersions` at the newest read timestamp. Ordered by key
/// ascending, then version descending.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DbExpected {
    pub format_version: u32,
    pub name: String,
    pub options: DbOptions,
    /// The directory was copied from a live DB (unflushed WAL, untruncated
    /// vlog). Go cannot open it read-only; `entries` come from a recovered copy.
    pub crash_snapshot: bool,
    pub max_version: u64,
    pub entries: Vec<DbEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DbEntry {
    #[serde(with = "hex_bytes")]
    pub key: Vec<u8>,
    pub version: u64,
    #[serde(with = "hex_bytes")]
    pub value: Vec<u8>,
    pub user_meta: u8,
    pub expires_at: u64,
    pub deleted_or_expired: bool,
    pub discard_earlier_versions: bool,
}

/// A standalone table and the entries it was built from.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SstExpected {
    pub format_version: u32,
    pub name: String,
    pub compression: String,
    pub block_size: usize,
    pub bloom_false_positive: f64,
    pub table_size: u64,
    /// The data key the table is encrypted with, if any.
    pub data_key: Option<DataKey>,
    /// Smallest internal key (user key followed by the 8-byte encoded version).
    #[serde(with = "hex_bytes")]
    pub smallest: Vec<u8>,
    /// Biggest internal key.
    #[serde(with = "hex_bytes")]
    pub biggest: Vec<u8>,
    pub key_count: u32,
    pub max_version: u64,
    /// In table order: user key ascending, version descending.
    pub entries: Vec<SstEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataKey {
    pub key_id: u64,
    #[serde(with = "hex_bytes")]
    pub data: Vec<u8>,
    #[serde(with = "hex_bytes")]
    pub iv: Vec<u8>,
    pub created_at: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SstEntry {
    /// User key, without the version suffix.
    #[serde(with = "hex_bytes")]
    pub key: Vec<u8>,
    pub version: u64,
    pub meta: u8,
    pub user_meta: u8,
    pub expires_at: u64,
    #[serde(with = "hex_bytes")]
    pub value: Vec<u8>,
    /// `valueLen` passed to Go's `Builder.Add` (vlog length for value pointers).
    pub value_len: u32,
}

/// A loaded `fixtures/index.json` plus the directory it lives in.
#[derive(Debug, Clone)]
pub struct Fixtures {
    root: PathBuf,
    pub index: Index,
}

impl Fixtures {
    /// The fixtures directory: `$BADGER_FIXTURES` if set, else `compat/fixtures`
    /// in this repository.
    pub fn default_root() -> PathBuf {
        match std::env::var_os(FIXTURES_ENV) {
            Some(dir) => PathBuf::from(dir),
            None => Path::new(env!("CARGO_MANIFEST_DIR")).join("../../compat/fixtures"),
        }
    }

    /// Loads the index from [`Fixtures::default_root`].
    pub fn load() -> Result<Self> {
        Self::load_from(Self::default_root())
    }

    pub fn load_from(root: impl Into<PathBuf>) -> Result<Self> {
        let root = root.into();
        let index: Index = read_json(&root.join("index.json"))?;
        if index.format_version != FORMAT_VERSION {
            return Err(format!(
                "fixtures format version {} is not supported (expected {FORMAT_VERSION})",
                index.format_version
            )
            .into());
        }
        Ok(Self { root, index })
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn dbs(&self) -> impl Iterator<Item = &IndexEntry> {
        self.index.fixtures.iter().filter(|f| f.kind == Kind::Db)
    }

    pub fn ssts(&self) -> impl Iterator<Item = &IndexEntry> {
        self.index.fixtures.iter().filter(|f| f.kind == Kind::Sst)
    }

    /// Looks up a fixture by kind and name.
    pub fn get(&self, kind: Kind, name: &str) -> Result<&IndexEntry> {
        self.index
            .fixtures
            .iter()
            .find(|f| f.kind == kind && f.name == name)
            .ok_or_else(|| format!("no {kind:?} fixture named {name:?}").into())
    }

    /// Absolute path of the fixture's DB directory or `.sst` file.
    pub fn path(&self, entry: &IndexEntry) -> PathBuf {
        self.root.join(&entry.path)
    }

    pub fn db_expected(&self, entry: &IndexEntry) -> Result<DbExpected> {
        self.expected(entry, Kind::Db)
    }

    pub fn sst_expected(&self, entry: &IndexEntry) -> Result<SstExpected> {
        self.expected(entry, Kind::Sst)
    }

    fn expected<T: for<'de> Deserialize<'de>>(&self, entry: &IndexEntry, kind: Kind) -> Result<T> {
        if entry.kind != kind {
            return Err(format!("fixture {} is {:?}, not {kind:?}", entry.name, entry.kind).into());
        }
        read_json(&self.root.join(&entry.expected))
    }
}

impl DbExpected {
    /// Writes this file so `go-fixtures verify <db-dir> <path>` can check a DB
    /// written by Rust.
    pub fn write(&self, path: &Path) -> Result<()> {
        fs::write(path, serde_json::to_vec_pretty(self)?)?;
        Ok(())
    }
}

/// Encodes a user key and version the way Go Badger's `y.KeyWithTs` does: the
/// key followed by `u64::MAX - version` in big-endian, so newer versions sort first.
pub fn key_with_ts(key: &[u8], version: u64) -> Vec<u8> {
    let mut out = Vec::with_capacity(key.len() + 8);
    out.extend_from_slice(key);
    out.extend_from_slice(&(u64::MAX - version).to_be_bytes());
    out
}

fn read_json<T: for<'de> Deserialize<'de>>(path: &Path) -> Result<T> {
    let bytes = fs::read(path).map_err(|e| format!("reading {}: {e}", path.display()))?;
    serde_json::from_slice(&bytes).map_err(|e| format!("parsing {}: {e}", path.display()).into())
}

/// Serde adapter: `Vec<u8>` as a lowercase hex string.
mod hex_bytes {
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(bytes: &[u8], s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&hex::encode(bytes))
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<u8>, D::Error> {
        let s = <&str>::deserialize(d)?;
        hex::decode(s).map_err(serde::de::Error::custom)
    }
}
