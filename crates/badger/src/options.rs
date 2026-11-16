//! Database options. Port of Go Badger's `options.go` and `options/options.go`.
//!
//! [`Options::new`] gives Go's `DefaultOptions`. Every field is public, and has
//! a `with_*` method for chaining:
//!
//! ```
//! use badger::options::{CompressionType, Options};
//! let opts = Options::new("/tmp/db")
//!     .with_compression(CompressionType::Zstd)
//!     .with_num_versions_to_keep(3);
//! assert_eq!(opts.value_dir, opts.dir);
//! ```
//!
//! Differences from Go:
//! - No `Logger` option: the crate will log through the `log` facade, so the
//!   application chooses the logger and level.
//! - `namespace_offset` is an `Option` instead of `-1` meaning "none".
//! - Validation (`checkAndSetOptions` in Go's `db.go`) comes with `DB::open`.
//! - `FromSuperFlag` (string-based options for CLIs) is not ported yet.

use std::path::{Path, PathBuf};
use std::time::Duration;

use crate::error::{Error, Result};

/// Block compression for SSTables. The numeric values are stored on disk.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[repr(u32)]
pub enum CompressionType {
    #[default]
    None = 0,
    Snappy = 1,
    Zstd = 2,
}

impl CompressionType {
    /// Parses Go's compression flag syntax: `none`, `snappy`, `zstd` or
    /// `zstd:<level>`. Returns the type and level (3 for zstd by default, 0
    /// otherwise).
    pub fn parse(s: &str) -> Result<(CompressionType, i32)> {
        let invalid = |msg: String| Err(Error::InvalidOptions(msg));
        let mut parts = s.split(':');
        let kind = parts.next().unwrap_or_default();
        let level = match (parts.next(), parts.next()) {
            (None, _) => 3,
            (Some(l), None) => match l.parse::<i32>() {
                Ok(l) if l > 0 => l,
                _ => return invalid(format!("compression level ({l}) must be greater than zero")),
            },
            (Some(_), Some(_)) => return invalid(format!("invalid compression argument: {s}")),
        };
        match kind {
            "zstd" => Ok((CompressionType::Zstd, level)),
            "snappy" => Ok((CompressionType::Snappy, 0)),
            "none" => Ok((CompressionType::None, 0)),
            _ => invalid(format!("compression type ({kind}) invalid")),
        }
    }
}

impl TryFrom<u32> for CompressionType {
    type Error = Error;
    fn try_from(v: u32) -> Result<Self> {
        match v {
            0 => Ok(CompressionType::None),
            1 => Ok(CompressionType::Snappy),
            2 => Ok(CompressionType::Zstd),
            _ => Err(Error::corruption(format!("unknown compression type {v}"))),
        }
    }
}

/// When to verify SSTable checksums.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum ChecksumVerificationMode {
    #[default]
    NoVerification = 0,
    /// When a table is opened.
    OnTableRead = 1,
    /// On every block read.
    OnBlockRead = 2,
    OnTableAndBlockRead = 3,
}

/// Go's `maxValueThreshold`: values of at least 1 MiB always go to the value log.
pub const MAX_VALUE_THRESHOLD: u64 = 1 << 20;

/// Parameters for opening a database. See the module docs.
#[derive(Debug, Clone, PartialEq)]
pub struct Options {
    // Required.
    pub dir: PathBuf,
    pub value_dir: PathBuf,

    // Usually modified.
    pub sync_writes: bool,
    pub num_versions_to_keep: usize,
    pub read_only: bool,
    pub compression: CompressionType,
    pub in_memory: bool,
    pub metrics_enabled: bool,
    /// Number of threads a `Stream` uses.
    pub num_goroutines: usize,

    // Fine tuning.
    pub mem_table_size: u64,
    pub base_table_size: u64,
    pub base_level_size: u64,
    pub level_size_multiplier: usize,
    pub table_size_multiplier: usize,
    pub max_levels: usize,

    pub vlog_percentile: f64,
    pub value_threshold: u64,
    pub num_memtables: usize,
    /// Can change between runs: each table records its own block size.
    pub block_size: usize,
    pub bloom_false_positive: f64,
    pub block_cache_size: u64,
    pub index_cache_size: u64,

    pub num_level_zero_tables: usize,
    pub num_level_zero_tables_stall: usize,

    pub value_log_file_size: u64,
    pub value_log_max_entries: u32,

    pub num_compactors: usize,
    pub compact_l0_on_close: bool,
    pub lmax_compaction: bool,
    pub zstd_compression_level: i32,

    /// Validate the checksum of every entry read from the value log.
    pub verify_value_checksum: bool,

    // Encryption.
    /// AES key of 16, 24 or 32 bytes; empty disables encryption.
    pub encryption_key: Vec<u8>,
    pub encryption_key_rotation_duration: Duration,

    /// Skip the directory lock. Dangerous: concurrent instances corrupt data.
    pub bypass_lock_guard: bool,
    pub checksum_verification_mode: ChecksumVerificationMode,
    /// Check transactions for conflicts. Off is faster.
    pub detect_conflicts: bool,
    /// Offset in each key of an 8-byte namespace, if keys have one.
    pub namespace_offset: Option<usize>,
    /// Application-defined format version, checked when opening the DB.
    pub external_magic_version: u16,

    /// Transaction timestamps are managed by the application (`open_managed`).
    pub(crate) managed_txns: bool,
}

impl Options {
    /// Recommended defaults for a database in `path` (Go's `DefaultOptions`).
    pub fn new(path: impl AsRef<Path>) -> Self {
        let path = path.as_ref().to_path_buf();
        Options {
            dir: path.clone(),
            value_dir: path,

            mem_table_size: 64 << 20,
            base_table_size: 2 << 20,
            base_level_size: 10 << 20,
            table_size_multiplier: 2,
            level_size_multiplier: 10,
            max_levels: 7,
            num_goroutines: 8,
            metrics_enabled: true,

            num_compactors: 4, // at least 2; compactor 0 prioritizes L0
            num_level_zero_tables: 5,
            num_level_zero_tables_stall: 15,
            num_memtables: 5,
            bloom_false_positive: 0.01,
            block_size: 4 * 1024,
            sync_writes: false,
            num_versions_to_keep: 1,
            compact_l0_on_close: false,
            verify_value_checksum: false,
            compression: CompressionType::Snappy,
            block_cache_size: 256 << 20,
            index_cache_size: 0,
            zstd_compression_level: 1,

            // (2^30 - 1) * 2 stays below 2^31 - 1 when mmapped.
            value_log_file_size: (1 << 30) - 1,
            value_log_max_entries: 1_000_000,

            vlog_percentile: 0.0,
            value_threshold: MAX_VALUE_THRESHOLD,

            encryption_key: Vec::new(),
            encryption_key_rotation_duration: Duration::from_secs(10 * 24 * 60 * 60),
            detect_conflicts: true,
            namespace_offset: None,

            read_only: false,
            in_memory: false,
            lmax_compaction: false,
            bypass_lock_guard: false,
            checksum_verification_mode: ChecksumVerificationMode::NoVerification,
            external_magic_version: 0,
            managed_txns: false,
        }
    }

    /// Defaults tuned to keep values in the LSM tree (Go's `LSMOnlyOptions`).
    pub fn lsm_only(path: impl AsRef<Path>) -> Self {
        Self::new(path).with_value_threshold(MAX_VALUE_THRESHOLD)
    }

    /// Largest write batch in bytes: 15% of the memtable (Go's `maxBatchSize`).
    pub(crate) fn max_batch_size(&self) -> u64 {
        (15 * self.mem_table_size) / 100
    }

    /// Most entries in a write batch (Go's `maxBatchCount`).
    pub(crate) fn max_batch_count(&self) -> u64 {
        self.max_batch_size() / crate::skl::MAX_NODE_SIZE as u64
    }
}

macro_rules! with_methods {
    ($($(#[$doc:meta])* $method:ident => $field:ident: $ty:ty;)*) => {
        impl Options {
            $(
                $(#[$doc])*
                pub fn $method(mut self, val: $ty) -> Self {
                    self.$field = val;
                    self
                }
            )*
        }
    };
}

with_methods! {
    with_sync_writes => sync_writes: bool;
    with_num_versions_to_keep => num_versions_to_keep: usize;
    /// Opens the DB read-only. Not supported on Windows.
    with_read_only => read_only: bool;
    with_compression => compression: CompressionType;
    /// Keeps everything in memory. Requires empty `dir` and `value_dir`.
    with_in_memory => in_memory: bool;
    with_metrics_enabled => metrics_enabled: bool;
    with_num_goroutines => num_goroutines: usize;
    with_mem_table_size => mem_table_size: u64;
    with_base_table_size => base_table_size: u64;
    with_base_level_size => base_level_size: u64;
    with_level_size_multiplier => level_size_multiplier: usize;
    with_table_size_multiplier => table_size_multiplier: usize;
    with_max_levels => max_levels: usize;
    with_vlog_percentile => vlog_percentile: f64;
    /// Values of at least this many bytes go to the value log.
    with_value_threshold => value_threshold: u64;
    with_num_memtables => num_memtables: usize;
    with_block_size => block_size: usize;
    with_bloom_false_positive => bloom_false_positive: f64;
    with_block_cache_size => block_cache_size: u64;
    with_index_cache_size => index_cache_size: u64;
    with_num_level_zero_tables => num_level_zero_tables: usize;
    with_num_level_zero_tables_stall => num_level_zero_tables_stall: usize;
    with_value_log_file_size => value_log_file_size: u64;
    with_value_log_max_entries => value_log_max_entries: u32;
    with_num_compactors => num_compactors: usize;
    with_compact_l0_on_close => compact_l0_on_close: bool;
    with_zstd_compression_level => zstd_compression_level: i32;
    with_verify_value_checksum => verify_value_checksum: bool;
    with_encryption_key => encryption_key: Vec<u8>;
    with_encryption_key_rotation_duration => encryption_key_rotation_duration: Duration;
    with_bypass_lock_guard => bypass_lock_guard: bool;
    with_checksum_verification_mode => checksum_verification_mode: ChecksumVerificationMode;
    with_detect_conflicts => detect_conflicts: bool;
    with_namespace_offset => namespace_offset: Option<usize>;
    with_external_magic => external_magic_version: u16;
}

impl Options {
    /// Checks option values and combinations before opening a DB, and turns
    /// off settings that do not apply (Go's `checkAndSetOptions` in `db.go`).
    pub(crate) fn check_and_set(&mut self) -> Result<()> {
        let invalid = |msg: String| Err(Error::InvalidOptions(msg));
        // Zero compactors disables compaction; one would leave all data in L2.
        if self.num_compactors == 1 {
            return invalid("Cannot have 1 compactor. Need at least 2".into());
        }
        let has_dir = |p: &PathBuf| !p.as_os_str().is_empty();
        if self.in_memory && (has_dir(&self.dir) || has_dir(&self.value_dir)) {
            return invalid("Cannot use badger in Disk-less mode with Dir or ValueDir set".into());
        }
        if !(0.0..=1.0).contains(&self.vlog_percentile) {
            return invalid("vlogPercentile must be within range of 0.0-1.0".into());
        }
        if self.value_threshold > MAX_VALUE_THRESHOLD {
            return invalid(format!(
                "Invalid ValueThreshold, must be less or equal to {MAX_VALUE_THRESHOLD}"
            ));
        }
        // Transactions are batched by max_batch_size, so bigger values could
        // never be written.
        if self.value_threshold > self.max_batch_size() {
            return invalid(format!(
                "Valuethreshold {} greater than max batch size of {}. Either reduce \
                 opt.ValueThreshold or increase opt.BaseTableSize.",
                self.value_threshold,
                self.max_batch_size()
            ));
        }
        // Offsets in a value log file are u32, and files are mapped at twice
        // their size.
        if !(1 << 20..2 << 30).contains(&self.value_log_file_size) {
            return Err(Error::ValueLogSize);
        }
        if self.read_only {
            self.compact_l0_on_close = false;
        }
        let need_cache =
            self.compression != CompressionType::None || !self.encryption_key.is_empty();
        if need_cache && self.block_cache_size == 0 {
            // Go panics here.
            return invalid(
                "BlockCacheSize should be set since compression/encryption are enabled".into(),
            );
        }
        if self.num_level_zero_tables_stall <= self.num_level_zero_tables {
            return invalid("NumLevelZeroTablesStall must be above NumLevelZeroTables".into());
        }
        if self.in_memory {
            self.sync_writes = false;
            // Everything stays in the LSM tree.
            self.value_threshold = i32::MAX as u64;
        }
        Ok(())
    }

    pub fn with_dir(mut self, dir: impl AsRef<Path>) -> Self {
        self.dir = dir.as_ref().to_path_buf();
        self
    }

    pub fn with_value_dir(mut self, dir: impl AsRef<Path>) -> Self {
        self.value_dir = dir.as_ref().to_path_buf();
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn check_and_set() {
        assert!(Options::new("/d").check_and_set().is_ok());
        assert!(
            Options::new("/d")
                .with_num_compactors(1)
                .check_and_set()
                .is_err()
        );
        assert!(
            Options::new("/d")
                .with_in_memory(true)
                .check_and_set()
                .is_err()
        );
        let mut mem = Options::new("").with_in_memory(true);
        mem.check_and_set().unwrap();
        assert_eq!(mem.value_threshold, i32::MAX as u64);
        assert!(matches!(
            Options::new("/d")
                .with_value_log_file_size(1 << 19)
                .check_and_set(),
            Err(Error::ValueLogSize)
        ));
        let small = Options::new("/d").with_mem_table_size(1 << 20);
        assert!(
            small
                .clone()
                .with_value_threshold(1 << 18)
                .check_and_set()
                .is_err()
        );
        assert!(small.with_value_threshold(1 << 10).check_and_set().is_ok());
        assert!(
            Options::new("/d")
                .with_block_cache_size(0)
                .check_and_set()
                .is_err()
        );
    }

    #[test]
    fn defaults_match_go() {
        let o = Options::new("/data");
        assert_eq!(o.dir, PathBuf::from("/data"));
        assert_eq!(o.value_dir, o.dir);
        assert_eq!(o.mem_table_size, 64 << 20);
        assert_eq!(o.compression, CompressionType::Snappy);
        assert_eq!(o.value_threshold, MAX_VALUE_THRESHOLD);
        assert_eq!(o.value_log_file_size, (1 << 30) - 1);
        assert_eq!(
            o.encryption_key_rotation_duration,
            Duration::from_secs(864_000)
        );
        assert!(o.detect_conflicts && o.metrics_enabled && !o.read_only);
        assert_eq!(o.namespace_offset, None);
    }

    #[test]
    fn with_methods_chain() {
        let o = Options::new("a")
            .with_value_dir("b")
            .with_num_compactors(2)
            .with_read_only(true);
        assert_eq!(
            (o.value_dir.to_str(), o.num_compactors, o.read_only),
            (Some("b"), 2, true)
        );
    }

    #[test]
    fn parse_compression() {
        assert_eq!(
            CompressionType::parse("zstd").unwrap(),
            (CompressionType::Zstd, 3)
        );
        assert_eq!(
            CompressionType::parse("zstd:7").unwrap(),
            (CompressionType::Zstd, 7)
        );
        assert_eq!(
            CompressionType::parse("snappy").unwrap(),
            (CompressionType::Snappy, 0)
        );
        assert_eq!(
            CompressionType::parse("none").unwrap(),
            (CompressionType::None, 0)
        );
        for bad in ["zstd:0", "zstd:x", "zstd:1:2", "lz4"] {
            assert!(CompressionType::parse(bad).is_err(), "{bad}");
        }
    }
}
