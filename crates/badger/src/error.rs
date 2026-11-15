//! Errors returned by badger. Mirrors Go Badger's `errors.go` (the exported
//! `Err*` values) plus the errors from its `y` package.
//!
//! Go wraps errors with `y.Wrapf` to add context. Here that is
//! [`ResultExt::context`], which produces [`Error::Context`]. Use
//! [`Error::root`] to see past the context, the way Go uses `errors.Is`.

use std::fmt::Display;

/// `std::result::Result` with badger's [`Error`].
pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    // ----- errors.go -----
    #[error("Invalid ValueLogFileSize, must be in range [1MB, 2GB)")]
    ValueLogSize,
    #[error("Key not found")]
    KeyNotFound,
    #[error("Txn is too big to fit into one request")]
    TxnTooBig,
    #[error("Transaction Conflict. Please retry")]
    Conflict,
    #[error("No sets or deletes are allowed in a read-only transaction")]
    ReadOnlyTxn,
    #[error("This transaction has been discarded. Create a new one")]
    DiscardedTxn,
    #[error("Key cannot be empty")]
    EmptyKey,
    #[error("Key is using a reserved !badger! prefix")]
    InvalidKey,
    #[error("Key is using the banned prefix")]
    BannedKey,
    #[error("Value log GC can't run because threshold is set to zero")]
    ThresholdZero,
    #[error("Value log GC attempt didn't result in any cleanup")]
    NoRewrite,
    #[error("Value log GC request rejected")]
    Rejected,
    #[error("Invalid request")]
    InvalidRequest,
    #[error("Invalid API request. Not allowed to perform this action using ManagedDB")]
    ManagedTxn,
    #[error(
        "Invalid API request. Not allowed to perform this action when NamespaceMode is not set."
    )]
    NamespaceMode,
    #[error("Data dump cannot be read")]
    InvalidDump,
    #[error("Bandwidth must be greater than zero")]
    ZeroBandwidth,
    #[error("Read-only mode is not supported on Windows")]
    WindowsNotSupported,
    #[error("Log truncate required to run DB. This might result in data loss")]
    TruncateNeeded,
    #[error("Writes are blocked, possibly due to DropAll or Close")]
    BlockedWrites,
    #[error("Encryption key mismatch")]
    EncryptionKeyMismatch,
    #[error("Invalid datakey id")]
    InvalidDataKeyId,
    #[error("Encryption key's length should be either 16, 24, or 32 bytes")]
    InvalidEncryptionKey,
    #[error("Cannot run value log GC when DB is opened in InMemory mode")]
    GcInMemoryMode,
    #[error("Cannot run value log GC when DB is opened in ReadOnly mode")]
    GcInReadOnlyMode,
    #[error("DB Closed")]
    DbClosed,

    // ----- y package -----
    #[error("checksum mismatch: actual: {actual}, expected: {expected}")]
    ChecksumMismatch { actual: u64, expected: u64 },
    #[error("ErrEOF: End of file")]
    Eof,
    #[error("Batch commit not permitted after finish")]
    CommitAfterFinish,

    // ----- Rust-side additions -----
    /// Invalid or truncated on-disk data. Go usually panics in these places.
    #[error("corruption: {0}")]
    Corruption(String),
    /// An invalid option value or combination.
    #[error("invalid options: {0}")]
    InvalidOptions(String),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error("protobuf decode: {0}")]
    ProtoDecode(#[from] prost::DecodeError),
    #[error("flatbuffer: {0}")]
    Flatbuffer(#[from] flatbuffers::InvalidFlatbuffer),
    /// Another error with added context (Go's `y.Wrapf`).
    #[error("{context}: {source}")]
    Context {
        context: String,
        #[source]
        source: Box<Error>,
    },
}

impl Error {
    /// The innermost error, skipping any [`Error::Context`] layers.
    pub fn root(&self) -> &Error {
        let mut e = self;
        while let Error::Context { source, .. } = e {
            e = source;
        }
        e
    }

    pub(crate) fn corruption(msg: impl Display) -> Self {
        Error::Corruption(msg.to_string())
    }
}

/// Adds context to errors, like Go Badger's `y.Wrap` / `y.Wrapf`.
pub trait ResultExt<T> {
    fn context(self, context: impl Display) -> Result<T>;
    fn with_context<C: Display>(self, f: impl FnOnce() -> C) -> Result<T>;
}

impl<T, E: Into<Error>> ResultExt<T> for std::result::Result<T, E> {
    fn context(self, context: impl Display) -> Result<T> {
        self.with_context(|| context)
    }

    fn with_context<C: Display>(self, f: impl FnOnce() -> C) -> Result<T> {
        self.map_err(|e| Error::Context {
            context: f().to_string(),
            source: Box::new(e.into()),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn context_wraps_and_root_unwraps() {
        let r: Result<()> = Err(Error::KeyNotFound);
        let err = r.context("while reading").context("outer").unwrap_err();
        assert_eq!(err.to_string(), "outer: while reading: Key not found");
        assert!(matches!(err.root(), Error::KeyNotFound));
    }
}
