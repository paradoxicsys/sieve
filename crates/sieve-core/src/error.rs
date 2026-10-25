//! Library error type.
//!
//! `thiserror` in the library (callers can match on variants), `anyhow` only in
//! the binary (where errors are just reported). That split is the usual Rust
//! convention: libraries expose structured errors, applications add context.

use std::path::PathBuf;

/// Errors surfaced by `sieve-core`.
#[derive(Debug, thiserror::Error)]
pub enum SieveError {
    /// The configuration is invalid; the run must not start.
    #[error("invalid config: {msg}")]
    Config {
        /// Human-readable reason.
        msg: String,
    },
    /// Filesystem error with the path that caused it.
    #[error("io error on {path}: {source}")]
    Io {
        /// Path being accessed.
        path: PathBuf,
        /// Underlying error.
        #[source]
        source: std::io::Error,
    },
    /// Object-store (local/S3/HTTP) error.
    #[error("object store: {0}")]
    ObjectStore(#[from] object_store::Error),
    /// Parquet encoding error.
    #[error("parquet: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),
    /// Arrow array construction error.
    #[error("arrow: {0}")]
    Arrow(#[from] arrow::error::ArrowError),
    /// JSON (checkpoint/manifest) error.
    #[error("json: {0}")]
    Json(#[from] serde_json::Error),
    /// The checkpoint on disk is inconsistent with this run.
    #[error("checkpoint: {0}")]
    Checkpoint(String),
    /// An input specification could not be resolved.
    #[error("input: {0}")]
    Input(String),
    /// A pipeline thread panicked or a channel closed unexpectedly.
    #[error("pipeline: {0}")]
    Pipeline(String),
}

/// Convenience alias.
pub type Result<T, E = SieveError> = std::result::Result<T, E>;

/// Attach a path to an `io::Error`. Bare `io::Error`s ("No such file or
/// directory") are useless in a pipeline that touches thousands of files.
pub(crate) fn io_err(path: impl Into<PathBuf>) -> impl FnOnce(std::io::Error) -> SieveError {
    let path = path.into();
    move |source| SieveError::Io { path, source }
}
