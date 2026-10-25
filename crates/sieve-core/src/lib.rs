//! # sieve-core
//!
//! Streaming clean + dedup of Common Crawl WARC/WET data into Parquet.
//!
//! The crate is organised by pipeline stage. Each stage's logic sits behind a
//! small trait ([`extract::Extractor`], [`filter::Filter`],
//! [`dedup::Deduplicator`], [`pii::Redactor`]) so it can be unit-tested and
//! benchmarked in isolation; [`pipeline`] wires the stages together with
//! bounded channels, thread pools, and the checkpointing sink.
//!
//! Start reading at [`pipeline::run`].

pub mod checkpoint;
pub mod config;
pub mod dedup;
pub mod doc;
pub mod error;
pub mod extract;
pub mod filter;
pub mod ingest;
pub mod metrics;
pub mod pii;
pub mod pipeline;
pub mod sink;
pub mod text;

pub use config::Config;
pub use error::{Result, SieveError};
pub use pipeline::{run, run_with, Components, RunOptions, RunReport};
