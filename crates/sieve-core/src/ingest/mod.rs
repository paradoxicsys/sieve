//! Ingest: resolve inputs to shards, fetch bytes asynchronously, decode
//! synchronously.
//!
//! The split is the tokio/CPU boundary of the whole pipeline:
//! * [`source`] runs on tokio and only moves bytes (network / disk).
//! * [`reader`] runs on a plain OS thread per shard and does the CPU work of
//!   gunzip + WARC parsing, pulling bytes through a bounded `tokio::mpsc`
//!   channel with `blocking_recv`.

pub mod reader;
pub mod source;

pub use reader::{decode_shard, ChannelReader, DecodeStats};
pub use source::{fetch_shard, resolve_inputs};
