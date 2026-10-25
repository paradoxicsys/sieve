//! The data types that flow between stages.
//!
//! Every input record is turned into exactly one [`SinkMsg`] terminal: kept,
//! dropped (with a rule name), or dead-lettered. The sink relies on this
//! "one record in, one terminal out" invariant to know when a shard is complete,
//! so any new stage must preserve it.

use std::sync::Arc;

/// Index of an input shard within this run's shard list. Cheap to copy into
/// every message; the sink maps it back to the shard's name.
pub type ShardId = u32;

/// Which kind of WARC record carried the document.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordKind {
    /// `WARC-Type: response` — an HTTP response; body is headers + HTML.
    Response,
    /// `WARC-Type: conversion` — WET plain text already extracted by Common Crawl.
    Conversion,
    /// `WARC-Type: resource` — a stored file with no HTTP envelope.
    Resource,
}

/// Metadata carried by every document from ingest to sink.
#[derive(Debug, Clone)]
pub struct DocMeta {
    /// Shard this record came from.
    pub shard: ShardId,
    /// 0-based position of the record among the shard's emitted docs.
    pub seq: u64,
    /// `WARC-Target-URI`.
    pub url: String,
    /// `WARC-Record-ID`.
    pub record_id: String,
    /// `WARC-Date` as milliseconds since the Unix epoch.
    pub timestamp_ms: i64,
    /// Common Crawl's own language guess (WET `WARC-Identified-Content-Language`),
    /// kept as a cross-check against our language ID.
    pub cc_lang: Option<String>,
}

/// A record straight out of the WARC parser.
#[derive(Debug)]
pub struct RawRecord {
    /// Metadata.
    pub meta: DocMeta,
    /// Record type.
    pub kind: RecordKind,
    /// Record body (HTTP response, or text for WET).
    pub body: Vec<u8>,
}

/// Per-document quality signals, written as Parquet columns so downstream users
/// can re-threshold without re-running the pipeline.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct QualitySignals {
    /// Detected language (ISO 639-3), if language ID ran.
    pub lang: Option<String>,
    /// Language-ID confidence.
    pub lang_score: f32,
    /// Whitespace word count of the final text.
    pub word_count: u32,
    /// Mean word length in characters.
    pub mean_word_len: f32,
    /// max(# ratio, ellipsis ratio) per word.
    pub symbol_word_ratio: f32,
    /// Fraction of duplicate lines.
    pub dup_line_frac: f32,
    /// Fraction of duplicate paragraphs.
    pub dup_para_frac: f32,
    /// Fraction of words with an alphabetic character.
    pub alpha_words_frac: f32,
    /// Distinct stop words present.
    pub stop_words: u32,
    /// Lines removed by the C4 line filter.
    pub c4_lines_removed: u32,
}

/// The near/exact-dedup fingerprint of a document.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Fingerprint {
    /// xxh3-64 of the normalised text.
    pub exact: u64,
    /// One hash per LSH band (empty when near-dedup is disabled).
    pub bands: Vec<u64>,
}

/// A document that passed the filters and is waiting for dedup.
#[derive(Debug)]
pub struct Candidate {
    /// Metadata.
    pub meta: DocMeta,
    /// Cleaned text (pre-redaction).
    pub text: String,
    /// Quality signals computed by the filters.
    pub signals: QualitySignals,
    /// Dedup fingerprint, computed in the parallel stage so the single-threaded
    /// dedup stage only does hash-table lookups.
    pub fingerprint: Fingerprint,
}

/// A fully processed, kept document.
#[derive(Debug)]
pub struct CleanDoc {
    /// Metadata.
    pub meta: DocMeta,
    /// Final, redacted text.
    pub text: String,
    /// Quality signals.
    pub signals: QualitySignals,
    /// Fingerprint, persisted into the shard's dedup delta on commit.
    pub fingerprint: Fingerprint,
    /// Number of PII spans replaced.
    pub pii_redactions: u32,
}

/// Why a document was dropped. `rule` is a stable dotted name
/// (`"gopher_quality.stop_words"`) used in metrics labels and the rejects file.
#[derive(Debug, Clone)]
pub struct Drop {
    /// Metadata of the dropped doc.
    pub meta: DocMeta,
    /// Stable rule name.
    pub rule: &'static str,
    /// The measured value that failed the rule (NaN when not meaningful).
    pub value: f64,
}

/// A record that could not be parsed or decoded.
#[derive(Debug, Clone)]
pub struct DeadLetter {
    /// Shard it came from.
    pub shard: ShardId,
    /// Record id / url if they could be read.
    pub record_id: Option<String>,
    /// URL if known.
    pub url: Option<String>,
    /// What went wrong.
    pub reason: String,
    /// Up to a few hundred bytes of the offending input, lossily decoded.
    pub snippet: String,
}

/// How an input shard ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ShardStatus {
    /// Read to the end.
    Complete,
    /// The data was corrupt part-way (bad gzip / WARC framing). Everything read
    /// before the corruption is kept; retrying would hit the same bytes, so the
    /// shard is still committed.
    Corrupt(String),
    /// The *source* failed (network, permissions). Transient by assumption: the
    /// shard's partial output is discarded and it is not checkpointed, so a
    /// later `--resume` retries it.
    SourceFailed(String),
}

/// Messages consumed by the sink.
#[derive(Debug)]
pub enum SinkMsg {
    /// A kept document.
    Keep(Box<CleanDoc>),
    /// A dropped document.
    Drop(Drop),
    /// An unparseable record.
    DeadLetter(DeadLetter),
    /// Sent by the decoder after its last record: `total` terminals will arrive
    /// for this shard (in any order, possibly before this message).
    ShardEnd {
        /// The shard.
        shard: ShardId,
        /// Number of terminal messages the sink must see for this shard.
        total: u64,
        /// How reading ended.
        status: ShardStatus,
    },
}

impl SinkMsg {
    /// The shard a message belongs to.
    pub fn shard(&self) -> ShardId {
        match self {
            SinkMsg::Keep(d) => d.meta.shard,
            SinkMsg::Drop(d) => d.meta.shard,
            SinkMsg::DeadLetter(d) => d.shard,
            SinkMsg::ShardEnd { shard, .. } => *shard,
        }
    }
}

/// An input shard: where it lives and a stable name used for output files.
#[derive(Debug, Clone)]
pub struct ShardSpec {
    /// Run-local index.
    pub id: ShardId,
    /// Full location (URL or path) as given; the checkpoint key.
    pub location: String,
    /// Stable, filesystem-safe output stem derived from `location`.
    pub stem: String,
    /// The store holding it.
    pub store: Arc<dyn object_store::ObjectStore>,
    /// Path within the store.
    pub path: object_store::path::Path,
}
