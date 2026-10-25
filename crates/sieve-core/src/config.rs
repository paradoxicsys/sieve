//! Pipeline configuration, loaded from TOML.
//!
//! Every struct uses `#[serde(default)]` so a config file only needs to mention
//! the knobs it changes, and `deny_unknown_fields` so a typo (`min_word = 50`)
//! fails loudly instead of being silently ignored — a silently ignored filter
//! threshold is the worst kind of data-quality bug because nothing crashes.

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::error::{Result, SieveError};

/// Top-level configuration for one pipeline run.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    /// Threading, queue sizes, and resource limits.
    pub pipeline: PipelineConfig,
    /// HTML → text extraction.
    pub extract: ExtractConfig,
    /// Quality filters (Gopher / C4 style).
    pub filters: FiltersConfig,
    /// Exact and near-duplicate removal.
    pub dedup: DedupConfig,
    /// PII redaction.
    pub pii: PiiConfig,
    /// Parquet output.
    pub sink: SinkConfig,
    /// Fault injection, used only by tests.
    pub debug: DebugConfig,
}

impl Config {
    /// Load a config from a TOML file.
    pub fn from_file(path: &Path) -> Result<Self> {
        let text = std::fs::read_to_string(path).map_err(|e| SieveError::Config {
            msg: format!("reading {}: {e}", path.display()),
        })?;
        Self::from_toml(&text)
    }

    /// Parse a config from a TOML string and validate it.
    pub fn from_toml(text: &str) -> Result<Self> {
        let cfg: Config =
            toml::from_str(text).map_err(|e| SieveError::Config { msg: e.to_string() })?;
        cfg.validate()?;
        Ok(cfg)
    }

    /// Reject configurations that would make the pipeline wrong rather than slow.
    pub fn validate(&self) -> Result<()> {
        let d = &self.dedup;
        if d.near && d.bands * d.rows_per_band != d.num_perm {
            return Err(SieveError::Config {
                msg: format!(
                    "dedup: bands ({}) * rows_per_band ({}) must equal num_perm ({})",
                    d.bands, d.rows_per_band, d.num_perm
                ),
            });
        }
        if d.shingle_size == 0 {
            return Err(SieveError::Config {
                msg: "dedup.shingle_size must be >= 1".into(),
            });
        }
        if self.pipeline.channel_capacity == 0 {
            return Err(SieveError::Config {
                msg: "pipeline.channel_capacity must be >= 1 (0 would be a rendezvous channel)"
                    .into(),
            });
        }
        if self.pipeline.max_concurrent_shards == 0 {
            return Err(SieveError::Config {
                msg: "pipeline.max_concurrent_shards must be >= 1".into(),
            });
        }
        Ok(())
    }

    /// A stable hash of every setting that changes *what* is written.
    ///
    /// Stored in the checkpoint so `--resume` refuses to mix shards produced
    /// under different filter/dedup settings into one dataset. Thread counts and
    /// queue sizes are deliberately excluded: they change speed, not output.
    pub fn output_fingerprint(&self) -> u64 {
        #[derive(Serialize)]
        struct Semantic<'a> {
            extract: &'a ExtractConfig,
            filters: &'a FiltersConfig,
            dedup: &'a DedupConfig,
            pii: &'a PiiConfig,
            max_record_bytes: usize,
        }
        let s = Semantic {
            extract: &self.extract,
            filters: &self.filters,
            dedup: &self.dedup,
            pii: &self.pii,
            max_record_bytes: self.pipeline.max_record_bytes,
        };
        // serde_json of plain structs with no maps is deterministic.
        let bytes = serde_json::to_vec(&s).unwrap_or_default();
        xxhash_rust::xxh3::xxh3_64(&bytes)
    }
}

/// Serialise a config to TOML (used by `sieve print-config`).
pub fn to_toml(cfg: &Config) -> Result<String> {
    toml::to_string_pretty(cfg).map_err(|e| SieveError::Config { msg: e.to_string() })
}

/// Threading and resource limits.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct PipelineConfig {
    /// Capacity of each bounded inter-stage channel, in items. This is the
    /// backpressure knob: memory in flight ≈ stages × capacity × doc size.
    pub channel_capacity: usize,
    /// Worker threads for the CPU-heavy process stage (0 = number of cores).
    pub process_threads: usize,
    /// Worker threads for the PII redaction stage.
    pub redact_threads: usize,
    /// Input shards decoded concurrently (one decoder thread each).
    pub max_concurrent_shards: usize,
    /// Byte chunks buffered between the async fetcher and a decoder thread.
    pub io_chunk_queue: usize,
    /// Records whose body exceeds this many bytes are dead-lettered unread.
    pub max_record_bytes: usize,
    /// Worker threads for the tokio I/O runtime.
    pub io_threads: usize,
}

impl Default for PipelineConfig {
    fn default() -> Self {
        Self {
            channel_capacity: 256,
            process_threads: 0,
            redact_threads: 1,
            max_concurrent_shards: 2,
            io_chunk_queue: 32,
            max_record_bytes: 4 * 1024 * 1024,
            io_threads: 2,
        }
    }
}

/// HTML extraction settings.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ExtractConfig {
    /// Elements whose whole subtree is boilerplate or non-text.
    pub skip_tags: Vec<String>,
    /// Substrings of `class`/`id` attributes marking boilerplate containers.
    pub boilerplate_markers: Vec<String>,
}

impl Default for ExtractConfig {
    fn default() -> Self {
        Self {
            skip_tags: [
                "script", "style", "noscript", "nav", "footer", "header", "aside", "form",
                "iframe", "svg", "template", "button", "select", "head", "menu",
            ]
            .map(String::from)
            .to_vec(),
            boilerplate_markers: [
                "navbar",
                "nav-",
                "menu",
                "footer",
                "sidebar",
                "breadcrumb",
                "cookie",
                "banner",
                "share",
                "social",
                "advert",
                "popup",
                "newsletter",
            ]
            .map(String::from)
            .to_vec(),
        }
    }
}

/// All quality filters. Each sub-section can be disabled independently.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct FiltersConfig {
    /// Document length bounds.
    pub length: LengthConfig,
    /// Language identification.
    pub language: LanguageConfig,
    /// Gopher repetition heuristics (Rae et al. 2021, table A1).
    pub gopher_repetition: GopherRepetitionConfig,
    /// Gopher quality heuristics.
    pub gopher_quality: GopherQualityConfig,
    /// C4 heuristics (Raffel et al. 2020).
    pub c4: C4Config,
    /// Bad-word list filter (C4 style).
    pub bad_words: BadWordsConfig,
}

/// Word-count bounds.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct LengthConfig {
    /// Enable this filter.
    pub enabled: bool,
    /// Minimum whitespace-separated words.
    pub min_words: usize,
    /// Maximum whitespace-separated words.
    pub max_words: usize,
}

impl Default for LengthConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            min_words: 50,
            max_words: 100_000,
        }
    }
}

/// Language-ID settings.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct LanguageConfig {
    /// Enable this filter.
    pub enabled: bool,
    /// ISO 639-3 codes to keep (whatlang codes, e.g. `"eng"`). Empty = keep all.
    pub allowed: Vec<String>,
    /// Minimum whatlang confidence in [0, 1].
    pub min_confidence: f64,
    /// Only the first N bytes are classified: language ID is O(n) and the
    /// answer stabilises after a few KB.
    pub sample_bytes: usize,
}

impl Default for LanguageConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            allowed: vec!["eng".into()],
            min_confidence: 0.3,
            sample_bytes: 4096,
        }
    }
}

/// Gopher repetition thresholds; a doc is dropped when a measured fraction
/// exceeds the configured maximum.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct GopherRepetitionConfig {
    /// Enable this filter.
    pub enabled: bool,
    /// Max fraction of lines that are duplicates of an earlier line.
    pub dup_line_frac: f64,
    /// Max fraction of paragraphs that are duplicates.
    pub dup_para_frac: f64,
    /// Max fraction of characters inside duplicate lines.
    pub dup_line_char_frac: f64,
    /// Max fraction of characters inside duplicate paragraphs.
    pub dup_para_char_frac: f64,
    /// `(n, max)`: max char fraction covered by the most common n-gram.
    pub top_ngram_char_frac: Vec<(usize, f64)>,
    /// `(n, max)`: max char fraction covered by repeated n-grams.
    pub dup_ngram_char_frac: Vec<(usize, f64)>,
}

impl Default for GopherRepetitionConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            dup_line_frac: 0.3,
            dup_para_frac: 0.3,
            dup_line_char_frac: 0.2,
            dup_para_char_frac: 0.2,
            top_ngram_char_frac: vec![(2, 0.20), (3, 0.18), (4, 0.16)],
            dup_ngram_char_frac: vec![
                (5, 0.15),
                (6, 0.14),
                (7, 0.13),
                (8, 0.12),
                (9, 0.11),
                (10, 0.10),
            ],
        }
    }
}

/// Gopher quality thresholds.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct GopherQualityConfig {
    /// Enable this filter.
    pub enabled: bool,
    /// Minimum mean word length in characters.
    pub min_mean_word_len: f64,
    /// Maximum mean word length in characters.
    pub max_mean_word_len: f64,
    /// Max ratio of `#` or ellipsis occurrences to words.
    pub max_symbol_word_ratio: f64,
    /// Max fraction of lines starting with a bullet.
    pub max_bullet_lines_frac: f64,
    /// Max fraction of lines ending with an ellipsis.
    pub max_ellipsis_lines_frac: f64,
    /// Min fraction of words containing at least one alphabetic character.
    pub min_alpha_words_frac: f64,
    /// Min number of distinct stop words present.
    pub min_stop_words: usize,
    /// The stop-word list (Gopher uses these eight).
    pub stop_words: Vec<String>,
}

impl Default for GopherQualityConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            min_mean_word_len: 3.0,
            max_mean_word_len: 10.0,
            max_symbol_word_ratio: 0.1,
            max_bullet_lines_frac: 0.9,
            max_ellipsis_lines_frac: 0.3,
            min_alpha_words_frac: 0.8,
            min_stop_words: 2,
            stop_words: ["the", "be", "to", "of", "and", "that", "have", "with"]
                .map(String::from)
                .to_vec(),
        }
    }
}

/// C4 heuristics.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct C4Config {
    /// Enable this filter.
    pub enabled: bool,
    /// Remove lines that do not look like prose (C4 line filter). This edits the
    /// document text rather than only accepting/rejecting it.
    pub line_filter: bool,
    /// Lines with fewer words are removed by the line filter.
    pub min_words_per_line: usize,
    /// Lines must end in terminal punctuation to survive the line filter.
    pub require_terminal_punct: bool,
    /// Drop docs with fewer sentences (after line filtering).
    pub min_sentences: usize,
    /// Drop docs containing "lorem ipsum".
    pub drop_lorem_ipsum: bool,
    /// Drop docs containing `{` (C4 does this to remove code; it also removes
    /// legitimate prose that quotes code, which is a known C4 trade-off).
    pub drop_curly_brace: bool,
}

impl Default for C4Config {
    fn default() -> Self {
        Self {
            enabled: true,
            line_filter: true,
            min_words_per_line: 3,
            require_terminal_punct: true,
            min_sentences: 5,
            drop_lorem_ipsum: true,
            drop_curly_brace: true,
        }
    }
}

/// Bad-word list filter.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct BadWordsConfig {
    /// Enable this filter.
    pub enabled: bool,
    /// Word-list file (one word/phrase per line). `None` = the bundled LDNOOBW
    /// English list, which is what C4 used.
    pub path: Option<PathBuf>,
    /// Drop the doc when the number of matches exceeds this (C4 uses 0: any match).
    pub max_matches: usize,
}

impl Default for BadWordsConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            path: None,
            max_matches: 0,
        }
    }
}

/// Dedup settings.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct DedupConfig {
    /// Exact dedup on xxh3 of normalised text.
    pub exact: bool,
    /// Near-dedup with MinHash + LSH.
    pub near: bool,
    /// Number of MinHash permutations (signature length).
    pub num_perm: usize,
    /// LSH bands. A pair collides if *any* band matches exactly.
    pub bands: usize,
    /// Rows (signature values) per band. `bands * rows_per_band == num_perm`.
    pub rows_per_band: usize,
    /// Words per shingle.
    pub shingle_size: usize,
    /// Seed for the permutation coefficients; fixed so runs are reproducible
    /// and so a resumed run hashes identically to the original.
    pub seed: u64,
}

impl Default for DedupConfig {
    fn default() -> Self {
        Self {
            exact: true,
            near: true,
            num_perm: 128,
            bands: 16,
            rows_per_band: 8,
            shingle_size: 5,
            seed: 0x5EED_0F51_E7E5,
        }
    }
}

/// PII redaction settings.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct PiiConfig {
    /// Enable redaction at all.
    pub enabled: bool,
    /// Redact email addresses.
    pub email: bool,
    /// Redact phone numbers.
    pub phone: bool,
    /// Redact IPv4/IPv6 addresses.
    pub ip: bool,
}

impl Default for PiiConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            email: true,
            phone: true,
            ip: true,
        }
    }
}

/// Parquet sink settings.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct SinkConfig {
    /// Rows buffered in Arrow builders before being handed to the Parquet writer.
    pub batch_rows: usize,
    /// Flush a row group once its encoded in-memory size passes this many bytes.
    /// This, not row count, is what bounds sink memory: docs vary 100x in size.
    pub row_group_bytes: usize,
    /// zstd level for the text-heavy data files.
    pub zstd_level: i32,
    /// Also write a Parquet file of dropped docs (url, rule, value).
    pub write_rejects: bool,
}

impl Default for SinkConfig {
    fn default() -> Self {
        Self {
            batch_rows: 2048,
            row_group_bytes: 32 * 1024 * 1024,
            zstd_level: 3,
            write_rejects: true,
        }
    }
}

/// Fault injection. Never set these in production configs.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct DebugConfig {
    /// Abort the process (no cleanup, like `kill -9`) right after the Nth
    /// shard commit. Used by the crash/resume test.
    pub crash_after_commits: Option<usize>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn toml_round_trip() {
        let text = to_toml(&Config::default()).unwrap();
        let back = Config::from_toml(&text).unwrap();
        assert_eq!(
            back.output_fingerprint(),
            Config::default().output_fingerprint()
        );
    }

    #[test]
    fn defaults_validate() {
        Config::default().validate().unwrap();
    }

    #[test]
    fn partial_toml_uses_defaults() {
        let cfg = Config::from_toml("[filters.length]\nmin_words = 10\n").unwrap();
        assert_eq!(cfg.filters.length.min_words, 10);
        assert_eq!(cfg.filters.length.max_words, 100_000);
        assert_eq!(cfg.dedup.num_perm, 128);
    }

    #[test]
    fn unknown_field_is_an_error() {
        assert!(Config::from_toml("[filters.length]\nmin_word = 10\n").is_err());
    }

    #[test]
    fn bands_must_divide_perms() {
        assert!(Config::from_toml("[dedup]\nbands = 10\n").is_err());
    }

    #[test]
    fn fingerprint_ignores_threads_but_not_filters() {
        let a = Config::default();
        let mut b = Config::default();
        b.pipeline.process_threads = 3;
        assert_eq!(a.output_fingerprint(), b.output_fingerprint());
        b.filters.length.min_words = 1;
        assert_ne!(a.output_fingerprint(), b.output_fingerprint());
    }
}
