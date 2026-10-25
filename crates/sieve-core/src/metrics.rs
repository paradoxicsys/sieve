//! Metric names and the queue-depth sampler.
//!
//! The pipeline records through the `metrics` facade only; which exporter (if
//! any) receives the values is decided by the binary. With no exporter
//! installed every macro is a cheap no-op, so library users and tests pay
//! nothing for instrumentation.
//!
//! Hot paths fetch a `Counter` handle once per thread and reuse it, rather than
//! calling `counter!(name, labels)` per document, which would hash the key on
//! every call.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// Documents leaving each stage. Label `stage`. `rate()` of this is docs/sec per stage.
pub const STAGE_DOCS: &str = "sieve_stage_docs_total";
/// Nanoseconds threads spent working (not waiting) in a stage. Label `stage`.
/// `rate(busy) / 1e9` = number of cores a stage keeps busy.
pub const STAGE_BUSY_NS: &str = "sieve_stage_busy_nanoseconds_total";
/// Documents dropped. Label `rule` (e.g. `gopher_quality.stop_words`, `dedup.near`).
pub const DROPS: &str = "sieve_drops_total";
/// Records that could not be parsed.
pub const DEAD_LETTERS: &str = "sieve_dead_letters_total";
/// Documents entering dedup. Duplicate ratio = drops{rule=~"dedup.*"} / this.
pub const DEDUP_INPUT: &str = "sieve_dedup_input_total";
/// Current items in a bounded queue. Label `queue`.
pub const QUEUE_DEPTH: &str = "sieve_queue_depth";
/// Capacity of a bounded queue. Label `queue`. Lets dashboards show "% full".
pub const QUEUE_CAPACITY: &str = "sieve_queue_capacity";
/// Compressed input bytes read.
pub const BYTES_READ: &str = "sieve_input_bytes_total";
/// PII spans redacted. Label `kind`.
pub const PII_REDACTIONS: &str = "sieve_pii_redactions_total";
/// Shards committed (made durable).
pub const SHARDS_COMMITTED: &str = "sieve_shards_committed_total";
/// Rows written to Parquet.
pub const ROWS_WRITTEN: &str = "sieve_rows_written_total";
/// Entries in the in-memory LSH index (memory grows with this).
pub const DEDUP_INDEX_DOCS: &str = "sieve_dedup_index_docs";

/// A named queue whose length can be sampled.
pub struct QueueProbe {
    /// `queue` label value.
    pub name: &'static str,
    /// Returns the current length.
    pub len: Box<dyn Fn() -> usize + Send>,
    /// Configured capacity.
    pub capacity: usize,
    /// Highest length observed by the sampler.
    pub max_seen: Arc<AtomicUsize>,
}

/// Periodically publish queue depths as gauges.
///
/// Channel length is a cheap atomic read, so sampling every 200 ms costs
/// nothing. A queue pinned at capacity identifies the stage *after* it as the
/// bottleneck; a queue pinned at zero means its consumer is starved.
pub fn spawn_queue_sampler(
    probes: Vec<QueueProbe>,
    stop: Arc<AtomicBool>,
) -> std::io::Result<std::thread::JoinHandle<()>> {
    std::thread::Builder::new()
        .name("queue-sampler".into())
        .spawn(move || {
            for p in &probes {
                metrics::gauge!(QUEUE_CAPACITY, "queue" => p.name).set(p.capacity as f64);
            }
            let sample = || {
                for p in &probes {
                    let len = (p.len)();
                    p.max_seen.fetch_max(len, Ordering::Relaxed);
                    metrics::gauge!(QUEUE_DEPTH, "queue" => p.name).set(len as f64);
                }
            };
            while !stop.load(Ordering::Relaxed) {
                sample();
                std::thread::sleep(Duration::from_millis(200));
            }
            sample(); // final values, so a lingering exporter does not show stale depths
        })
}

/// Register human-readable descriptions (shown by Prometheus as `# HELP`).
pub fn describe() {
    metrics::describe_counter!(STAGE_DOCS, "Documents emitted by each pipeline stage");
    metrics::describe_counter!(
        STAGE_BUSY_NS,
        "Nanoseconds of thread time spent working per stage"
    );
    metrics::describe_counter!(DROPS, "Documents dropped, by rule");
    metrics::describe_counter!(
        DEAD_LETTERS,
        "Malformed records sent to the dead-letter file"
    );
    metrics::describe_counter!(DEDUP_INPUT, "Documents entering the dedup stage");
    metrics::describe_gauge!(QUEUE_DEPTH, "Items currently buffered in a bounded queue");
    metrics::describe_gauge!(QUEUE_CAPACITY, "Capacity of a bounded queue");
    metrics::describe_counter!(BYTES_READ, "Compressed input bytes read from the source");
    metrics::describe_counter!(PII_REDACTIONS, "PII spans replaced, by kind");
    metrics::describe_counter!(SHARDS_COMMITTED, "Shards durably committed");
    metrics::describe_counter!(ROWS_WRITTEN, "Rows written to Parquet data files");
    metrics::describe_gauge!(DEDUP_INDEX_DOCS, "Documents held in the dedup index");
}
