//! Wiring: channels, threads, pools, and the run lifecycle.
//!
//! ```text
//! tokio (I/O)        decode threads      process pool        dedup thread   redact pool     sink thread
//! fetch_shard ─mpsc─► gunzip+WARC ─raw─► extract/filter/ ─cand─► index ─uniq─► PII regex ─sink─► Parquet
//!                          │              fingerprint │            │                       ▲   + commit
//!                          └─ dead letters, ShardEnd ─┴── drops ───┴───────────────────────┘
//! ```
//!
//! Every arrow is a *bounded* channel, so the slowest stage sets the pace for
//! all of them and memory in flight stays constant. Each stage exits when its
//! input channel is closed and drained, then drops its senders, which closes
//! the next stage's input: shutdown propagates downstream in order, and an
//! error anywhere propagates upstream through failed sends.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use crossbeam_channel::{bounded, Receiver};
use futures::StreamExt;
use serde::Serialize;

use crate::checkpoint::{read_dedup_delta, Checkpoint, OutputLayout};
use crate::config::Config;
use crate::dedup::{DedupIndex, Deduplicator, Fingerprinter, Verdict};
use crate::doc::{Candidate, CleanDoc, DeadLetter, Drop, RawRecord, ShardSpec, SinkMsg};
use crate::error::{Result, SieveError};
use crate::extract::{ExtractError, Extractor, HtmlExtractor};
use crate::filter::{FilterChain, FilterDoc};
use crate::ingest::{decode_shard, fetch_shard, resolve_inputs, ChannelReader};
use crate::metrics::{self as m, QueueProbe};
use crate::pii::{Redactor, RegexRedactor};
use crate::sink::{totals, ShardName, Sink, SinkReport, Totals};

/// The pluggable, stateless stage implementations, shared by reference across
/// worker threads. Defaults come from config; tests and benchmarks can
/// substitute their own. The stateful dedup index is passed separately because
/// it is *moved* into its single owning thread rather than shared.
pub struct Components {
    /// HTML/text extractor.
    pub extractor: Arc<dyn Extractor>,
    /// Ordered filter chain.
    pub filters: Arc<FilterChain>,
    /// Exact + MinHash fingerprinting.
    pub fingerprinter: Arc<Fingerprinter>,
    /// PII redactor.
    pub redactor: Arc<dyn Redactor>,
}

impl Components {
    /// Build the default components from config.
    pub fn from_config(cfg: &Config) -> Result<Self> {
        Ok(Self {
            extractor: Arc::new(HtmlExtractor::new(&cfg.extract)),
            filters: Arc::new(FilterChain::from_config(&cfg.filters)?),
            fingerprinter: Arc::new(Fingerprinter::new(&cfg.dedup)),
            redactor: Arc::new(RegexRedactor::new(&cfg.pii)?),
        })
    }
}

/// What to run.
#[derive(Debug, Clone)]
pub struct RunOptions {
    /// Input specs (paths, directories, URLs, prefixes).
    pub inputs: Vec<String>,
    /// Output directory.
    pub output: PathBuf,
    /// Continue from an existing checkpoint in `output`.
    pub resume: bool,
    /// Process at most this many (pending) shards.
    pub max_shards: Option<usize>,
}

/// Throughput of one stage.
#[derive(Debug, Clone, Default, Serialize)]
pub struct StageStats {
    /// Items the stage handled.
    pub items: u64,
    /// Thread-seconds spent working (excludes time blocked on channels).
    pub busy_seconds: f64,
    /// `items / wall_seconds` of the run.
    pub items_per_sec: f64,
    /// `busy_seconds / wall_seconds`: how many cores the stage kept busy.
    /// Close to its thread count ⇒ this stage is the bottleneck.
    pub utilization_cores: f64,
}

/// Summary of a run, also written as the JSON manifest.
#[derive(Debug, Clone, Serialize)]
pub struct RunReport {
    /// Crate version.
    pub sieve_version: &'static str,
    /// Unix start time.
    pub started_at_unix: u64,
    /// Wall-clock seconds.
    pub wall_seconds: f64,
    /// Hex config fingerprint.
    pub config_fingerprint: String,
    /// Full configuration used.
    pub config: Config,
    /// Filter family order (drop attribution depends on it).
    pub filter_order: Vec<&'static str>,
    /// Shards found in the inputs.
    pub shards_total: usize,
    /// Shards skipped because the checkpoint already had them.
    pub shards_already_committed: usize,
    /// What the sink did this run.
    pub sink: SinkReport,
    /// Documents processed in *this* run (excludes resumed shards).
    pub run_docs_in: u64,
    /// `run_docs_in / wall_seconds`.
    pub run_docs_per_sec: f64,
    /// Per-stage throughput for this run.
    pub stages: BTreeMap<&'static str, StageStats>,
    /// Highest observed depth of each queue.
    pub queue_max_depth: BTreeMap<&'static str, usize>,
    /// Documents in the dedup index at the end (restored + inserted).
    pub dedup_index_docs: usize,
    /// Totals over *all* committed shards (including earlier runs).
    pub totals: Totals,
}

#[derive(Default)]
struct Counters {
    items: AtomicU64,
    busy_ns: AtomicU64,
}

impl Counters {
    fn add(&self, items: u64, ns: u64) {
        self.items.fetch_add(items, Ordering::Relaxed);
        self.busy_ns.fetch_add(ns, Ordering::Relaxed);
    }
}

fn spawn<T: Send + 'static>(
    name: &str,
    f: impl FnOnce() -> T + Send + 'static,
) -> Result<JoinHandle<T>> {
    std::thread::Builder::new()
        .name(name.into())
        .spawn(f)
        .map_err(|e| SieveError::Pipeline(format!("spawning {name}: {e}")))
}

fn join<T>(h: JoinHandle<T>, name: &str) -> Result<T> {
    h.join()
        .map_err(|_| SieveError::Pipeline(format!("{name} thread panicked")))
}

/// Run `f` on every item of `rx`, on every thread of `pool`, until the channel
/// is closed and drained or `f` reports that its downstream is gone.
///
/// Each pool thread loops on the shared crossbeam receiver directly
/// (`ThreadPool::broadcast`). An earlier version used `rx.into_iter().par_bridge()`;
/// profiling showed 19–25% of process-worker time waiting on par_bridge's
/// internal mutex, because the worker that holds it blocks inside `recv()`
/// while the others queue behind it. crossbeam receivers are MPMC and need no
/// such lock. Blocking inside the pool is fine because each pool is dedicated
/// to one stage (see DECISIONS §3).
fn drain_on_pool<T: Send>(
    pool: &rayon::ThreadPool,
    rx: &Receiver<T>,
    f: impl Fn(T) -> std::result::Result<(), ()> + Sync,
) {
    pool.broadcast(|_| {
        for item in rx.iter() {
            if f(item).is_err() {
                break; // downstream closed: the pipeline is shutting down
            }
        }
    });
}

fn rayon_pool(threads: usize, prefix: &'static str) -> Result<rayon::ThreadPool> {
    rayon::ThreadPoolBuilder::new()
        .num_threads(threads)
        .thread_name(move |i| format!("{prefix}-{i}"))
        .build()
        .map_err(|e| SieveError::Pipeline(format!("building {prefix} pool: {e}")))
}

/// The CPU-heavy per-document work: extract → filter → fingerprint.
/// Pure function of its input plus shared immutable components, which is what
/// lets it run on any number of threads with no coordination.
pub fn process_record(rec: RawRecord, c: &Components) -> std::result::Result<Candidate, SinkMsg> {
    let text = match c.extractor.extract(&rec) {
        Ok(t) => t,
        Err(ExtractError::Drop(rule)) => {
            return Err(SinkMsg::Drop(Drop {
                meta: rec.meta,
                rule,
                value: f64::NAN,
            }))
        }
        Err(ExtractError::Malformed(reason)) => {
            return Err(SinkMsg::DeadLetter(DeadLetter {
                shard: rec.meta.shard,
                record_id: Some(rec.meta.record_id),
                url: Some(rec.meta.url),
                snippet: String::new(),
                reason,
            }))
        }
    };
    drop(rec.body);
    let mut doc = FilterDoc {
        text,
        ..Default::default()
    };
    if let Err(rej) = c.filters.apply(&mut doc) {
        return Err(SinkMsg::Drop(Drop {
            meta: rec.meta,
            rule: rej.rule,
            value: rej.value,
        }));
    }
    doc.signals.word_count = doc.text.split_whitespace().count() as u32;
    let fingerprint = c.fingerprinter.fingerprint(&doc.text);
    Ok(Candidate {
        meta: rec.meta,
        text: doc.text,
        signals: doc.signals,
        fingerprint,
    })
}

/// Run the pipeline to completion.
pub fn run(cfg: Config, opts: RunOptions) -> Result<RunReport> {
    let components = Components::from_config(&cfg)?;
    let index = Box::new(DedupIndex::new(&cfg.dedup));
    run_with(cfg, opts, components, index)
}

/// Run with caller-supplied components and dedup index.
pub fn run_with(
    cfg: Config,
    opts: RunOptions,
    comps: Components,
    mut index: Box<dyn Deduplicator>,
) -> Result<RunReport> {
    cfg.validate()?;
    let started = Instant::now();
    let started_at_unix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs());
    let p = &cfg.pipeline;
    m::describe();

    // tokio is used for I/O only, on a small dedicated runtime.
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(p.io_threads.max(1))
        .thread_name("sieve-io")
        .enable_all()
        .build()
        .map_err(|e| SieveError::Pipeline(format!("tokio runtime: {e}")))?;

    let shards = rt.block_on(resolve_inputs(&opts.inputs))?;

    // --- checkpoint / resume -------------------------------------------------
    let layout = OutputLayout::create(&opts.output)?;
    let fingerprint = cfg.output_fingerprint();
    let checkpoint = match Checkpoint::load(&layout.checkpoint())? {
        Some(ck) if opts.resume => {
            ck.ensure_fingerprint(fingerprint)?;
            ck
        }
        Some(_) => {
            return Err(SieveError::Checkpoint(format!(
            "{} already contains a checkpoint; pass --resume to continue it or use a new directory",
            opts.output.display()
        )))
        }
        None => Checkpoint::new(fingerprint),
    };
    let stale = layout.remove_stale_tmp()?;
    if stale > 0 {
        tracing::info!(files = stale, "removed temp files from an interrupted run");
    }

    // Restore dedup state from committed shards, so documents kept before a
    // crash still suppress their duplicates in the remaining shards.
    for rec in checkpoint.shards.values() {
        for fp in read_dedup_delta(&layout.dedup_delta(&rec.stem))? {
            index.insert(&fp);
        }
    }
    let already = shards
        .iter()
        .filter(|s| checkpoint.shards.contains_key(&s.location))
        .count();
    let mut pending: Vec<ShardSpec> = shards
        .iter()
        .filter(|s| !checkpoint.shards.contains_key(&s.location))
        .cloned()
        .collect();
    if let Some(n) = opts.max_shards {
        pending.truncate(n);
    }
    tracing::info!(
        total = shards.len(),
        already_committed = already,
        pending = pending.len(),
        restored_index_docs = index.len(),
        "starting"
    );
    let names: Arc<Vec<ShardName>> = Arc::new(
        shards
            .iter()
            .map(|s| ShardName {
                location: s.location.clone(),
                stem: s.stem.clone(),
            })
            .collect(),
    );

    // --- channels -------------------------------------------------------------
    let cap = p.channel_capacity;
    let (raw_tx, raw_rx) = bounded::<RawRecord>(cap);
    let (cand_tx, cand_rx) = bounded::<Candidate>(cap);
    let (uniq_tx, uniq_rx) = bounded::<Candidate>(cap);
    let (sink_tx, sink_rx) = bounded::<SinkMsg>(cap);

    let stop_sampler = Arc::new(AtomicBool::new(false));
    let probe = |name: &'static str, len: Box<dyn Fn() -> usize + Send>| QueueProbe {
        name,
        len,
        capacity: cap,
        max_seen: Arc::new(AtomicUsize::new(0)),
    };
    let probes = vec![
        probe(
            "raw",
            Box::new({
                let r = raw_rx.clone();
                move || r.len()
            }),
        ),
        probe(
            "candidates",
            Box::new({
                let r = cand_rx.clone();
                move || r.len()
            }),
        ),
        probe(
            "unique",
            Box::new({
                let r = uniq_rx.clone();
                move || r.len()
            }),
        ),
        probe(
            "sink",
            Box::new({
                let r = sink_rx.clone();
                move || r.len()
            }),
        ),
    ];
    let max_seen: Vec<(&'static str, Arc<AtomicUsize>)> = probes
        .iter()
        .map(|p| (p.name, p.max_seen.clone()))
        .collect();
    let sampler = m::spawn_queue_sampler(probes, stop_sampler.clone())
        .map_err(|e| SieveError::Pipeline(format!("sampler: {e}")))?;

    let ingest_ctr = Arc::new(Counters::default());
    let process_ctr = Arc::new(Counters::default());
    let dedup_ctr = Arc::new(Counters::default());
    let redact_ctr = Arc::new(Counters::default());

    // --- sink -----------------------------------------------------------------
    let sink = Sink::new(
        layout.clone(),
        cfg.sink.clone(),
        names.clone(),
        if cfg.dedup.near { cfg.dedup.bands } else { 0 },
        checkpoint,
        cfg.debug.crash_after_commits,
    );
    let sink_h = spawn("sink", move || sink.run(sink_rx))?;

    // --- redact pool ------------------------------------------------------------
    // A *separate* rayon pool from the process stage. With one shared pool, all
    // workers could end up blocked in `send` on a full queue whose consumer is
    // itself a task waiting for a free worker of the same pool: a deadlock.
    let redact_h = {
        let pool = rayon_pool(p.redact_threads.max(1), "redact")?;
        let redactor = comps.redactor.clone();
        let sink_tx = sink_tx.clone();
        let ctr = redact_ctr.clone();
        spawn("redact-driver", move || {
            let docs = metrics::counter!(m::STAGE_DOCS, "stage" => "redact");
            let busy = metrics::counter!(m::STAGE_BUSY_NS, "stage" => "redact");
            let pii_c = [
                metrics::counter!(m::PII_REDACTIONS, "kind" => "email"),
                metrics::counter!(m::PII_REDACTIONS, "kind" => "phone"),
                metrics::counter!(m::PII_REDACTIONS, "kind" => "ip"),
            ];
            drain_on_pool(&pool, &uniq_rx, |c: Candidate| {
                let t0 = Instant::now();
                let (text, counts) = redactor.redact(&c.text);
                let text = text.into_owned();
                pii_c[0].increment(u64::from(counts.email));
                pii_c[1].increment(u64::from(counts.phone));
                pii_c[2].increment(u64::from(counts.ip));
                let doc = CleanDoc {
                    meta: c.meta,
                    text,
                    signals: c.signals,
                    fingerprint: c.fingerprint,
                    pii_redactions: counts.total(),
                };
                let ns = t0.elapsed().as_nanos() as u64;
                ctr.add(1, ns);
                docs.increment(1);
                busy.increment(ns);
                sink_tx.send(SinkMsg::Keep(Box::new(doc))).map_err(|_| ())
            })
        })?
    };

    // --- dedup thread -------------------------------------------------------------
    let dedup_h = {
        let sink_tx = sink_tx.clone();
        let ctr = dedup_ctr.clone();
        spawn("dedup", move || {
            let input = metrics::counter!(m::DEDUP_INPUT);
            let docs = metrics::counter!(m::STAGE_DOCS, "stage" => "dedup");
            let busy = metrics::counter!(m::STAGE_BUSY_NS, "stage" => "dedup");
            let exact = metrics::counter!(m::DROPS, "rule" => "dedup.exact");
            let near = metrics::counter!(m::DROPS, "rule" => "dedup.near");
            let size = metrics::gauge!(m::DEDUP_INDEX_DOCS);
            for c in cand_rx {
                let t0 = Instant::now();
                input.increment(1);
                let verdict = index.check_and_insert(&c.fingerprint);
                let ns = t0.elapsed().as_nanos() as u64;
                ctr.add(1, ns);
                busy.increment(ns);
                let sent = match verdict {
                    Verdict::Unique => {
                        docs.increment(1);
                        uniq_tx.send(c).is_ok()
                    }
                    Verdict::ExactDuplicate | Verdict::NearDuplicate => {
                        let rule = if verdict == Verdict::ExactDuplicate {
                            exact.increment(1);
                            "dedup.exact"
                        } else {
                            near.increment(1);
                            "dedup.near"
                        };
                        sink_tx
                            .send(SinkMsg::Drop(Drop {
                                meta: c.meta,
                                rule,
                                value: f64::NAN,
                            }))
                            .is_ok()
                    }
                };
                if !sent {
                    break;
                }
                if index.len() % 1024 == 0 {
                    size.set(index.len() as f64);
                }
            }
            size.set(index.len() as f64);
            index.len()
        })?
    };

    // --- process pool ----------------------------------------------------------------
    let process_h = {
        let threads = if p.process_threads == 0 {
            std::thread::available_parallelism().map_or(4, |n| n.get())
        } else {
            p.process_threads
        };
        let pool = rayon_pool(threads, "process")?;
        let shared = Arc::new(Components {
            extractor: comps.extractor.clone(),
            filters: comps.filters.clone(),
            fingerprinter: comps.fingerprinter.clone(),
            redactor: comps.redactor.clone(),
        });
        let sink_tx = sink_tx.clone();
        let ctr = process_ctr.clone();
        let rules = shared.filters.all_rules();
        spawn("process-driver", move || {
            for r in rules {
                metrics::counter!(m::DROPS, "rule" => r).absolute(0);
            }
            let docs = metrics::counter!(m::STAGE_DOCS, "stage" => "process");
            let busy = metrics::counter!(m::STAGE_BUSY_NS, "stage" => "process");
            // Each worker takes one record at a time from the bounded channel, so
            // the backlog stays in the channel (visible, bounded), never in a pool queue.
            drain_on_pool(&pool, &raw_rx, |rec| {
                let t0 = Instant::now();
                let out = process_record(rec, &shared);
                let ns = t0.elapsed().as_nanos() as u64;
                ctr.add(1, ns);
                busy.increment(ns);
                match out {
                    Ok(c) => {
                        docs.increment(1);
                        cand_tx.send(c).map_err(|_| ())
                    }
                    Err(msg) => {
                        if let SinkMsg::Drop(d) = &msg {
                            metrics::counter!(m::DROPS, "rule" => d.rule).increment(1);
                        }
                        sink_tx.send(msg).map_err(|_| ())
                    }
                }
            })
        })?
    };

    // --- ingest on tokio -------------------------------------------------------------------
    let max_record = p.max_record_bytes;
    let chunk_q = p.io_chunk_queue.max(1);
    let concurrency = p.max_concurrent_shards;
    {
        let raw_tx = raw_tx.clone();
        let sink_tx = sink_tx.clone();
        let ctr = ingest_ctr.clone();
        rt.block_on(async move {
            futures::stream::iter(pending)
                .for_each_concurrent(concurrency, |spec| {
                    let raw_tx = raw_tx.clone();
                    let sink_tx = sink_tx.clone();
                    let ctr = ctr.clone();
                    async move {
                        let (btx, brx) = tokio::sync::mpsc::channel(chunk_q);
                        let (done_tx, done_rx) = tokio::sync::oneshot::channel();
                        let id = spec.id;
                        let spawned = std::thread::Builder::new()
                            .name(format!("decode-{id}"))
                            .spawn(move || {
                                let st = decode_shard(
                                    id,
                                    ChannelReader::new(brx),
                                    max_record,
                                    &raw_tx,
                                    &sink_tx,
                                );
                                ctr.add(st.docs, st.busy_ns);
                                let _ = done_tx.send(());
                            });
                        if let Err(e) = spawned {
                            tracing::error!(error = %e, "could not spawn decoder thread");
                            return;
                        }
                        fetch_shard(&spec, btx).await;
                        let _ = done_rx.await;
                    }
                })
                .await;
        });
    }
    // Close the pipeline's front door: once the last decoder exits and these
    // originals are dropped, the process stage sees end-of-input.
    drop(raw_tx);
    drop(sink_tx);

    join(process_h, "process")?;
    let index_docs = join(dedup_h, "dedup")?;
    join(redact_h, "redact")?;
    let (sink_report, checkpoint) = join(sink_h, "sink")??;
    stop_sampler.store(true, Ordering::Relaxed);
    let _ = sampler.join();
    rt.shutdown_timeout(Duration::from_secs(1));

    let wall = started.elapsed().as_secs_f64();
    let stat = |c: &Counters| {
        let items = c.items.load(Ordering::Relaxed);
        let busy = c.busy_ns.load(Ordering::Relaxed) as f64 / 1e9;
        StageStats {
            items,
            busy_seconds: busy,
            items_per_sec: items as f64 / wall.max(1e-9),
            utilization_cores: busy / wall.max(1e-9),
        }
    };
    let mut stages = BTreeMap::new();
    stages.insert("1_ingest", stat(&ingest_ctr));
    stages.insert("2_process", stat(&process_ctr));
    stages.insert("3_dedup", stat(&dedup_ctr));
    stages.insert("4_redact", stat(&redact_ctr));
    let sink_items = sink_report.committed.len() as u64;
    stages.insert(
        "5_sink",
        StageStats {
            items: sink_items,
            busy_seconds: sink_report.busy_ns as f64 / 1e9,
            items_per_sec: 0.0,
            utilization_cores: sink_report.busy_ns as f64 / 1e9 / wall.max(1e-9),
        },
    );
    let run_docs_in = ingest_ctr.items.load(Ordering::Relaxed);
    let report = RunReport {
        sieve_version: env!("CARGO_PKG_VERSION"),
        started_at_unix,
        wall_seconds: wall,
        config_fingerprint: format!("{fingerprint:016x}"),
        filter_order: comps.filters.order(),
        config: cfg,
        shards_total: shards.len(),
        shards_already_committed: already,
        sink: sink_report,
        run_docs_in,
        run_docs_per_sec: run_docs_in as f64 / wall.max(1e-9),
        stages,
        queue_max_depth: max_seen
            .into_iter()
            .map(|(n, v)| (n, v.load(Ordering::Relaxed)))
            .collect(),
        dedup_index_docs: index_docs,
        totals: totals(&checkpoint),
    };
    crate::checkpoint::atomic_write(&layout.manifest(), &serde_json::to_vec_pretty(&report)?)?;
    Ok(report)
}
