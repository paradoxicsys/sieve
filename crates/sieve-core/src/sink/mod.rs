//! Sink: the single thread that owns all output files and the checkpoint.
//!
//! Documents from several shards arrive interleaved and out of order (the
//! process stage is parallel). The sink keeps one open writer set per in-flight
//! shard and counts terminal messages; when a shard's count reaches the total
//! announced by its decoder, every record of that shard has been accounted for
//! and the shard is committed (or discarded, if its source failed).

pub mod parquet;

use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::io::Write;
use std::path::Path;
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use crossbeam_channel::Receiver;
use serde::Serialize;

use crate::checkpoint::{
    commit_file, tmp_path, write_dedup_delta, Checkpoint, OutputLayout, ShardRecord,
};
use crate::config::SinkConfig;
use crate::doc::{DeadLetter, Fingerprint, ShardId, ShardStatus, SinkMsg};
use crate::error::{io_err, Result};
use crate::metrics as m;
use parquet::{DataWriter, RejectsWriter};

/// Static per-shard info the sink needs.
#[derive(Debug, Clone)]
pub struct ShardName {
    /// Input location (checkpoint key).
    pub location: String,
    /// Output stem.
    pub stem: String,
}

/// Everything open for one in-flight shard.
struct OpenShard {
    data: DataWriter,
    rejects: Option<RejectsWriter>,
    dead: Vec<DeadLetter>,
    delta: Vec<Fingerprint>,
    record: ShardRecord,
    received: u64,
    expected: Option<u64>,
    status: ShardStatus,
}

/// Result of a sink run.
#[derive(Debug, Default, Clone, Serialize)]
pub struct SinkReport {
    /// Shards committed in this run, in commit order.
    pub committed: Vec<String>,
    /// Shards whose source failed (not checkpointed; retry with `--resume`).
    pub failed: Vec<(String, String)>,
    /// Shards still incomplete when input ended (only on abnormal shutdown).
    pub incomplete: Vec<String>,
    /// Thread time spent writing, in nanoseconds.
    pub busy_ns: u64,
}

/// The sink stage.
pub struct Sink {
    layout: OutputLayout,
    cfg: SinkConfig,
    names: Arc<Vec<ShardName>>,
    bands: usize,
    open: HashMap<ShardId, OpenShard>,
    checkpoint: Checkpoint,
    crash_after_commits: Option<usize>,
    report: SinkReport,
}

fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
}

impl Sink {
    /// Create a sink writing under `layout` and updating `checkpoint`.
    pub fn new(
        layout: OutputLayout,
        cfg: SinkConfig,
        names: Arc<Vec<ShardName>>,
        bands: usize,
        checkpoint: Checkpoint,
        crash_after_commits: Option<usize>,
    ) -> Self {
        Self {
            layout,
            cfg,
            names,
            bands,
            open: HashMap::new(),
            checkpoint,
            crash_after_commits,
            report: SinkReport::default(),
        }
    }

    fn name(&self, id: ShardId) -> &ShardName {
        &self.names[id as usize]
    }

    fn open_shard(&self, id: ShardId) -> Result<OpenShard> {
        let stem = &self.name(id).stem;
        let data = DataWriter::create(tmp_path(&self.layout.data(stem)), &self.cfg)?;
        let rejects = if self.cfg.write_rejects {
            Some(RejectsWriter::create(
                tmp_path(&self.layout.rejects(stem)),
                &self.cfg,
            )?)
        } else {
            None
        };
        Ok(OpenShard {
            data,
            rejects,
            dead: Vec::new(),
            delta: Vec::new(),
            record: ShardRecord {
                stem: stem.clone(),
                ..Default::default()
            },
            received: 0,
            expected: None,
            status: ShardStatus::Complete,
        })
    }

    /// Consume messages until every sender is gone.
    ///
    /// On error the function returns early and drops `rx`; every upstream
    /// `send` then fails, which is how a sink failure (disk full) shuts the
    /// whole pipeline down instead of deadlocking it on a full queue.
    pub fn run(mut self, rx: Receiver<SinkMsg>) -> Result<(SinkReport, Checkpoint)> {
        let kept_ctr = metrics::counter!(m::STAGE_DOCS, "stage" => "sink");
        let busy_ctr = metrics::counter!(m::STAGE_BUSY_NS, "stage" => "sink");
        for msg in rx {
            let t0 = Instant::now();
            let id = msg.shard();
            if !self.open.contains_key(&id) {
                let st = self.open_shard(id)?;
                self.open.insert(id, st);
            }
            let source = self.names[id as usize].stem.clone();
            let Some(st) = self.open.get_mut(&id) else {
                continue;
            };
            match msg {
                SinkMsg::Keep(doc) => {
                    st.data.push(&doc, &source)?;
                    st.record.kept += 1;
                    st.record.docs_in += 1;
                    st.record.pii_redactions += u64::from(doc.pii_redactions);
                    st.delta.push(doc.fingerprint);
                    st.received += 1;
                    kept_ctr.increment(1);
                }
                SinkMsg::Drop(d) => {
                    if let Some(r) = st.rejects.as_mut() {
                        r.push(&d)?;
                    }
                    *st.record.drops.entry(d.rule.to_string()).or_default() += 1;
                    st.record.docs_in += 1;
                    st.received += 1;
                }
                SinkMsg::DeadLetter(d) => {
                    st.dead.push(d);
                    st.record.dead_letters += 1;
                    st.received += 1;
                }
                SinkMsg::ShardEnd { total, status, .. } => {
                    st.expected = Some(total);
                    st.status = status;
                }
            }
            if st.expected == Some(st.received) {
                if let Some(st) = self.open.remove(&id) {
                    self.finalize(id, st)?;
                }
            }
            let ns = t0.elapsed().as_nanos() as u64;
            self.report.busy_ns += ns;
            busy_ctr.increment(ns);
        }
        // All senders are gone. Anything still open never got its ShardEnd or
        // never received all its records: an upstream stage died. Discard.
        let leftover: Vec<ShardId> = self.open.keys().copied().collect();
        for id in leftover {
            if let Some(st) = self.open.remove(&id) {
                self.report.incomplete.push(self.name(id).location.clone());
                discard(st);
            }
        }
        Ok((self.report, self.checkpoint))
    }

    fn finalize(&mut self, id: ShardId, mut st: OpenShard) -> Result<()> {
        let name = self.name(id).clone();
        match std::mem::replace(&mut st.status, ShardStatus::Complete) {
            ShardStatus::SourceFailed(e) => {
                tracing::warn!(shard = %name.location, error = %e, "source failed; shard discarded (retry with --resume)");
                self.report.failed.push((name.location, e));
                discard(st);
                return Ok(());
            }
            ShardStatus::Corrupt(e) => {
                tracing::warn!(shard = %name.location, error = %e, "corrupt input; committing records read before the error");
                st.record.status = "corrupt".into();
                st.record.error = Some(e);
            }
            ShardStatus::Complete => st.record.status = "complete".into(),
        }
        self.commit(&name, st)
    }

    /// The commit protocol (see `checkpoint` module docs for why this order).
    fn commit(&mut self, name: &ShardName, st: OpenShard) -> Result<()> {
        let OpenShard {
            data,
            rejects,
            dead,
            delta,
            mut record,
            ..
        } = st;
        let stem = &name.stem;

        let (tmp, rows) = data.finish()?;
        commit_file(&tmp, &self.layout.data(stem))?;
        debug_assert_eq!(rows, record.kept);
        if let Some(r) = rejects {
            let (tmp, _) = r.finish()?;
            commit_file(&tmp, &self.layout.rejects(stem))?;
        }
        write_dead_letters(&self.layout.dead_letter(stem), &name.location, &dead)?;
        write_dedup_delta(&self.layout.dedup_delta(stem), self.bands, &delta)?;

        record.committed_at_unix = now_unix();
        self.checkpoint.shards.insert(name.location.clone(), record);
        self.checkpoint.save(&self.layout.checkpoint())?; // ← commit point
        self.report.committed.push(name.location.clone());
        metrics::counter!(m::SHARDS_COMMITTED).increment(1);
        metrics::counter!(m::ROWS_WRITTEN).increment(rows);
        tracing::info!(shard = %name.location, rows, "shard committed");

        if let Some(n) = self.crash_after_commits {
            if self.report.committed.len() >= n {
                // Simulates kill -9: no destructors, no flushing of other
                // shards' writers, temp files left on disk.
                tracing::error!("debug.crash_after_commits reached; aborting");
                std::process::abort();
            }
        }
        Ok(())
    }
}

/// Best-effort removal of an uncommitted shard's temp files.
fn discard(st: OpenShard) {
    if let Ok((tmp, _)) = st.data.finish() {
        let _ = fs::remove_file(tmp);
    }
    if let Some(r) = st.rejects {
        if let Ok((tmp, _)) = r.finish() {
            let _ = fs::remove_file(tmp);
        }
    }
}

#[derive(Serialize)]
struct DeadLetterLine<'a> {
    shard: &'a str,
    record_id: Option<&'a str>,
    url: Option<&'a str>,
    reason: &'a str,
}

fn write_dead_letters(path: &Path, shard: &str, dead: &[DeadLetter]) -> Result<()> {
    let tmp = tmp_path(path);
    {
        let mut f = std::io::BufWriter::new(fs::File::create(&tmp).map_err(io_err(&tmp))?);
        for d in dead {
            let line = DeadLetterLine {
                shard,
                record_id: d.record_id.as_deref(),
                url: d.url.as_deref(),
                reason: &d.reason,
            };
            serde_json::to_writer(&mut f, &line)?;
            f.write_all(b"\n").map_err(io_err(&tmp))?;
        }
        let f = f.into_inner().map_err(|e| io_err(&tmp)(e.into_error()))?;
        f.sync_all().map_err(io_err(&tmp))?;
    }
    commit_file(&tmp, path)
}

/// Sum per-shard records into run totals.
pub fn totals(ck: &Checkpoint) -> Totals {
    let mut t = Totals::default();
    for r in ck.shards.values() {
        t.shards += 1;
        t.docs_in += r.docs_in;
        t.kept += r.kept;
        t.dead_letters += r.dead_letters;
        t.pii_redactions += r.pii_redactions;
        for (k, v) in &r.drops {
            *t.drops.entry(k.clone()).or_default() += v;
        }
    }
    t.exact_dups = t.drops.get("dedup.exact").copied().unwrap_or(0);
    t.near_dups = t.drops.get("dedup.near").copied().unwrap_or(0);
    let dedup_in = t.kept + t.exact_dups + t.near_dups;
    t.dup_ratio = if dedup_in == 0 {
        0.0
    } else {
        (t.exact_dups + t.near_dups) as f64 / dedup_in as f64
    };
    t
}

/// Aggregate counts over committed shards.
#[derive(Debug, Default, Clone, Serialize)]
pub struct Totals {
    /// Committed shards.
    pub shards: u64,
    /// Documents entering the pipeline.
    pub docs_in: u64,
    /// Documents written.
    pub kept: u64,
    /// Malformed records.
    pub dead_letters: u64,
    /// Exact duplicates removed.
    pub exact_dups: u64,
    /// Near duplicates removed.
    pub near_dups: u64,
    /// (exact + near) / documents that reached dedup.
    pub dup_ratio: f64,
    /// PII spans redacted.
    pub pii_redactions: u64,
    /// Drops by rule.
    pub drops: BTreeMap<String, u64>,
}
