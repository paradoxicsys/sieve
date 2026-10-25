//! `sieve` command-line driver.
//!
//! Thin by design: argument parsing, logging, the Prometheus exporter, and a
//! human-readable summary. All pipeline behaviour lives in `sieve-core` so it
//! is testable without spawning processes.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use clap::{Parser, Subcommand};
use sieve_core::dedup::lsh::{candidate_probability, threshold};
use sieve_core::{Config, RunOptions, RunReport};

#[derive(Parser)]
#[command(
    name = "sieve",
    version,
    about = "Clean and deduplicate Common Crawl WARC/WET data into Parquet"
)]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Run the pipeline.
    Run(RunArgs),
    /// Print the default configuration as TOML.
    PrintConfig,
    /// Print the LSH candidate-probability S-curve for a bands × rows choice.
    SCurve {
        /// Number of bands.
        #[arg(long, default_value_t = 16)]
        bands: usize,
        /// Rows per band.
        #[arg(long, default_value_t = 8)]
        rows: usize,
    },
}

#[derive(clap::Args)]
struct RunArgs {
    /// Input files, directories, or URLs (file://, s3://, http(s)://; trailing `/` = prefix).
    #[arg(short, long, num_args = 1..)]
    input: Vec<String>,
    /// File with one input per line (e.g. Common Crawl's wet.paths).
    #[arg(long)]
    input_list: Option<PathBuf>,
    /// Prefix joined to every relative line of --input-list (e.g. https://data.commoncrawl.org/).
    #[arg(long)]
    base_url: Option<String>,
    /// Output directory.
    #[arg(short, long)]
    output: PathBuf,
    /// TOML config; unspecified keys use defaults.
    #[arg(short, long)]
    config: Option<PathBuf>,
    /// Continue a previous run in --output from its checkpoint.
    #[arg(long)]
    resume: bool,
    /// Process at most N pending shards.
    #[arg(long)]
    max_shards: Option<usize>,
    /// Override pipeline.process_threads.
    #[arg(long)]
    threads: Option<usize>,
    /// Serve Prometheus metrics on this address (e.g. 0.0.0.0:9898).
    #[arg(long)]
    metrics_addr: Option<SocketAddr>,
    /// Keep the metrics endpoint up this many seconds after finishing, so the
    /// final values get scraped.
    #[arg(long, default_value_t = 0)]
    linger_secs: u64,
    /// Print the full JSON report to stdout.
    #[arg(long)]
    json: bool,
    /// Fault injection for tests: abort after N shard commits.
    #[arg(long, hide = true)]
    crash_after_commits: Option<usize>,
}

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with_writer(std::io::stderr)
        .init();

    match Cli::parse().cmd {
        Cmd::PrintConfig => {
            print!("{}", toml_string(&Config::default())?);
            Ok(())
        }
        Cmd::SCurve { bands, rows } => {
            println!(
                "bands={bands} rows={rows} perms={} threshold≈{:.3}",
                bands * rows,
                threshold(bands, rows)
            );
            println!("{:>8} {:>12}", "jaccard", "P(candidate)");
            for i in 1..=19 {
                let s = i as f64 * 0.05;
                println!("{s:>8.2} {:>12.4}", candidate_probability(s, bands, rows));
            }
            Ok(())
        }
        Cmd::Run(args) => run(args),
    }
}

fn toml_string(cfg: &Config) -> Result<String> {
    // Config serialises cleanly; `toml` needs a table at the root, which it is.
    Ok(sieve_core::config::to_toml(cfg)?)
}

fn run(args: RunArgs) -> Result<()> {
    let mut cfg = match &args.config {
        Some(p) => Config::from_file(p).with_context(|| format!("loading {}", p.display()))?,
        None => Config::default(),
    };
    if let Some(t) = args.threads {
        cfg.pipeline.process_threads = t;
    }
    cfg.debug.crash_after_commits = args.crash_after_commits;

    let mut inputs = args.input.clone();
    if let Some(list) = &args.input_list {
        let text =
            std::fs::read_to_string(list).with_context(|| format!("reading {}", list.display()))?;
        for line in text
            .lines()
            .map(str::trim)
            .filter(|l| !l.is_empty() && !l.starts_with('#'))
        {
            let full = match &args.base_url {
                Some(base) if !line.contains("://") => {
                    format!(
                        "{}/{}",
                        base.trim_end_matches('/'),
                        line.trim_start_matches('/')
                    )
                }
                _ => line.to_string(),
            };
            inputs.push(full);
        }
    }
    if inputs.is_empty() {
        bail!("no inputs: pass --input and/or --input-list");
    }

    if let Some(addr) = args.metrics_addr {
        metrics_exporter_prometheus::PrometheusBuilder::new()
            .with_http_listener(addr)
            .install()
            .context("starting Prometheus exporter")?;
        tracing::info!(%addr, "serving Prometheus metrics at /metrics");
    }

    let opts = RunOptions {
        inputs,
        output: args.output.clone(),
        resume: args.resume,
        max_shards: args.max_shards,
    };
    let report = sieve_core::run(cfg, opts)?;
    print_summary(&report, &args.output);
    if args.json {
        println!("{}", serde_json::to_string_pretty(&report)?);
    }
    if args.linger_secs > 0 && args.metrics_addr.is_some() {
        tracing::info!(
            secs = args.linger_secs,
            "lingering so Prometheus can scrape final values"
        );
        std::thread::sleep(Duration::from_secs(args.linger_secs));
    }
    if !report.sink.failed.is_empty() || !report.sink.incomplete.is_empty() {
        bail!(
            "{} shard(s) failed and {} were incomplete; rerun with --resume to retry them",
            report.sink.failed.len(),
            report.sink.incomplete.len()
        );
    }
    Ok(())
}

fn print_summary(r: &RunReport, out: &std::path::Path) {
    let t = &r.totals;
    eprintln!();
    eprintln!("── sieve run ─────────────────────────────────────────────");
    eprintln!(
        "shards: {} found, {} already committed, {} committed now, {} failed",
        r.shards_total,
        r.shards_already_committed,
        r.sink.committed.len(),
        r.sink.failed.len()
    );
    eprintln!(
        "this run: {} docs in {:.1}s = {:.0} docs/s ({:.1} M docs/day)",
        r.run_docs_in,
        r.wall_seconds,
        r.run_docs_per_sec,
        r.run_docs_per_sec * 86_400.0 / 1e6
    );
    eprintln!("stages (items, items/s, cores busy):");
    for (name, s) in &r.stages {
        eprintln!(
            "  {name:<10} {:>9} {:>10.0} {:>6.2}",
            s.items, s.items_per_sec, s.utilization_cores
        );
    }
    eprintln!("queue max depth: {:?}", r.queue_max_depth);
    eprintln!(
        "all committed shards: {} docs in, {} kept ({:.1}%), {} dead letters",
        t.docs_in,
        t.kept,
        100.0 * t.kept as f64 / t.docs_in.max(1) as f64,
        t.dead_letters
    );
    eprintln!(
        "dedup: {} exact + {} near = {:.2}% of docs reaching dedup; index holds {} docs",
        t.exact_dups,
        t.near_dups,
        100.0 * t.dup_ratio,
        r.dedup_index_docs
    );
    eprintln!("pii redactions: {}", t.pii_redactions);
    eprintln!(
        "drops by rule (filter order: {}):",
        r.filter_order.join(" → ")
    );
    let mut drops: Vec<_> = t.drops.iter().collect();
    drops.sort_by(|a, b| b.1.cmp(a.1));
    for (rule, n) in drops {
        eprintln!(
            "  {rule:<36} {n:>9} {:>6.2}%",
            100.0 * *n as f64 / t.docs_in.max(1) as f64
        );
    }
    eprintln!("manifest: {}", out.join("_manifest.json").display());
}
