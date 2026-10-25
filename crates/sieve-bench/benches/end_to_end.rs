//! End-to-end throughput: the whole pipeline (threads, channels, Parquet,
//! commit) on one real WET file, or on the fixtures if no real data exists.

#![allow(missing_docs, clippy::unwrap_used, clippy::expect_used)]

use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use sieve_bench::{root, wet_path};
use sieve_core::{Config, RunOptions};

fn bench_e2e(c: &mut Criterion) {
    let (input, label) = match wet_path() {
        Some(p) => (p, "real-wet 1 file"),
        None => (root().join("tests/data"), "fixtures"),
    };
    // One untimed run to learn how many documents the input holds.
    let probe = tempfile::tempdir().unwrap();
    let opts = |out: std::path::PathBuf| RunOptions {
        inputs: vec![input.display().to_string()],
        output: out,
        resume: false,
        max_shards: None,
    };
    let docs = sieve_core::run(Config::default(), opts(probe.path().join("o")))
        .unwrap()
        .run_docs_in;

    let mut g = c.benchmark_group("end_to_end");
    g.throughput(Throughput::Elements(docs));
    g.sample_size(10);
    g.bench_function(
        format!("pipeline ({label}, {docs} docs, default config)"),
        |b| {
            b.iter_with_setup(tempfile::tempdir, |dir| {
                let d = dir.unwrap();
                sieve_core::run(Config::default(), opts(d.path().join("o")))
                    .unwrap()
                    .totals
                    .kept
            })
        },
    );
    g.finish();
}

criterion_group!(benches, bench_e2e);
criterion_main!(benches);
