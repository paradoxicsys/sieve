//! Per-stage micro-benchmarks. Throughput is reported in documents (or bytes)
//! per second so stages can be compared with the pipeline's end-to-end rate.

#![allow(missing_docs, clippy::unwrap_used, clippy::expect_used)]

use std::hint::black_box;
use std::io::Read;

use criterion::{criterion_group, criterion_main, BatchSize, Criterion, Throughput};
use sieve_bench::{html_records, texts, wet_path};
use sieve_core::config::{Config, DedupConfig};
use sieve_core::dedup::{DedupIndex, Deduplicator, Fingerprinter};
use sieve_core::doc::{CleanDoc, DocMeta, QualitySignals};
use sieve_core::extract::{Extractor, HtmlExtractor};
use sieve_core::filter::{self, Filter, FilterChain, FilterDoc};
use sieve_core::pii::{Redactor, RegexRedactor};
use sieve_core::sink::parquet::DataWriter;
use sieve_core::text::normalize_for_dedup;

const N: usize = 2000;

fn bench_decode(c: &mut Criterion) {
    let Some(path) = wet_path() else { return };
    // First 16 MiB of compressed input, held in memory: measures gunzip + WARC
    // parsing without disk effects.
    let mut buf = Vec::new();
    std::fs::File::open(path)
        .unwrap()
        .take(16 << 20)
        .read_to_end(&mut buf)
        .unwrap();
    let mut g = c.benchmark_group("decode");
    g.throughput(Throughput::Bytes(buf.len() as u64));
    g.sample_size(10);
    g.bench_function("gunzip+warc (real-wet, 16MiB compressed)", |b| {
        b.iter(|| {
            let (dtx, drx) = crossbeam_channel::unbounded();
            let (stx, _srx) = crossbeam_channel::unbounded();
            let st = sieve_core::ingest::decode_shard(
                0,
                std::io::Cursor::new(&buf),
                4 << 20,
                &dtx,
                &stx,
            );
            drop(drx);
            black_box(st.docs)
        })
    });
    g.finish();
}

fn bench_extract(c: &mut Criterion) {
    let (recs, label) = html_records(300);
    let ex = HtmlExtractor::new(&Default::default());
    let bytes: usize = recs.iter().map(|r| r.body.len()).sum();
    let mut g = c.benchmark_group("extract");
    g.throughput(Throughput::Elements(recs.len() as u64));
    g.bench_function(
        format!(
            "html_to_text ({label}, {} docs, {} KiB avg)",
            recs.len(),
            bytes / recs.len().max(1) / 1024
        ),
        |b| {
            b.iter(|| {
                for r in &recs {
                    let _ = black_box(ex.extract(r));
                }
            })
        },
    );
    g.finish();
}

fn bench_filters(c: &mut Criterion) {
    let (docs, label) = texts(N);
    let cfg = Config::default().filters;
    let families: Vec<(&str, Box<dyn Filter>)> = vec![
        (
            "length",
            Box::new(filter::length::LengthFilter::new(&cfg.length)),
        ),
        (
            "language",
            Box::new(filter::lang::LanguageFilter::new(&cfg.language)),
        ),
        (
            "gopher_repetition",
            Box::new(filter::gopher::GopherRepetition::new(
                &cfg.gopher_repetition,
            )),
        ),
        (
            "gopher_quality",
            Box::new(filter::gopher::GopherQuality::new(&cfg.gopher_quality)),
        ),
        ("c4", Box::new(filter::c4::C4Filter::new(&cfg.c4))),
        (
            "bad_words",
            Box::new(filter::badwords::BadWordsFilter::new(&cfg.bad_words).unwrap()),
        ),
    ];
    let mut g = c.benchmark_group(format!(
        "filter ({label}, {} docs, each family on every doc)",
        docs.len()
    ));
    g.throughput(Throughput::Elements(docs.len() as u64));
    g.sample_size(20);
    for (name, f) in &families {
        g.bench_function(*name, |b| {
            b.iter_batched(
                || docs.clone(),
                |docs| {
                    for t in docs {
                        let mut d = FilterDoc {
                            text: t,
                            ..Default::default()
                        };
                        let _ = black_box(f.apply(&mut d));
                    }
                },
                BatchSize::LargeInput,
            )
        });
    }
    let chain = FilterChain::from_config(&cfg).unwrap();
    g.bench_function("full chain (short-circuits)", |b| {
        b.iter_batched(
            || docs.clone(),
            |docs| {
                for t in docs {
                    let mut d = FilterDoc {
                        text: t,
                        ..Default::default()
                    };
                    let _ = black_box(chain.apply(&mut d));
                }
            },
            BatchSize::LargeInput,
        )
    });
    g.finish();
}

fn bench_dedup(c: &mut Criterion) {
    let (docs, label) = texts(N);
    let mut g = c.benchmark_group(format!("dedup ({label}, {} docs)", docs.len()));
    g.throughput(Throughput::Elements(docs.len() as u64));
    g.sample_size(20);
    g.bench_function("normalize", |b| {
        b.iter(|| {
            docs.iter()
                .map(|d| normalize_for_dedup(d).len())
                .sum::<usize>()
        })
    });
    for shingle in [3usize, 5, 9] {
        let fp = Fingerprinter::new(&DedupConfig {
            shingle_size: shingle,
            ..Default::default()
        });
        g.bench_function(
            format!("fingerprint 128 perms, {shingle}-word shingles"),
            |b| {
                b.iter(|| {
                    docs.iter()
                        .map(|d| fp.fingerprint(d).exact)
                        .fold(0u64, |a, x| a ^ x)
                })
            },
        );
    }
    let fp = Fingerprinter::new(&DedupConfig::default());
    let fps: Vec<_> = docs.iter().map(|d| fp.fingerprint(d)).collect();
    g.bench_function("index check_and_insert (16 bands)", |b| {
        b.iter_batched(
            || DedupIndex::new(&DedupConfig::default()),
            |mut idx| {
                for f in &fps {
                    black_box(idx.check_and_insert(f));
                }
                idx.len()
            },
            BatchSize::SmallInput,
        )
    });
    g.finish();
}

fn bench_pii(c: &mut Criterion) {
    let (docs, label) = texts(N);
    let r = RegexRedactor::new(&Default::default()).unwrap();
    let bytes: usize = docs.iter().map(String::len).sum();
    let mut g = c.benchmark_group(format!("pii ({label})"));
    g.throughput(Throughput::Bytes(bytes as u64));
    g.sample_size(20);
    g.bench_function(format!("redact {} docs", docs.len()), |b| {
        b.iter(|| docs.iter().map(|d| r.redact(d).1.total()).sum::<u32>())
    });
    g.finish();
}

fn bench_parquet(c: &mut Criterion) {
    let (docs, label) = texts(N);
    let cfg = Config::default().sink;
    let clean: Vec<CleanDoc> = docs
        .into_iter()
        .enumerate()
        .map(|(i, text)| CleanDoc {
            meta: DocMeta {
                shard: 0,
                seq: i as u64,
                url: format!("http://example.com/{i}"),
                record_id: format!("<urn:uuid:{i}>"),
                timestamp_ms: 1_708_465_238_000,
                cc_lang: Some("eng".into()),
            },
            text,
            signals: QualitySignals {
                lang: Some("eng".into()),
                ..Default::default()
            },
            fingerprint: Default::default(),
            pii_redactions: 0,
        })
        .collect();
    let bytes: usize = clean.iter().map(|d| d.text.len()).sum();
    let mut g = c.benchmark_group(format!("sink ({label})"));
    g.throughput(Throughput::Bytes(bytes as u64));
    g.sample_size(10);
    g.bench_function(format!("parquet zstd-3 write {} docs", clean.len()), |b| {
        let dir = tempfile::tempdir().unwrap();
        b.iter(|| {
            let mut w = DataWriter::create(dir.path().join("x.parquet.tmp"), &cfg).unwrap();
            for d in &clean {
                w.push(d, "bench").unwrap();
            }
            w.finish().unwrap().1
        })
    });
    g.finish();
}

criterion_group!(
    benches,
    bench_decode,
    bench_extract,
    bench_filters,
    bench_dedup,
    bench_pii,
    bench_parquet
);
criterion_main!(benches);
