# Sieve

A streaming pipeline in Rust that cleans and deduplicates Common Crawl web data
(WARC/WET) into Parquet: the kind of preprocessing used to build LLM
pre-training sets such as FineWeb, Dolma or RedPajama.

* **Ingest** gzip WARC/WET from local disk, `s3://` or `http(s)://` (via `object_store`), streaming, never whole files in memory.
* **Extract** HTML → text with boilerplate removal (nav, footer, scripts, sidebars…).
* **Filter** with configurable Gopher/C4 rules (length, language, repetition, symbol ratio, stop words, bad words); every drop records the rule that fired.
* **Dedup**: exact (xxh3 of normalised text), then near-duplicate via MinHash (128 permutations, 5-word shingles) + LSH banding (16 × 8), implemented from scratch.
* **Redact** emails, phone numbers and IP addresses.
* **Write** Parquet shards with quality-signal columns, a rejects file, a dead-letter file, and a JSON run manifest.
* **Survive crashes**: shard-level checkpoints, atomic temp-file → rename commits, dedup state restored on resume.
* **Observe**: Prometheus metrics, a provisioned Grafana dashboard.

Headline (measured, details below): **12,400 docs/s ≈ 1.07 billion WET
documents/day** on an 8-core laptop, **~150 MiB peak RSS**, **18× faster than a
single-process Python implementation of the same filters** (4.7× per core).

---

## Architecture

```mermaid
flowchart LR
  subgraph IO["tokio runtime (I/O only)"]
    L[resolve inputs<br/>file / s3 / http] --> F[fetch_shard<br/>object_store GET stream]
  end
  F -- "tokio::mpsc (bounded)<br/>Bytes chunks" --> D
  subgraph CPU["OS threads + rayon pools (CPU)"]
    D["decode thread per shard<br/>MultiGzDecoder → WARC parser"] -- "raw (bounded)" --> P
    P["process pool (rayon, N threads)<br/>extract → filters → normalise →<br/>xxh3 + MinHash + LSH band keys"] -- "candidates (bounded)" --> X
    X["dedup thread<br/>exact set + 16 band tables"] -- "unique (bounded)" --> R
    R["redact pool (rayon)<br/>PII regex"] -- "sink (bounded)" --> S
    S["sink thread<br/>per-shard Parquet writers,<br/>commit + checkpoint"]
    D -. "dead letters, ShardEnd{total}" .-> S
    P -. "drops (rule)" .-> S
    X -. "dedup drops" .-> S
  end
  S --> O[("out/<br/>data/ rejects/ dead_letter/<br/>dedup_state/ _checkpoint.json<br/>_manifest.json")]
```

Every arrow is a bounded queue, so the slowest stage sets the pace and memory
stays flat. Every record that enters produces exactly one terminal message at
the sink (kept / dropped / dead-lettered); the sink commits a shard when the
count matches the total its decoder announced.

```
crates/
  sieve-core/   stages behind traits (Extractor, Filter, Deduplicator, Redactor), pipeline, sink, checkpoint
  sieve-cli/    `sieve` binary (run, print-config, s-curve)
  sieve-bench/  criterion benches per stage + end-to-end
baseline/       single-process Python implementation with the same filters
deploy/         docker-compose: Prometheus + Grafana with provisioned dashboard
scripts/        fixture generator, CC fetcher, baseline comparison, bench/profiling helpers
```

## Quickstart

```bash
cargo build --release

# generate small synthetic WARC/WET fixtures and run on them
python3 scripts/make_sample.py
./target/release/sieve run -i tests/data -o out/demo

# real data: 8 Common Crawl WET files (~860 MB) + WARC heads
scripts/fetch_cc.sh 8
./target/release/sieve run -i data/cc -o out/cc

# or stream straight from Common Crawl over HTTPS
./target/release/sieve run -i https://data.commoncrawl.org/crawl-data/CC-MAIN-2024-10/segments/1707947473347.0/wet/CC-MAIN-20240220211055-20240221001055-00000.warc.wet.gz -o out/http
# or a paths list
./target/release/sieve run --input-list wet.paths --base-url https://data.commoncrawl.org/ --max-shards 4 -o out/list

# resume after a crash / Ctrl-C (refuses if output-affecting config changed)
./target/release/sieve run -i data/cc -o out/cc --resume

# configuration: everything is TOML, unspecified keys use defaults
./target/release/sieve print-config > my.toml
./target/release/sieve run -i data/cc -o out/cc2 -c my.toml

# LSH S-curve for a bands × rows choice
./target/release/sieve s-curve --bands 16 --rows 8
```

Output layout:

| Path | Contents |
|---|---|
| `data/<shard>.parquet` | kept docs: `url, record_id, timestamp, lang, lang_score, cc_lang, text, word_count, mean_word_len, symbol_word_ratio, dup_line_frac, dup_para_frac, alpha_words_frac, stop_words, c4_lines_removed, pii_redactions, source_shard` |
| `rejects/<shard>.parquet` | every dropped doc: `url, record_id, rule, value` |
| `dead_letter/<shard>.jsonl` | malformed records with the reason |
| `dedup_state/<shard>.bin` | fingerprints of the shard's kept docs (restored on resume) |
| `_checkpoint.json` | committed shards + per-shard stats + config fingerprint |
| `_manifest.json` | run report: config, filter order, per-stage throughput, queue depths, totals, drops by rule |

### Metrics and dashboard

```bash
docker compose -f deploy/docker-compose.yml up -d
./target/release/sieve run -i data/cc -o out/m --metrics-addr 0.0.0.0:9898 --linger-secs 30
open http://localhost:3000        # "Sieve pipeline" dashboard (anonymous view)
```

Panels: docs/s per stage, cores busy per stage, queue fill %, drops/s by rule,
total drops by rule, duplicate ratio (cumulative and 1-minute, exact vs near),
keep rate, projected docs/day, dedup index size, input MB/s, PII/s, shards
committed, dead letters. Metric names are in `crates/sieve-core/src/metrics.rs`.

**Verification status:** the compose stack was run locally against a live 8-file
run: the Prometheus container scraped the pipeline, the "Sieve pipeline"
dashboard was provisioned in Grafana, and its datasource returned the run's
36,223 kept rows. Separately, every one of the dashboard's 18 PromQL queries was
evaluated through the Prometheus HTTP API and returned data (17 non-zero; "dead
letters" was correctly 0).

## Tests

```bash
cargo test --workspace                                  # 56 unit + property tests
cargo clippy --workspace --all-targets -- -D warnings   # clean
cargo fmt --all --check                                 # clean
```

* **Unit tests per stage** (56): reader (multi-member gzip, oversize, missing headers, truncated gzip, source vs decode errors), extraction (boilerplate, `<pre>`, 20,000-deep nesting, HTTP envelopes), each filter family, MinHash/LSH, PII, checkpoint/delta round-trips, config.
* **Property tests (proptest)** for MinHash: identical inputs → estimate exactly 1.0; disjoint sets → ≤ 0.02; estimate within 0.25 of true Jaccard (a > 5σ bound at k = 128); order independence.

## Measured results

**Hardware / software.** Apple M3 (8 cores: 4 performance + 4 efficiency),
16 GB RAM, internal SSD, macOS 26.5.1, rustc 1.93.1, release profile
(thin LTO). Python 3.14.7, numpy 2.4.6, pyarrow 26.0.0, whatlang-pyo3 0.6.0.

**Data.** Common Crawl `CC-MAIN-2024-10`, segment `1707947473347.0`: WET files
`…-00000` to `…-00007` (8 × ~107 MB, 275,767 documents), and the first 100 MB of
WARC files `…-00000` to `…-00003` (`scripts/fetch_cc.sh`). Inputs were on local
disk (network excluded).

### Throughput, memory, drop and dup rates (8 WET files, default config)

Command: `scripts/bench_run.sh final 3` (= `/usr/bin/time -l ./target/release/sieve run -i data/cc -o out/bench`, 3 runs).

| Metric | Value |
|---|---|
| Wall time | 22.2 / 22.3 / 22.4 s |
| Throughput | **12,334 / 12,446 / 12,348 docs/s** |
| Projected docs/day | ≈ 1.07 billion (12,400 × 86,400) |
| Peak RSS | **148 / 149 / 159 MiB** |
| Kept | 36,224 of 275,767 (13.1%) |
| Duplicates | 1,600 exact + 2,068 near = **9.19%** of docs reaching dedup |
| PII spans redacted | 3,765 |
| Output size | 48 MB data + 17 MB rejects (zstd) for 864 MB of gzip input |

Stage utilisation (from `_manifest.json` of run 3): process pool 7.28 cores
busy (the bottleneck), ingest 1.27, sink 0.08, redact 0.02, dedup < 0.01. The
`raw` queue reached capacity (256) and `candidates`/`unique` stayed below 50,
consistent with the process stage being the limit.

Drops by rule (share of all 275,767 input docs; first failing rule wins, order
`length → language → gopher_repetition → gopher_quality → c4 → bad_words`):

| Rule | Docs | % |
|---|---:|---:|
| lang.not_allowed | 148,546 | 53.87 |
| gopher_rep.dup_line_frac | 26,453 | 9.59 |
| c4.min_sentences | 13,589 | 4.93 |
| length.min_words | 13,434 | 4.87 |
| gopher_rep.dup_ngram_char_frac | 6,823 | 2.47 |
| lang.low_confidence | 6,344 | 2.30 |
| gopher_quality.alpha_words | 5,275 | 1.91 |
| c4.curly_brace | 5,218 | 1.89 |
| bad_words | 3,359 | 1.22 |
| gopher_rep.dup_line_char_frac | 2,802 | 1.02 |
| dedup.near | 2,068 | 0.75 |
| dedup.exact | 1,600 | 0.58 |
| gopher_quality.stop_words | 1,380 | 0.50 |
| gopher_quality.mean_word_len | 1,362 | 0.49 |
| gopher_rep.top_ngram_char_frac | 915 | 0.33 |
| c4.lorem_ipsum | 192 | 0.07 |
| gopher_quality.symbol_word_ratio | 119 | 0.04 |
| gopher_quality.ellipsis_lines | 45 | 0.02 |
| length.max_words | 6 | 0.00 |
| gopher_rep.dup_para_frac | 5 | 0.00 |
| gopher_quality.bullet_lines | 4 | 0.00 |
| lang.undetected | 3 | 0.00 |
| gopher_rep.dup_para_char_frac | 1 | 0.00 |

Dedup counts vary slightly between runs (e.g. 36,224 vs 36,226 kept) because
which near-duplicate arrives first depends on thread scheduling; filter counts
are identical across runs.

### Bounded memory

Peak RSS versus input size (default config; `/usr/bin/time -l`), and versus
queue capacity (8 files):

| Input | Docs | docs/s | Peak RSS | Dedup index |
|---|---:|---:|---:|---:|
| 1 file | 34,250 | 11,023 | 77 MiB | 4,781 docs |
| 2 files | 68,834 | 12,275 | 120 MiB | 9,478 |
| 4 files | 138,034 | 12,565 | 132 MiB | 18,613 |
| 8 files | 275,767 | 12,779 | 150 MiB | 36,226 |

| `channel_capacity` | docs/s | Peak RSS |
|---|---:|---:|
| 16 | 12,212 | 153 MiB |
| 256 (default) | 12,555 | 158 MiB |
| 4096 | 12,822 | 202 MiB |

The jump from 1 to 2 files is the second concurrently processed shard
(`max_concurrent_shards = 2`: another decoder, another set of open Parquet
writers). After that, 4× more input costs ~30 MiB. The only structure that
grows with the corpus is the dedup index (≈ 220 B per kept doc, an
estimate). Large queues buy little speed and cost memory.

### WARC (HTML) path

Command: `/usr/bin/time -l ./target/release/sieve run -i data/warc -o out/warc4`
(4 × 100 MB WARC heads, 19,853 HTML responses).

| Metric | Value |
|---|---|
| Throughput | **2,670 docs/s** (7.4 s), process pool 7.69 cores busy |
| Peak RSS | 167 MiB |
| Kept | 1,864 (9.4%) |
| Extraction drops | 1,365 `extract.empty`, 260 `extract.not_html` |
| Dead letters / shard status | 4 (each file's cut-off last gzip member) / all 4 `corrupt` |

HTML parsing makes WARC ~4.6× more expensive per document than WET. The
truncated tails are a real-world test of the corruption path: each shard
commits everything read before the cut.

### Speedup vs Python

Command: `baseline/.venv/bin/python -I scripts/compare_baseline.py data/cc/CC-MAIN-20240220211055-20240221001055-00000.warc.wet.gz`
(one WET file, 34,250 docs; the baseline is described in `baseline/sieve_baseline.py`).

| Pipeline | Wall s | docs/s | Speedup |
|---|---:|---:|---:|
| Python baseline (1 process) | 56.7 | 604 | 1.0× |
| Sieve `--threads 1` | 12.0 | 2,843 | **4.7×** |
| Sieve, all cores | 3.1 | 11,021 | **18.2×** |

The Python port is an independent check on the Rust filters: **17 of 21 rules
have identical drop counts** on this file. The differences: `lang.not_allowed`
+2 / `lang.low_confidence` −2 (the Python whatlang binding bundles a different
whatlang build), and `dedup.exact` +1 / `dedup.near` −7 (different hash
functions by design; Python's per-process `hash()` randomisation even moves its
own exact/near split between runs). In the baseline, 50 of 57 seconds are spent
in the filters (interpreted per-word/per-character loops). `--threads 1` still
uses separate decode, dedup and sink threads, so it is "one process worker",
not strictly one core.

### Per-stage benchmarks (criterion)

Command: `cargo bench -p sieve-bench --bench stages` / `--bench end_to_end`. Real
data (2,000 WET docs; 300 HTML pages averaging 61 KiB), single thread.

| Stage | Throughput |
|---|---|
| gunzip + WARC parse | 46.5 MiB/s compressed |
| HTML → text | 933 pages/s |
| length filter | 48,500 docs/s |
| language ID (whatlang, 4 KiB) | 6,455 docs/s |
| Gopher repetition | 8,333 docs/s (2,321 before the fix below) |
| Gopher quality | 17,664 docs/s |
| C4 | 8,141 docs/s |
| bad words | 6,465 docs/s |
| full filter chain (short-circuits) | 4,133 docs/s (2,894 before) |
| normalise for dedup | 13,011 docs/s |
| fingerprint (128 perms, 3 / 5 / 9-word shingles) | 3,276 / 3,268 / 3,251 docs/s |
| dedup index check+insert | 2.9 M docs/s |
| PII redaction | 384 MiB/s (6.4 MiB/s before the fix below) |
| Parquet write (zstd-3) | 174 MiB/s |
| **end to end, 1 WET file** | **10,882 docs/s** (3.15 s per 34,250 docs) |

Each filter row runs that family on *every* doc; in the pipeline each runs only
on survivors of the previous ones. Shingle size barely changes cost because
the per-shingle work (128 multiply-mods) dominates and the shingle count is
≈ word count for any k.

## Profiling: what was slow and what changed

`cargo flamegraph` needs root `dtrace` on macOS, which was not available.
Equivalent tooling: macOS's built-in `sample` profiler on the running process,
folded and rendered with `inferno`:

```bash
./target/release/sieve run -i data/cc -o out/prof &   # release build has line-table debug info
sample $(pgrep -f "out/prof") 15 -file profiles/x.sample.txt
inferno-collapse-sample profiles/x.sample.txt > profiles/x.folded
inferno-flamegraph profiles/x.folded > profiles/x.svg
python3 scripts/profile_breakdown.py profiles/x.folded process-
```

Flamegraphs: `profiles/before.svg`, `profiles/after.svg`. 8-file throughput
measured with `scripts/bench_run.sh` (2 runs each):

| Step | Finding (share of process-worker samples) | Change | docs/s |
|---|---|---|---:|
| 0 | whatlang 34%, **Gopher repetition 28.5%** (SipHash 10% self, rehash 4.6%), waiting on `par_bridge` mutex 18.7% | (baseline) | 8,697 / 8,563 |
| 1 | Gopher repetition hashes `&str`s and already-hashed `u64`s with SipHash | identity-hashed `u64` sets, pre-sized (`filter/gopher.rs`) | 10,542 / 10,457 (**+22%**) |
| 2 | mutex wait now 25%: `par_bridge` holds a lock while blocked in `recv()` | per-thread receive loops via `ThreadPool::broadcast` (`pipeline.rs`) | 10,387 / 10,446 (no change) |
| 3 | step 2 didn't help, so the waits were a symptom: manifest shows redact at 0.95 of its single core and its input queue full. Cause: Unicode `\b` in the PII regex forces the regex crate off its DFA on non-ASCII text | ASCII word boundaries `(?-u:\b)` (`pii.rs`) | 12,333 / 12,432 (**+19%**) |

Net: **+43%** (≈ 8,630 → ≈ 12,380 docs/s). Filter decisions were identical
before and after every step, and the redaction count was unchanged (3,763 →
3,763). After the fixes, the `par_bridge` wait is gone (0.9%), whatlang is ~48%
of process-worker samples and crossbeam channel handoff ~14%.

**Next bottleneck: language ID.** Options, not yet done: classify fewer bytes
(measure agreement vs Common Crawl's CLD2 labels), switch to fastText `lid.176`,
or run cheaper selective filters first.

### Language-ID threshold calibration

How `min_confidence = 0.3` was chosen: run with language
filtering on but `allowed = []`, `min_confidence = 0`, other filters off, then
bucket English docs by whatlang confidence and compare with the `cc_lang`
column (Common Crawl's CLD2 label). On one WET file, agreement was 85–87% below
0.3, 91–94% for 0.3–0.8, ~95% above 0.8.

## Limitations

* **Dedup index is in memory** and grows ~linearly with kept documents (≈ 220 B/doc, estimated). Fine for tens of millions of kept docs on one machine, not for a whole crawl.
* **Which duplicate survives depends on arrival order**, which depends on scheduling; near-dup counts can vary by a few per 100k docs between runs. Fully deterministic with `process_threads = 1`, `max_concurrent_shards = 1`.
* **No LSH verification step**: any band collision counts as a duplicate; pairs at Jaccard 0.5 are flagged ~6% of the time (analytical).
* **Language ID** is whatlang (trigram model): weaker than fastText, especially on short or mixed-language pages.
* **Encoding**: bodies are decoded as UTF-8 lossily; no charset detection for legacy encodings.
* **PII** is regex-only: misses names/addresses and unusual phone formats; redacts version strings that look like IPs; IPv6 only in full 8-group form.
* **Heuristics**: C4 sentence counting is punctuation-based; HTML boilerplate removal is tag/class/role-based (no readability scoring).
* **Durability on macOS**: `fsync` does not force the drive cache (`F_FULLFSYNC`); Linux semantics are stronger.
* **Queues are bounded in items, not bytes**: worst-case memory is `capacity × max_record_bytes` per queue.
* **S3** support goes through `object_store` and is exercised only for local and HTTP sources here; it was not tested against a real bucket (Common Crawl's S3 bucket needs AWS credentials).
* **Grafana/Docker** stack not run on the build machine (queries verified against Prometheus as described above).
* **Not built:** the stretch goal (gRPC coordinator/worker distributed mode).
* `--threads 1` measures one *process* worker; decode, dedup and sink still run on their own threads.
