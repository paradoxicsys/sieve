//! Shared corpus loading for the criterion benchmarks.
//!
//! Benchmarks prefer real Common Crawl data (`data/cc/*.wet.gz` and the WARC
//! slice downloaded by `scripts/fetch_cc.sh`) because filter and dedup costs
//! depend heavily on real document lengths and languages. When that data is
//! absent they fall back to the committed fixtures so `cargo bench` still runs
//! on a fresh clone; numbers from fixtures are *not* representative.

use std::path::{Path, PathBuf};

use sieve_core::doc::{RawRecord, RecordKind};
use sieve_core::ingest::decode_shard;

/// Workspace root.
pub fn root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

/// A real WET file if available (`SIEVE_BENCH_WET` overrides).
pub fn wet_path() -> Option<PathBuf> {
    if let Ok(p) = std::env::var("SIEVE_BENCH_WET") {
        return Some(PathBuf::from(p));
    }
    let p = root().join("data/cc/CC-MAIN-20240220211055-20240221001055-00000.warc.wet.gz");
    p.exists().then_some(p)
}

/// A real (partial) WARC file with HTML responses, if available.
pub fn warc_path() -> Option<PathBuf> {
    if let Ok(p) = std::env::var("SIEVE_BENCH_WARC") {
        return Some(PathBuf::from(p));
    }
    let p = root().join("data/cc/warc-head-100MB.warc.gz.part");
    p.exists().then_some(p)
}

/// Decode up to `limit` records from a WARC/WET file.
pub fn load_records(path: &Path, limit: usize) -> Vec<RawRecord> {
    let (dtx, drx) = crossbeam_channel::unbounded();
    let (stx, _srx) = crossbeam_channel::unbounded();
    let file = match std::fs::File::open(path) {
        Ok(f) => f,
        Err(e) => panic!("open {}: {e}", path.display()),
    };
    // Decode on a helper thread and stop reading once we have enough.
    let h = std::thread::spawn(move || decode_shard(0, file, 4 << 20, &dtx, &stx));
    let out: Vec<RawRecord> = drx.iter().take(limit).collect();
    drop(drx); // makes the decoder's next send fail, so it stops early
    let _ = h.join();
    out
}

/// Real WET texts (or fixture texts), as the process stage would see them.
pub fn texts(limit: usize) -> (Vec<String>, &'static str) {
    let (path, label) = match wet_path() {
        Some(p) => (p, "real-wet"),
        None => (
            root().join("tests/data/shards/shard-00.warc.wet.gz"),
            "fixture",
        ),
    };
    let t = load_records(&path, limit)
        .into_iter()
        .filter(|r| r.kind == RecordKind::Conversion)
        .map(|r| String::from_utf8_lossy(&r.body).into_owned())
        .collect();
    (t, label)
}

/// HTML response records (or fixture responses).
pub fn html_records(limit: usize) -> (Vec<RawRecord>, &'static str) {
    let (path, label) = match warc_path() {
        Some(p) => (p, "real-warc"),
        None => (root().join("tests/data/sample.warc.gz"), "fixture"),
    };
    let r = load_records(&path, limit * 3)
        .into_iter()
        .filter(|r| r.kind == RecordKind::Response)
        .take(limit)
        .collect();
    (r, label)
}
