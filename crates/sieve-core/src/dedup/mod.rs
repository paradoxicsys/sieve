//! Deduplication: exact (xxh3 of normalised text) then near (MinHash + LSH).
//!
//! Work is split across two stages on purpose:
//! * [`Fingerprinter`] (pure, `Sync`) runs in the parallel process stage and
//!   does all the hashing: normalise → xxh3 → shingles → 128 mins → band keys.
//! * [`DedupIndex`] (stateful, single owner) runs on one thread and only does
//!   hash-set lookups/inserts, which take well under a microsecond per document.
//!
//! The index is therefore not shared between threads at all: no locks, no
//! atomics, and no question of two threads racing to insert near-duplicates
//! that each think they saw first.

pub mod lsh;
pub mod minhash;

use std::collections::HashSet;
use std::hash::{BuildHasherDefault, Hasher};

use xxhash_rust::xxh3::xxh3_64;

use crate::config::DedupConfig;
use crate::doc::Fingerprint;
use crate::text::normalize_for_dedup;
use minhash::MinHasher;

/// Computes fingerprints. Immutable after construction, so one instance is
/// shared by reference across all process workers.
#[derive(Debug, Clone)]
pub struct Fingerprinter {
    hasher: Option<MinHasher>,
    bands: usize,
    rows: usize,
}

impl Fingerprinter {
    /// Build from config.
    pub fn new(cfg: &DedupConfig) -> Self {
        Self {
            hasher: cfg
                .near
                .then(|| MinHasher::new(cfg.num_perm, cfg.shingle_size, cfg.seed)),
            bands: cfg.bands,
            rows: cfg.rows_per_band,
        }
    }

    /// Fingerprint a document's text.
    pub fn fingerprint(&self, text: &str) -> Fingerprint {
        let norm = normalize_for_dedup(text);
        let exact = xxh3_64(norm.as_bytes());
        let bands = match &self.hasher {
            Some(h) => lsh::band_keys(&h.signature(&norm), self.bands, self.rows),
            None => Vec::new(),
        };
        Fingerprint { exact, bands }
    }
}

/// Outcome of checking a document against the index.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verdict {
    /// First time seen: kept, and now in the index.
    Unique,
    /// Same normalised text as a kept document.
    ExactDuplicate,
    /// Shares at least one LSH band with a kept document.
    NearDuplicate,
}

/// Stateful duplicate detector. A trait so the in-memory index could be
/// replaced by e.g. a Bloom-filter or on-disk implementation for corpora whose
/// index does not fit in RAM.
pub trait Deduplicator: Send {
    /// Check a fingerprint and, if unique, insert it ("first seen wins").
    fn check_and_insert(&mut self, fp: &Fingerprint) -> Verdict;
    /// Insert without checking (restoring state from committed shards).
    fn insert(&mut self, fp: &Fingerprint);
    /// Number of documents in the index.
    fn len(&self) -> usize;
    /// True when empty.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// A `Hasher` that passes a `u64` through unchanged.
///
/// Keys in the index are already xxh3 outputs (uniformly distributed), so
/// hashing them again with SipHash would only burn cycles: this roughly halves
/// lookup cost. Using it on non-random keys would be a mistake.
#[derive(Default, Clone, Copy)]
pub struct IdentityHasher(u64);

impl Hasher for IdentityHasher {
    fn finish(&self) -> u64 {
        self.0
    }
    fn write(&mut self, bytes: &[u8]) {
        // Only reached for non-u64 keys; fold bytes in so it is still correct.
        for &b in bytes {
            self.0 = self.0.rotate_left(8) ^ u64::from(b);
        }
    }
    fn write_u64(&mut self, n: u64) {
        self.0 = n;
    }
}

/// A set of already-random `u64` keys (hashes), skipping re-hashing.
pub type U64Set = HashSet<u64, BuildHasherDefault<IdentityHasher>>;
/// A map keyed by already-random `u64` keys (hashes).
pub type U64Map<V> = std::collections::HashMap<u64, V, BuildHasherDefault<IdentityHasher>>;

/// In-memory exact + LSH index.
///
/// Memory: one u64 per kept doc for exact, plus one u64 per band per kept doc
/// for LSH, plus hash-table overhead — about 17 × 8 B × ~1.6 ≈ 220 B per kept
/// document at the default 16 bands. That is the one structure in Sieve that
/// grows with the corpus rather than staying flat.
#[derive(Debug, Default)]
pub struct DedupIndex {
    exact_enabled: bool,
    exact: U64Set,
    bands: Vec<U64Set>,
    docs: usize,
}

impl DedupIndex {
    /// Empty index.
    pub fn new(cfg: &DedupConfig) -> Self {
        Self {
            exact_enabled: cfg.exact,
            exact: U64Set::default(),
            bands: if cfg.near {
                (0..cfg.bands).map(|_| U64Set::default()).collect()
            } else {
                Vec::new()
            },
            docs: 0,
        }
    }
}

impl Deduplicator for DedupIndex {
    fn check_and_insert(&mut self, fp: &Fingerprint) -> Verdict {
        if self.exact_enabled && self.exact.contains(&fp.exact) {
            return Verdict::ExactDuplicate;
        }
        // An exact duplicate also matches every band, so checking exact first
        // only changes the *label*, letting us report the two rates separately.
        let near = self
            .bands
            .iter()
            .zip(&fp.bands)
            .any(|(table, key)| table.contains(key));
        if near {
            return Verdict::NearDuplicate;
        }
        self.insert(fp);
        Verdict::Unique
    }

    fn insert(&mut self, fp: &Fingerprint) {
        if self.exact_enabled {
            self.exact.insert(fp.exact);
        }
        for (table, key) in self.bands.iter_mut().zip(&fp.bands) {
            table.insert(*key);
        }
        self.docs += 1;
    }

    fn len(&self) -> usize {
        self.docs
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const BASE: &str = "Sieve is a streaming pipeline that cleans and deduplicates web data \
        from Common Crawl into Parquet files. It filters documents by language, length, \
        repetition and quality, removes exact and near duplicate pages, and redacts \
        personal information such as email addresses and phone numbers before writing \
        the output shards to disk with a manifest describing the run.";

    fn setup() -> (Fingerprinter, DedupIndex) {
        let cfg = DedupConfig::default();
        (Fingerprinter::new(&cfg), DedupIndex::new(&cfg))
    }

    #[test]
    fn exact_duplicates_ignore_case_and_punctuation() {
        let (f, mut idx) = setup();
        assert_eq!(idx.check_and_insert(&f.fingerprint(BASE)), Verdict::Unique);
        let shouted = BASE.to_uppercase().replace(',', " ;");
        assert_eq!(
            idx.check_and_insert(&f.fingerprint(&shouted)),
            Verdict::ExactDuplicate
        );
    }

    #[test]
    fn near_duplicate_with_small_edit_is_caught() {
        let (f, mut idx) = setup();
        idx.check_and_insert(&f.fingerprint(BASE));
        let edited = BASE.replace("before writing", "prior to writing");
        assert_eq!(
            idx.check_and_insert(&f.fingerprint(&edited)),
            Verdict::NearDuplicate
        );
    }

    #[test]
    fn unrelated_document_is_unique() {
        let (f, mut idx) = setup();
        idx.check_and_insert(&f.fingerprint(BASE));
        let other = "The recipe calls for two cups of flour, a pinch of salt, three eggs \
            and a generous amount of butter, mixed slowly until the dough is smooth and \
            then left to rest in a cool place for at least an hour before baking.";
        assert_eq!(idx.check_and_insert(&f.fingerprint(other)), Verdict::Unique);
        assert_eq!(idx.len(), 2);
    }

    #[test]
    fn disabled_near_dedup_has_no_bands() {
        let cfg = DedupConfig {
            near: false,
            ..Default::default()
        };
        assert!(Fingerprinter::new(&cfg).fingerprint(BASE).bands.is_empty());
    }
}
