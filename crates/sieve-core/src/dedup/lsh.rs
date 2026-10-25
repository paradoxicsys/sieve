//! Locality-sensitive hashing by banding.
//!
//! A k = b·r signature is cut into `b` bands of `r` rows. Two documents become
//! candidates if *any* band is identical. If their Jaccard similarity is `s`,
//! a band matches with probability `s^r`, so
//!
//! ```text
//! P(candidate) = 1 − (1 − s^r)^b
//! ```
//!
//! an S-shaped curve whose steepest point is near `s* ≈ (1/b)^(1/r)`. With the
//! default b = 16, r = 8: s* ≈ 0.71; P = 0.06 at s = 0.5, 0.61 at s = 0.7,
//! 0.95 at s = 0.8, 0.9999 at s = 0.9 (see `s_curve` tests and DECISIONS.md).

use xxhash_rust::xxh3::xxh3_64_with_seed;

/// Hash each band of a signature to one u64. The band index is the hash seed,
/// so equal row values in *different* bands never produce the same key.
pub fn band_keys(sig: &[u64], bands: usize, rows: usize) -> Vec<u64> {
    let mut buf = Vec::with_capacity(rows * 8);
    (0..bands)
        .map(|b| {
            buf.clear();
            for v in &sig[b * rows..(b + 1) * rows] {
                buf.extend_from_slice(&v.to_le_bytes());
            }
            xxh3_64_with_seed(&buf, b as u64)
        })
        .collect()
}

/// Probability that a pair with Jaccard `s` becomes an LSH candidate.
pub fn candidate_probability(s: f64, bands: usize, rows: usize) -> f64 {
    1.0 - (1.0 - s.powi(rows as i32)).powi(bands as i32)
}

/// Approximate similarity threshold (the S-curve's inflection point).
pub fn threshold(bands: usize, rows: usize) -> f64 {
    (1.0 / bands as f64).powf(1.0 / rows as f64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn s_curve_for_default_16x8() {
        let p = |s| candidate_probability(s, 16, 8);
        assert!(p(0.3) < 0.002);
        assert!((p(0.5) - 0.0606).abs() < 0.01, "{}", p(0.5));
        assert!(p(0.8) > 0.94);
        assert!(p(0.9) > 0.999);
        assert!((threshold(16, 8) - 0.707).abs() < 0.01);
    }

    #[test]
    fn identical_signatures_share_all_bands_and_bands_differ() {
        let sig: Vec<u64> = (0..128).map(|i| i % 3).collect();
        let a = band_keys(&sig, 16, 8);
        assert_eq!(a, band_keys(&sig, 16, 8));
        // rows repeat across bands, but seeds make the keys distinct
        let distinct: std::collections::HashSet<_> = a.iter().collect();
        assert_eq!(distinct.len(), 16);
    }
}
