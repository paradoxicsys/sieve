//! MinHash signatures, from scratch.
//!
//! For a set S and a random permutation π of the hash universe,
//! `P[min π(S) == min π(T)] = |S ∩ T| / |S ∪ T|` (Jaccard similarity). With
//! `k` independent permutations, the fraction of signature positions that agree
//! is an unbiased estimate of Jaccard with standard error `sqrt(J(1-J)/k)`
//! (≈ 0.044 at J = 0.5, k = 128).
//!
//! True random permutations of a 2^64 universe are impossible to store, so each
//! "permutation" is a universal hash `h_i(x) = (a_i·x + b_i) mod p` with
//! `p = 2^61 − 1` (a Mersenne prime, so `mod p` is two shifts and an add rather
//! than a division). This is the same family `datasketch` uses.

use xxhash_rust::xxh3::xxh3_64;

/// The Mersenne prime 2^61 − 1.
pub const MERSENNE_61: u64 = (1 << 61) - 1;

/// SplitMix64: a tiny, well-mixed PRNG. Used only to derive the permutation
/// coefficients from a seed, so signatures are reproducible across runs and
/// across a crash/resume.
pub fn splitmix64(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// `x mod (2^61 − 1)` for any `x < 2^122 + 2^61`, without division:
/// 2^61 ≡ 1 (mod p), so the high bits can simply be added to the low bits.
#[inline(always)]
fn mod_mersenne(x: u128) -> u64 {
    let p = MERSENNE_61 as u128;
    let folded = (x & p) + (x >> 61); // < 2^62 + 2^61
    let folded = (folded & p) + (folded >> 61); // < 2^61 + 2
    let r = folded as u64;
    if r >= MERSENNE_61 {
        r - MERSENNE_61
    } else {
        r
    }
}

/// Computes MinHash signatures over word shingles.
#[derive(Debug, Clone)]
pub struct MinHasher {
    a: Vec<u64>,
    b: Vec<u64>,
    shingle_size: usize,
}

impl MinHasher {
    /// `num_perm` hash functions over `shingle_size`-word shingles.
    pub fn new(num_perm: usize, shingle_size: usize, seed: u64) -> Self {
        let mut st = seed;
        let mut a = Vec::with_capacity(num_perm);
        let mut b = Vec::with_capacity(num_perm);
        for _ in 0..num_perm {
            // a must be non-zero, otherwise h_i is constant.
            a.push(splitmix64(&mut st) % (MERSENNE_61 - 1) + 1);
            b.push(splitmix64(&mut st) % MERSENNE_61);
        }
        Self {
            a,
            b,
            shingle_size: shingle_size.max(1),
        }
    }

    /// Signature length.
    pub fn num_perm(&self) -> usize {
        self.a.len()
    }

    /// 64-bit hashes of every word `shingle_size`-gram of `normalized` (words
    /// separated by single spaces, as produced by
    /// [`crate::text::normalize_for_dedup`]). Documents shorter than one shingle
    /// yield a single shingle of all their words, so short near-identical docs
    /// can still match.
    pub fn shingles(&self, normalized: &str) -> Vec<u64> {
        let words: Vec<u64> = normalized
            .split(' ')
            .filter(|w| !w.is_empty())
            .map(|w| xxh3_64(w.as_bytes()))
            .collect();
        if words.is_empty() {
            return Vec::new();
        }
        let k = self.shingle_size.min(words.len());
        words
            .windows(k)
            .map(|win| {
                // Order-sensitive combine, finished with an avalanche so similar
                // shingles produce unrelated hashes.
                let mut h = 0x5851_F42D_4C95_7F2Du64;
                for &w in win {
                    h = (h ^ w).wrapping_mul(0x9E37_79B9_7F4A_7C15).rotate_left(31);
                }
                let mut s = h;
                splitmix64(&mut s)
            })
            .collect()
    }

    /// Signature of a shingle set: per permutation, the minimum hash value.
    /// An empty set yields all `u64::MAX` (it matches only other empty sets).
    pub fn signature_of_shingles(&self, shingles: &[u64]) -> Vec<u64> {
        let mut sig = vec![u64::MAX; self.a.len()];
        for &s in shingles {
            let x = (s % MERSENNE_61) as u128;
            // Inner loop over permutations: a, b and sig are contiguous arrays
            // that stay in L1 for the whole document.
            for ((m, &a), &b) in sig.iter_mut().zip(&self.a).zip(&self.b) {
                let v = mod_mersenne(a as u128 * x + b as u128);
                if v < *m {
                    *m = v;
                }
            }
        }
        sig
    }

    /// Signature of normalised text.
    pub fn signature(&self, normalized: &str) -> Vec<u64> {
        self.signature_of_shingles(&self.shingles(normalized))
    }
}

/// Fraction of positions where two signatures agree: the Jaccard estimate.
pub fn estimate_jaccard(a: &[u64], b: &[u64]) -> f64 {
    if a.is_empty() {
        return 0.0;
    }
    let same = a.iter().zip(b).filter(|(x, y)| x == y).count();
    same as f64 / a.len() as f64
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use std::collections::HashSet;

    #[test]
    fn mersenne_reduction_matches_naive() {
        let p = MERSENNE_61 as u128;
        for x in [
            0u128,
            1,
            p - 1,
            p,
            p + 1,
            u64::MAX as u128,
            (p - 1) * (p - 1) + (p - 1),
        ] {
            assert_eq!(mod_mersenne(x) as u128, x % p, "x = {x}");
        }
    }

    #[test]
    fn short_docs_get_one_shingle() {
        let m = MinHasher::new(16, 5, 1);
        assert_eq!(m.shingles("a b c").len(), 1);
        assert_eq!(m.shingles("a b c d e f").len(), 2);
        assert!(m.shingles("").is_empty());
    }

    fn jaccard(a: &HashSet<u64>, b: &HashSet<u64>) -> f64 {
        let inter = a.intersection(b).count();
        let union = a.union(b).count();
        if union == 0 {
            1.0
        } else {
            inter as f64 / union as f64
        }
    }

    proptest! {
        // Identical inputs must give identical signatures: estimate exactly 1.0.
        #[test]
        fn identical_sets_estimate_one(words in prop::collection::vec("[a-z]{1,8}", 1..200)) {
            let m = MinHasher::new(128, 5, 42);
            let text = words.join(" ");
            let s1 = m.signature(&text);
            let s2 = m.signature(&text);
            prop_assert_eq!(estimate_jaccard(&s1, &s2), 1.0);
        }

        // Disjoint shingle sets: each permutation is a bijection on [0, p), so
        // two disjoint sets can only agree through a 64-bit hash collision.
        #[test]
        fn disjoint_sets_estimate_near_zero(
            a in prop::collection::hash_set(any::<u64>(), 1..300),
            b in prop::collection::hash_set(any::<u64>(), 1..300),
        ) {
            let b: Vec<u64> = b.difference(&a).copied().collect();
            prop_assume!(!b.is_empty());
            let a: Vec<u64> = a.into_iter().collect();
            let m = MinHasher::new(128, 5, 7);
            let est = estimate_jaccard(&m.signature_of_shingles(&a), &m.signature_of_shingles(&b));
            prop_assert!(est <= 0.02, "estimate {}", est);
        }

        // The estimate tracks true Jaccard. Std. error at k=128 is <= 0.045, so
        // 0.25 is a > 5-sigma bound: a failure means a real bug, not bad luck.
        #[test]
        fn estimate_tracks_true_jaccard(
            common in prop::collection::hash_set(any::<u64>(), 0..200),
            only_a in prop::collection::hash_set(any::<u64>(), 0..200),
            only_b in prop::collection::hash_set(any::<u64>(), 0..200),
        ) {
            let a: HashSet<u64> = common.union(&only_a).copied().collect();
            let b: HashSet<u64> = common.union(&only_b).copied().collect();
            prop_assume!(!a.is_empty() && !b.is_empty());
            let m = MinHasher::new(128, 5, 99);
            let av: Vec<u64> = a.iter().copied().collect();
            let bv: Vec<u64> = b.iter().copied().collect();
            let est = estimate_jaccard(&m.signature_of_shingles(&av), &m.signature_of_shingles(&bv));
            let truth = jaccard(&a, &b);
            prop_assert!((est - truth).abs() <= 0.25, "est {} truth {}", est, truth);
        }

        // Shingle order inside the set must not matter (it is a set).
        #[test]
        fn signature_is_order_independent(mut v in prop::collection::vec(any::<u64>(), 1..100)) {
            let m = MinHasher::new(32, 5, 3);
            let s1 = m.signature_of_shingles(&v);
            v.reverse();
            prop_assert_eq!(s1, m.signature_of_shingles(&v));
        }
    }
}
