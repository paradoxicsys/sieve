//! Gopher heuristics (Rae et al. 2021, "Scaling Language Models", appendix A),
//! following the reference semantics of `datatrove`'s implementation, which
//! FineWeb used. All character counts are Unicode scalar values (`chars()`),
//! and all "char fraction" denominators are the character length of the text.

use std::collections::HashSet;

use xxhash_rust::xxh3::xxh3_64;

use super::{ratio, Filter, FilterDoc, Rejection};
use crate::config::{GopherQualityConfig, GopherRepetitionConfig};
use crate::dedup::{U64Map, U64Set};
use crate::text::{lines, paragraphs};

/// Repetition filters: duplicated lines/paragraphs and repeated n-grams.
/// Targets boilerplate-heavy and spammy pages that repeat the same content.
pub struct GopherRepetition {
    cfg: GopherRepetitionConfig,
}

impl GopherRepetition {
    /// Build from config.
    pub fn new(cfg: &GopherRepetitionConfig) -> Self {
        Self { cfg: cfg.clone() }
    }
}

/// (fraction of elements that repeat an earlier element, chars inside them).
///
/// Elements are compared by their xxh3 hash in an identity-hashed set: the
/// profiler showed SipHash over `&str` keys plus rehashing as ~15% of process
/// worker time in this filter (see README "Profiling").
fn duplicate_stats<'a>(items: impl Iterator<Item = &'a str>) -> (f64, usize) {
    let mut seen = U64Set::default();
    let (mut total, mut dups, mut dup_chars) = (0usize, 0usize, 0usize);
    for it in items {
        total += 1;
        if !seen.insert(xxh3_64(it.as_bytes())) {
            dups += 1;
            dup_chars += it.chars().count();
        }
    }
    (ratio(dups, total), dup_chars)
}

/// Order-sensitive combination of word hashes into an n-gram hash. Hashing
/// n-grams (instead of building `String` keys) avoids an allocation per
/// n-gram; at 64 bits, collisions are negligible for a single document.
fn gram_hash(words: &[u64]) -> u64 {
    words.iter().fold(0x243F_6A88_85A3_08D3u64, |h, &w| {
        (h ^ w).wrapping_mul(0x9E37_79B9_7F4A_7C15).rotate_left(29)
    })
}

/// Characters covered by the single most frequent n-gram (n-gram measured as
/// its words joined by single spaces), times its count. Ties go to the n-gram
/// seen first, matching Python's `Counter.most_common`.
fn top_ngram_chars(hashes: &[u64], lens: &[usize], n: usize) -> usize {
    if hashes.len() < n {
        return 0;
    }
    // hash -> (count, first index, char length)
    let mut counts: U64Map<(usize, usize, usize)> = U64Map::default();
    counts.reserve(hashes.len() - n + 1);
    for i in 0..=hashes.len() - n {
        let h = gram_hash(&hashes[i..i + n]);
        counts
            .entry(h)
            .or_insert_with(|| (0, i, lens[i..i + n].iter().sum::<usize>() + n - 1))
            .0 += 1;
    }
    counts
        .values()
        .max_by(|a, b| a.0.cmp(&b.0).then(b.1.cmp(&a.1)))
        .map_or(0, |&(count, _, len)| count * len)
}

/// Characters inside n-grams that repeat an earlier n-gram, scanning left to
/// right and jumping past each repeat so overlapping repeats are not counted
/// twice (datatrove's `find_all_duplicate`). N-gram length here is the sum of
/// word lengths without spaces.
fn dup_ngram_chars(hashes: &[u64], lens: &[usize], n: usize) -> usize {
    let mut seen = U64Set::default();
    seen.reserve(hashes.len());
    let (mut i, mut repeated) = (0usize, 0usize);
    while i + n <= hashes.len() {
        let h = gram_hash(&hashes[i..i + n]);
        if seen.contains(&h) {
            repeated += lens[i..i + n].iter().sum::<usize>();
            i += n;
        } else {
            seen.insert(h);
            i += 1;
        }
    }
    repeated
}

impl Filter for GopherRepetition {
    fn name(&self) -> &'static str {
        "gopher_repetition"
    }
    fn rules(&self) -> &'static [&'static str] {
        &[
            "gopher_rep.dup_para_frac",
            "gopher_rep.dup_para_char_frac",
            "gopher_rep.dup_line_frac",
            "gopher_rep.dup_line_char_frac",
            "gopher_rep.top_ngram_char_frac",
            "gopher_rep.dup_ngram_char_frac",
        ]
    }

    fn apply(&self, doc: &mut FilterDoc) -> Result<(), Rejection> {
        let c = &self.cfg;
        let text = doc.text.as_str();
        let text_chars = text.chars().count();

        let (para_frac, para_chars) = duplicate_stats(paragraphs(text));
        doc.signals.dup_para_frac = para_frac as f32;
        if para_frac > c.dup_para_frac {
            return Err(Rejection::new("gopher_rep.dup_para_frac", para_frac));
        }
        let v = ratio(para_chars, text_chars);
        if v > c.dup_para_char_frac {
            return Err(Rejection::new("gopher_rep.dup_para_char_frac", v));
        }

        let (line_frac, line_chars) = duplicate_stats(lines(text));
        doc.signals.dup_line_frac = line_frac as f32;
        if line_frac > c.dup_line_frac {
            return Err(Rejection::new("gopher_rep.dup_line_frac", line_frac));
        }
        let v = ratio(line_chars, text_chars);
        if v > c.dup_line_char_frac {
            return Err(Rejection::new("gopher_rep.dup_line_char_frac", v));
        }

        let words: Vec<&str> = text.split_whitespace().collect();
        let hashes: Vec<u64> = words.iter().map(|w| xxh3_64(w.as_bytes())).collect();
        let lens: Vec<usize> = words.iter().map(|w| w.chars().count()).collect();

        for &(n, max) in &c.top_ngram_char_frac {
            let v = ratio(top_ngram_chars(&hashes, &lens, n), text_chars);
            if v > max {
                return Err(Rejection::new("gopher_rep.top_ngram_char_frac", v));
            }
        }
        for &(n, max) in &c.dup_ngram_char_frac {
            let v = ratio(dup_ngram_chars(&hashes, &lens, n), text_chars);
            if v > max {
                return Err(Rejection::new("gopher_rep.dup_ngram_char_frac", v));
            }
        }
        Ok(())
    }
}

/// Quality heuristics: word shape, symbols, bullets, stop words. Targets
/// non-prose: tables of numbers, keyword lists, menus, code, gibberish.
pub struct GopherQuality {
    cfg: GopherQualityConfig,
    stop_words: HashSet<String>,
    max_stop_len: usize,
}

impl GopherQuality {
    /// Build from config.
    pub fn new(cfg: &GopherQualityConfig) -> Self {
        let stop_words: HashSet<String> = cfg.stop_words.iter().map(|w| w.to_lowercase()).collect();
        let max_stop_len = stop_words.iter().map(String::len).max().unwrap_or(0);
        Self {
            cfg: cfg.clone(),
            stop_words,
            max_stop_len,
        }
    }
}

const BULLETS: &[char] = &['•', '-', '*'];

impl Filter for GopherQuality {
    fn name(&self) -> &'static str {
        "gopher_quality"
    }
    fn rules(&self) -> &'static [&'static str] {
        &[
            "gopher_quality.no_words",
            "gopher_quality.mean_word_len",
            "gopher_quality.symbol_word_ratio",
            "gopher_quality.bullet_lines",
            "gopher_quality.ellipsis_lines",
            "gopher_quality.alpha_words",
            "gopher_quality.stop_words",
        ]
    }

    fn apply(&self, doc: &mut FilterDoc) -> Result<(), Rejection> {
        let c = &self.cfg;
        let text = doc.text.as_str();
        let s = &mut doc.signals;

        let mut n_words = 0usize;
        let mut word_chars = 0usize;
        let mut alpha_words = 0usize;
        let mut stops: HashSet<&str> = HashSet::new();
        let mut lower = String::new();
        for w in text.split_whitespace() {
            n_words += 1;
            word_chars += w.chars().count();
            if w.chars().any(char::is_alphabetic) {
                alpha_words += 1;
            }
            // Only short words can be stop words; skip the lowercase work otherwise.
            if w.len() <= self.max_stop_len {
                lower.clear();
                lower.extend(w.chars().flat_map(char::to_lowercase));
                if let Some(sw) = self.stop_words.get(lower.as_str()) {
                    stops.insert(sw.as_str());
                }
            }
        }
        if n_words == 0 {
            return Err(Rejection::new("gopher_quality.no_words", 0.0));
        }

        let mean = word_chars as f64 / n_words as f64;
        s.mean_word_len = mean as f32;
        if mean < c.min_mean_word_len || mean > c.max_mean_word_len {
            return Err(Rejection::new("gopher_quality.mean_word_len", mean));
        }

        let hashes = text.matches('#').count();
        let ellipses = text.matches("...").count() + text.matches('…').count();
        let sym = ratio(hashes.max(ellipses), n_words);
        s.symbol_word_ratio = sym as f32;
        if sym > c.max_symbol_word_ratio {
            return Err(Rejection::new("gopher_quality.symbol_word_ratio", sym));
        }

        let (mut n_lines, mut bullets, mut ellipsis_lines) = (0usize, 0usize, 0usize);
        for l in lines(text) {
            n_lines += 1;
            if l.starts_with(BULLETS) {
                bullets += 1;
            }
            if l.ends_with("...") || l.ends_with('…') {
                ellipsis_lines += 1;
            }
        }
        let v = ratio(bullets, n_lines);
        if v > c.max_bullet_lines_frac {
            return Err(Rejection::new("gopher_quality.bullet_lines", v));
        }
        let v = ratio(ellipsis_lines, n_lines);
        if v > c.max_ellipsis_lines_frac {
            return Err(Rejection::new("gopher_quality.ellipsis_lines", v));
        }

        let alpha = ratio(alpha_words, n_words);
        s.alpha_words_frac = alpha as f32;
        if alpha < c.min_alpha_words_frac {
            return Err(Rejection::new("gopher_quality.alpha_words", alpha));
        }

        s.stop_words = stops.len() as u32;
        if stops.len() < c.min_stop_words {
            return Err(Rejection::new(
                "gopher_quality.stop_words",
                stops.len() as f64,
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rep(text: &str) -> Result<(), Rejection> {
        let f = GopherRepetition::new(&GopherRepetitionConfig::default());
        f.apply(&mut FilterDoc {
            text: text.into(),
            ..Default::default()
        })
    }

    fn qual(text: &str) -> Result<(), Rejection> {
        let f = GopherQuality::new(&GopherQualityConfig::default());
        f.apply(&mut FilterDoc {
            text: text.into(),
            ..Default::default()
        })
    }

    const PROSE: &str = "The committee met on Tuesday to discuss the new budget. \
        Members agreed that funding for the library should be increased, and that \
        the park renovation would have to wait until next year. A final vote is \
        expected with broad support from residents of the town.";

    #[test]
    fn prose_passes_both() {
        rep(PROSE).unwrap();
        qual(PROSE).unwrap();
    }

    #[test]
    fn repeated_lines_are_rejected() {
        let text = "Buy cheap watches now\n".repeat(10) + PROSE;
        let r = rep(&text).unwrap_err();
        assert!(r.rule.starts_with("gopher_rep."), "{r:?}");
    }

    #[test]
    fn repeated_ngrams_are_rejected() {
        let text = "click here for the best deals ".repeat(30);
        assert!(rep(&text).is_err());
    }

    #[test]
    fn dup_ngram_matches_reference_semantics() {
        // words: a b a b a b ; n=2 → grams ab ba ab(dup, jump 2) ab(dup, jump 2) → 2+2 = 4 chars
        let ws = ["a", "b", "a", "b", "a", "b"];
        let h: Vec<u64> = ws.iter().map(|w| xxh3_64(w.as_bytes())).collect();
        let l = vec![1; 6];
        assert_eq!(dup_ngram_chars(&h, &l, 2), 4);
        // top 2-gram "a b" occurs 3 times, length 3 (with the space)
        assert_eq!(top_ngram_chars(&h, &l, 2), 9);
    }

    #[test]
    fn no_stop_words_is_rejected() {
        let text = "Apple banana cherry grape lemon mango orange peach plum melon kiwi";
        assert_eq!(qual(text).unwrap_err().rule, "gopher_quality.stop_words");
    }

    #[test]
    fn symbol_heavy_is_rejected() {
        let text = format!("{PROSE} {}", "#tag ".repeat(20));
        assert_eq!(
            qual(&text).unwrap_err().rule,
            "gopher_quality.symbol_word_ratio"
        );
    }

    #[test]
    fn bullet_lists_are_rejected() {
        let text = "- the remarkable item number one\n".repeat(20);
        assert_eq!(qual(&text).unwrap_err().rule, "gopher_quality.bullet_lines");
    }

    #[test]
    fn numeric_tables_fail_alpha_words() {
        let text = "the and 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18";
        let r = qual(text).unwrap_err();
        assert!(matches!(
            r.rule,
            "gopher_quality.alpha_words" | "gopher_quality.mean_word_len"
        ));
    }
}
