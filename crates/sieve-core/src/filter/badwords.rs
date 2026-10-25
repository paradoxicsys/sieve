//! Bad-word filter (C4 style), using the LDNOOBW English list that C4 used
//! unless another list is configured.
//!
//! Matching happens on the dedup-normalised word stream (lower-case, punctuation
//! removed), so "F.O.O" and "foo!" are treated like "f o o" and "foo". List
//! entries are normalised the same way; entries that normalise to nothing (an
//! emoji) or contain a 1-character token ("s&m" → "s m", which would match
//! ordinary text) are skipped and counted in `skipped_entries`.

use std::collections::HashSet;

use super::{Filter, FilterDoc, Rejection};
use crate::config::BadWordsConfig;
use crate::error::{io_err, Result};
use crate::text::normalize_for_dedup;

/// The bundled list (CC-BY-4.0, github.com/LDNOOBW).
const BUNDLED_EN: &str = include_str!("../../resources/badwords_en.txt");

/// Drops documents with more than `max_matches` bad-word occurrences.
pub struct BadWordsFilter {
    phrases: HashSet<String>,
    /// Every proper word-prefix of a multi-word phrase. Lets the matcher stop
    /// extending a candidate as soon as it cannot become a phrase, so the common
    /// case is a single hash lookup per word.
    prefixes: HashSet<String>,
    max_words: usize,
    max_matches: usize,
    /// List entries ignored during normalisation (see module docs).
    pub skipped_entries: usize,
}

impl BadWordsFilter {
    /// Build from config, reading the list file if one is configured.
    pub fn new(cfg: &BadWordsConfig) -> Result<Self> {
        let list = match &cfg.path {
            Some(p) => std::fs::read_to_string(p).map_err(io_err(p))?,
            None => BUNDLED_EN.to_string(),
        };
        Ok(Self::from_list(&list, cfg.max_matches))
    }

    /// Build from list text (one entry per line).
    pub fn from_list(list: &str, max_matches: usize) -> Self {
        let mut phrases = HashSet::new();
        let mut prefixes = HashSet::new();
        let mut skipped = 0;
        let mut max_words = 1;
        for entry in list.lines().map(str::trim).filter(|l| !l.is_empty()) {
            let norm = normalize_for_dedup(entry);
            let words: Vec<&str> = norm.split(' ').collect();
            if norm.is_empty() || words.iter().any(|w| w.chars().count() < 2) {
                skipped += 1;
                continue;
            }
            for k in 1..words.len() {
                prefixes.insert(words[..k].join(" "));
            }
            max_words = max_words.max(words.len());
            phrases.insert(norm);
        }
        Self {
            phrases,
            prefixes,
            max_words,
            max_matches,
            skipped_entries: skipped,
        }
    }

    /// Count phrase occurrences in already-normalised text.
    pub fn count_matches(&self, normalized: &str) -> usize {
        let words: Vec<&str> = normalized.split(' ').filter(|w| !w.is_empty()).collect();
        let mut buf = String::new();
        let mut n = 0;
        for i in 0..words.len() {
            buf.clear();
            for (k, w) in words[i..].iter().take(self.max_words).enumerate() {
                if k > 0 {
                    buf.push(' ');
                }
                buf.push_str(w);
                if self.phrases.contains(buf.as_str()) {
                    n += 1;
                    break; // count each starting position once
                }
                if !self.prefixes.contains(buf.as_str()) {
                    break;
                }
            }
        }
        n
    }
}

impl Filter for BadWordsFilter {
    fn name(&self) -> &'static str {
        "bad_words"
    }
    fn rules(&self) -> &'static [&'static str] {
        &["bad_words"]
    }
    fn apply(&self, doc: &mut FilterDoc) -> Result<(), Rejection> {
        let n = self.count_matches(&normalize_for_dedup(&doc.text));
        if n > self.max_matches {
            return Err(Rejection::new("bad_words", n as f64));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matches_words_and_phrases_after_normalisation() {
        let f = BadWordsFilter::from_list("zork\nblue moon\ns&m\n🖕\n", 0);
        assert_eq!(f.skipped_entries, 2);
        assert_eq!(
            f.count_matches(&normalize_for_dedup("A ZORK! once in a Blue-Moon.")),
            2
        );
        assert_eq!(
            f.count_matches(&normalize_for_dedup("blue sky, zorkish moon")),
            0
        );
    }

    #[test]
    fn bundled_list_loads() {
        let f = BadWordsFilter::new(&BadWordsConfig::default()).unwrap();
        assert!(f.phrases.len() > 350, "{}", f.phrases.len());
        let mut d = FilterDoc {
            text: "A perfectly clean sentence about gardening.".into(),
            ..Default::default()
        };
        f.apply(&mut d).unwrap();
    }
}
