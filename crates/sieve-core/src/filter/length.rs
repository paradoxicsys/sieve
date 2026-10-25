//! Word-count bounds. Runs first: it is the cheapest rule and removes the
//! large population of near-empty pages (error pages, redirects, link farms).

use super::{Filter, FilterDoc, Rejection};
use crate::config::LengthConfig;

/// Rejects documents outside `[min_words, max_words]`.
pub struct LengthFilter {
    min: usize,
    max: usize,
}

impl LengthFilter {
    /// Build from config.
    pub fn new(cfg: &LengthConfig) -> Self {
        Self {
            min: cfg.min_words,
            max: cfg.max_words,
        }
    }
}

impl Filter for LengthFilter {
    fn name(&self) -> &'static str {
        "length"
    }
    fn rules(&self) -> &'static [&'static str] {
        &["length.min_words", "length.max_words"]
    }
    fn apply(&self, doc: &mut FilterDoc) -> Result<(), Rejection> {
        let n = doc.text.split_whitespace().count();
        doc.signals.word_count = n as u32;
        if n < self.min {
            return Err(Rejection::new("length.min_words", n as f64));
        }
        if n > self.max {
            return Err(Rejection::new("length.max_words", n as f64));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bounds() {
        let f = LengthFilter::new(&LengthConfig {
            enabled: true,
            min_words: 2,
            max_words: 3,
        });
        let mut d = FilterDoc {
            text: "one".into(),
            ..Default::default()
        };
        assert_eq!(f.apply(&mut d).unwrap_err().rule, "length.min_words");
        let mut d = FilterDoc {
            text: "one two".into(),
            ..Default::default()
        };
        assert!(f.apply(&mut d).is_ok());
        let mut d = FilterDoc {
            text: "a b c d".into(),
            ..Default::default()
        };
        assert_eq!(f.apply(&mut d).unwrap_err().rule, "length.max_words");
    }
}
