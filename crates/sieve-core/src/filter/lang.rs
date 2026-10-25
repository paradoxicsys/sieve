//! Language identification with `whatlang` (trigram-based, pure Rust).

use std::collections::HashSet;

use super::{Filter, FilterDoc, Rejection};
use crate::config::LanguageConfig;
use crate::text::truncate_at_char_boundary;

/// Keeps documents whose detected language is allowed with enough confidence.
pub struct LanguageFilter {
    allowed: HashSet<String>,
    min_confidence: f64,
    sample_bytes: usize,
}

impl LanguageFilter {
    /// Build from config.
    pub fn new(cfg: &LanguageConfig) -> Self {
        Self {
            allowed: cfg.allowed.iter().cloned().collect(),
            min_confidence: cfg.min_confidence,
            sample_bytes: cfg.sample_bytes,
        }
    }
}

impl Filter for LanguageFilter {
    fn name(&self) -> &'static str {
        "language"
    }
    fn rules(&self) -> &'static [&'static str] {
        &["lang.undetected", "lang.not_allowed", "lang.low_confidence"]
    }
    fn apply(&self, doc: &mut FilterDoc) -> Result<(), Rejection> {
        // Classify a prefix only: whatlang's cost is linear in input length,
        // and a few KB is plenty to identify a language.
        let sample = truncate_at_char_boundary(&doc.text, self.sample_bytes);
        let Some(info) = whatlang::detect(sample) else {
            return Err(Rejection::new("lang.undetected", f64::NAN));
        };
        let code = info.lang().code();
        doc.signals.lang = Some(code.to_string());
        doc.signals.lang_score = info.confidence() as f32;
        if !self.allowed.is_empty() && !self.allowed.contains(code) {
            return Err(Rejection::new("lang.not_allowed", info.confidence()));
        }
        if info.confidence() < self.min_confidence {
            return Err(Rejection::new("lang.low_confidence", info.confidence()));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run(text: &str) -> Result<FilterDoc, Rejection> {
        let f = LanguageFilter::new(&LanguageConfig::default());
        let mut d = FilterDoc {
            text: text.into(),
            ..Default::default()
        };
        f.apply(&mut d).map(|_| d)
    }

    #[test]
    fn english_kept_others_dropped() {
        let d = run(
            "The city council met on Tuesday evening to discuss the proposed budget for \
             the coming year. Several residents spoke in favour of more funding for the \
             public library, while others argued that road repairs should come first. \
             The council is expected to vote on the final plan next month.",
        )
        .unwrap();
        assert_eq!(d.signals.lang.as_deref(), Some("eng"));
        let r = run(
            "Der Stadtrat hat am Dienstagabend über den vorgeschlagenen Haushalt für das \
             kommende Jahr beraten. Mehrere Bürger sprachen sich für mehr Geld für die \
             öffentliche Bibliothek aus.",
        )
        .unwrap_err();
        assert_eq!(r.rule, "lang.not_allowed");
    }
}
