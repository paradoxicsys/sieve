//! Quality filters.
//!
//! Each rule family implements [`Filter`]. The [`FilterChain`] runs them in a
//! fixed order (cheapest and most selective first) and stops at the first
//! rejection, so a document's drop reason is the *first* rule it failed. That
//! makes per-rule drop counts order-dependent: "gopher_quality.stop_words
//! dropped 12%" means 12% of docs that survived everything before it. The order
//! is documented in DECISIONS.md and reported in the run manifest.

pub mod badwords;
pub mod c4;
pub mod gopher;
pub mod lang;
pub mod length;

use crate::config::FiltersConfig;
use crate::doc::QualitySignals;
use crate::error::Result;

/// A document being filtered. Filters may *edit* `text` (the C4 line filter
/// removes lines) and record signals for the output columns.
#[derive(Debug, Default)]
pub struct FilterDoc {
    /// Current text.
    pub text: String,
    /// Signals accumulated so far.
    pub signals: QualitySignals,
}

/// A failed rule: which one, and the measured value that failed it.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Rejection {
    /// Stable dotted rule name, used as a metrics label and in the rejects file.
    pub rule: &'static str,
    /// Measured value (NaN if not meaningful).
    pub value: f64,
}

impl Rejection {
    /// Construct a rejection.
    pub fn new(rule: &'static str, value: f64) -> Self {
        Self { rule, value }
    }
}

/// One family of quality rules.
///
/// `Send + Sync` because a single chain is shared by reference across all
/// rayon workers; filters therefore hold only immutable state (thresholds,
/// word lists) and keep per-document scratch space on the stack.
pub trait Filter: Send + Sync {
    /// Family name, for logs.
    fn name(&self) -> &'static str;
    /// Every rule name this filter can emit, so metrics can be pre-registered
    /// and the dashboard shows zero-count rules rather than missing series.
    fn rules(&self) -> &'static [&'static str];
    /// Accept (possibly editing the doc) or reject.
    fn apply(&self, doc: &mut FilterDoc) -> Result<(), Rejection>;
}

/// The ordered list of enabled filters.
pub struct FilterChain {
    filters: Vec<Box<dyn Filter>>,
}

impl FilterChain {
    /// Build the chain from config, in the canonical order.
    pub fn from_config(cfg: &FiltersConfig) -> Result<Self> {
        let mut filters: Vec<Box<dyn Filter>> = Vec::new();
        if cfg.length.enabled {
            filters.push(Box::new(length::LengthFilter::new(&cfg.length)));
        }
        if cfg.language.enabled {
            filters.push(Box::new(lang::LanguageFilter::new(&cfg.language)));
        }
        if cfg.gopher_repetition.enabled {
            filters.push(Box::new(gopher::GopherRepetition::new(
                &cfg.gopher_repetition,
            )));
        }
        if cfg.gopher_quality.enabled {
            filters.push(Box::new(gopher::GopherQuality::new(&cfg.gopher_quality)));
        }
        if cfg.c4.enabled {
            filters.push(Box::new(c4::C4Filter::new(&cfg.c4)));
        }
        if cfg.bad_words.enabled {
            filters.push(Box::new(badwords::BadWordsFilter::new(&cfg.bad_words)?));
        }
        Ok(Self { filters })
    }

    /// Build from an explicit list (tests, custom pipelines).
    pub fn from_filters(filters: Vec<Box<dyn Filter>>) -> Self {
        Self { filters }
    }

    /// Run all filters in order; the first rejection wins.
    pub fn apply(&self, doc: &mut FilterDoc) -> Result<(), Rejection> {
        for f in &self.filters {
            f.apply(doc)?;
        }
        Ok(())
    }

    /// Names of the filter families, in execution order.
    pub fn order(&self) -> Vec<&'static str> {
        self.filters.iter().map(|f| f.name()).collect()
    }

    /// All rule names the chain can emit.
    pub fn all_rules(&self) -> Vec<&'static str> {
        self.filters
            .iter()
            .flat_map(|f| f.rules().iter().copied())
            .collect()
    }
}

/// `a / b`, defined as 0 when `b == 0` so empty inputs never produce NaN
/// thresholds comparisons (NaN compares false with everything, which would
/// silently *pass* every `>` check).
pub(crate) fn ratio(a: usize, b: usize) -> f64 {
    if b == 0 {
        0.0
    } else {
        a as f64 / b as f64
    }
}
