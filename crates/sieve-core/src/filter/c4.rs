//! C4 heuristics (Raffel et al. 2020, "Exploring the Limits of Transfer
//! Learning", §2.2). Unlike the Gopher rules this filter *edits* text: the line
//! filter keeps only lines that look like prose, which is C4's main boilerplate
//! remover for already-extracted text such as WET.

use super::{Filter, FilterDoc, Rejection};
use crate::config::C4Config;

/// C4 line filter plus document-level rules.
pub struct C4Filter {
    cfg: C4Config,
}

impl C4Filter {
    /// Build from config.
    pub fn new(cfg: &C4Config) -> Self {
        Self { cfg: cfg.clone() }
    }

    fn keep_line(&self, line: &str, lower: &str) -> bool {
        if line.split_whitespace().count() < self.cfg.min_words_per_line {
            return false;
        }
        if self.cfg.require_terminal_punct && !line.ends_with(TERMINAL_PUNCT) {
            return false;
        }
        if lower.contains("javascript") {
            return false; // "please enable JavaScript" banners
        }
        if POLICY_PHRASES.iter().any(|p| lower.contains(p)) {
            return false;
        }
        // Absurdly long "words" are minified code or base64 blobs.
        !line.split_whitespace().any(|w| w.len() > 1000)
    }
}

const TERMINAL_PUNCT: &[char] = &['.', '!', '?', '"', '\'', '\u{201d}', '\u{2019}'];
const POLICY_PHRASES: &[&str] = &[
    "terms of use",
    "privacy policy",
    "cookie policy",
    "uses cookies",
    "use of cookies",
    "use cookies",
];

/// Approximate sentence count: terminal punctuation followed by whitespace or
/// end of text. Abbreviations ("e.g. ") over-count; that bias is accepted for
/// a filter whose job is to reject pages with almost no sentences at all.
pub(crate) fn count_sentences(text: &str) -> usize {
    let mut n = 0;
    let mut chars = text.chars().peekable();
    while let Some(c) = chars.next() {
        if matches!(c, '.' | '!' | '?') {
            match chars.peek() {
                None => n += 1,
                Some(nc) if nc.is_whitespace() => n += 1,
                _ => {}
            }
        }
    }
    n
}

impl Filter for C4Filter {
    fn name(&self) -> &'static str {
        "c4"
    }
    fn rules(&self) -> &'static [&'static str] {
        &["c4.lorem_ipsum", "c4.curly_brace", "c4.min_sentences"]
    }

    fn apply(&self, doc: &mut FilterDoc) -> Result<(), Rejection> {
        let lower_doc = doc.text.to_lowercase();
        if self.cfg.drop_lorem_ipsum && lower_doc.contains("lorem ipsum") {
            return Err(Rejection::new("c4.lorem_ipsum", f64::NAN));
        }
        if self.cfg.drop_curly_brace && doc.text.contains('{') {
            return Err(Rejection::new("c4.curly_brace", f64::NAN));
        }
        if self.cfg.line_filter {
            let mut kept = String::with_capacity(doc.text.len());
            let mut removed = 0u32;
            for line in doc.text.lines() {
                let line = line.trim();
                if line.is_empty() {
                    continue;
                }
                if self.keep_line(line, &line.to_lowercase()) {
                    if !kept.is_empty() {
                        kept.push('\n');
                    }
                    kept.push_str(line);
                } else {
                    removed += 1;
                }
            }
            doc.text = kept;
            doc.signals.c4_lines_removed = removed;
        }
        let n = count_sentences(&doc.text);
        if n < self.cfg.min_sentences {
            return Err(Rejection::new("c4.min_sentences", n as f64));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run(text: &str) -> Result<FilterDoc, Rejection> {
        let f = C4Filter::new(&C4Config::default());
        let mut d = FilterDoc {
            text: text.into(),
            ..Default::default()
        };
        f.apply(&mut d).map(|_| d)
    }

    #[test]
    fn line_filter_removes_nav_and_policy_lines() {
        let text = "Home | About | Contact\n\
            This site uses cookies to improve your experience.\n\
            The first real sentence is here. It is fine.\n\
            Another sentence follows it here. Good!\n\
            Please enable JavaScript to view comments.\n\
            The final line also ends well.\n\
            Copyright 2024";
        let d = run(text).unwrap();
        assert_eq!(
            d.text,
            "The first real sentence is here. It is fine.\n\
             Another sentence follows it here. Good!\n\
             The final line also ends well."
        );
        assert_eq!(d.signals.c4_lines_removed, 4);
    }

    #[test]
    fn doc_rules() {
        assert_eq!(
            run("Lorem ipsum dolor sit amet.").unwrap_err().rule,
            "c4.lorem_ipsum"
        );
        assert_eq!(
            run("function f() { return 1; }").unwrap_err().rule,
            "c4.curly_brace"
        );
        assert_eq!(
            run("Only one sentence here.").unwrap_err().rule,
            "c4.min_sentences"
        );
    }

    #[test]
    fn sentence_counter() {
        assert_eq!(count_sentences("A b. C d! E f? x.y"), 3);
        assert_eq!(count_sentences("End."), 1);
    }
}
