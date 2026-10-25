//! Text normalisation shared by the filters and dedup.
//!
//! Dedup must compare documents modulo trivial differences (case, punctuation,
//! whitespace), otherwise "Hello, World!" and "hello world" count as distinct
//! and exact dedup misses the most common kind of copy: re-templated pages.

/// Lower-case, replace every non-alphanumeric character with a space, and
/// collapse runs of whitespace. Output words are separated by single spaces.
///
/// Unicode-aware (`char::is_alphanumeric`), so CJK and accented text survive;
/// it does *not* strip accents or apply NFKC, which is a documented limitation.
pub fn normalize_for_dedup(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut pending_space = false;
    for c in text.chars() {
        if c.is_alphanumeric() {
            if pending_space && !out.is_empty() {
                out.push(' ');
            }
            pending_space = false;
            // `to_lowercase` can expand one char into several (e.g. 'İ').
            for lc in c.to_lowercase() {
                out.push(lc);
            }
        } else {
            pending_space = true;
        }
    }
    out
}

/// Truncate to at most `max_bytes` without splitting a UTF-8 code point.
pub fn truncate_at_char_boundary(s: &str, max_bytes: usize) -> &str {
    if s.len() <= max_bytes {
        return s;
    }
    let mut end = max_bytes;
    while !s.is_char_boundary(end) {
        end -= 1;
    }
    &s[..end]
}

/// Split into paragraphs: blocks separated by one or more blank lines.
pub fn paragraphs(text: &str) -> impl Iterator<Item = &str> {
    text.split("\n\n").map(str::trim).filter(|p| !p.is_empty())
}

/// Split into non-empty, trimmed lines.
pub fn lines(text: &str) -> impl Iterator<Item = &str> {
    text.lines().map(str::trim).filter(|l| !l.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalization_collapses_case_punct_space() {
        assert_eq!(
            normalize_for_dedup("  Hello,   World!\n\tFoo-bar "),
            "hello world foo bar"
        );
        assert_eq!(normalize_for_dedup("!!!"), "");
        assert_eq!(normalize_for_dedup("Ünïcode 東京"), "ünïcode 東京");
    }

    #[test]
    fn truncate_respects_char_boundaries() {
        let s = "aé"; // 'é' is 2 bytes
        assert_eq!(truncate_at_char_boundary(s, 2), "a");
        assert_eq!(truncate_at_char_boundary(s, 3), "aé");
    }

    #[test]
    fn paragraphs_and_lines() {
        let t = "a\nb\n\n\n c \n\n";
        assert_eq!(paragraphs(t).collect::<Vec<_>>(), vec!["a\nb", "c"]);
        assert_eq!(lines(t).collect::<Vec<_>>(), vec!["a", "b", "c"]);
    }
}
