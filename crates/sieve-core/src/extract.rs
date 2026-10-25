//! Extract: WARC record → plain text.
//!
//! WET `conversion` records are already text (Common Crawl ran its own
//! extractor), so they pass through. WARC `response` records carry an HTTP
//! envelope around HTML; we parse the envelope, keep only successful HTML
//! responses, then walk the DOM emitting text while skipping boilerplate.

use std::collections::HashSet;

use ego_tree::iter::Edge;
use scraper::{Html, Node};

use crate::config::ExtractConfig;
use crate::doc::{RawRecord, RecordKind};

/// Why a record produced no text. `Drop` reasons are normal data (an image, a
/// 404); `Malformed` means the record itself is broken and is dead-lettered.
#[derive(Debug, Clone, PartialEq)]
pub enum ExtractError {
    /// A legitimate record we choose not to keep. Carries the rule name.
    Drop(&'static str),
    /// The record could not be interpreted.
    Malformed(String),
}

/// Turns a raw record into text. A trait so alternative extractors (e.g. a
/// readability-style scorer, or trafilatura over FFI) can be swapped in and
/// benchmarked against this one without touching the pipeline.
pub trait Extractor: Send + Sync {
    /// Extract the document text.
    fn extract(&self, record: &RawRecord) -> Result<String, ExtractError>;
}

/// DOM-walking extractor with tag- and class-based boilerplate removal.
pub struct HtmlExtractor {
    skip_tags: HashSet<String>,
    markers: Vec<String>,
}

/// Elements that end a line of text.
const LINE_BREAK_TAGS: &[&str] = &[
    "br",
    "div",
    "section",
    "article",
    "main",
    "td",
    "th",
    "dd",
    "dt",
    "figcaption",
    "address",
    "hr",
];
/// Elements that end a paragraph (blank line after).
const PARA_TAGS: &[&str] = &[
    "p",
    "h1",
    "h2",
    "h3",
    "h4",
    "h5",
    "h6",
    "li",
    "tr",
    "blockquote",
    "pre",
    "table",
    "ul",
    "ol",
    "dl",
];
/// Never skipped by class/id markers: pages often put layout classes such as
/// `has-sidebar` on `<body>`, and skipping it would drop the whole page.
const MARKER_EXEMPT: &[&str] = &["html", "body", "main", "article"];

impl HtmlExtractor {
    /// Build from config.
    pub fn new(cfg: &ExtractConfig) -> Self {
        Self {
            skip_tags: cfg
                .skip_tags
                .iter()
                .map(|t| t.to_ascii_lowercase())
                .collect(),
            markers: cfg
                .boilerplate_markers
                .iter()
                .map(|m| m.to_ascii_lowercase())
                .collect(),
        }
    }

    fn is_boilerplate(&self, el: &scraper::node::Element) -> bool {
        let name = el.name();
        if self.skip_tags.contains(name) {
            return true;
        }
        if MARKER_EXEMPT.contains(&name) || self.markers.is_empty() {
            return false;
        }
        // ARIA roles are the most reliable boilerplate signal when present.
        if let Some(role) = el.attr("role") {
            if matches!(
                role,
                "navigation" | "banner" | "contentinfo" | "complementary"
            ) {
                return true;
            }
        }
        for attr in ["class", "id"] {
            if let Some(v) = el.attr(attr) {
                let v = v.to_ascii_lowercase();
                if self.markers.iter().any(|m| v.contains(m.as_str())) {
                    return true;
                }
            }
        }
        false
    }

    /// Extract visible, non-boilerplate text from an HTML string.
    ///
    /// Uses `traverse()` (an explicit open/close event stream) instead of
    /// recursion: real-world HTML can nest thousands of levels deep and a
    /// recursive walk would overflow a worker thread's stack.
    pub fn html_to_text(&self, html: &str) -> String {
        let doc = Html::parse_document(html);
        let mut out = String::with_capacity(html.len() / 4);
        let mut skip_depth = 0usize;
        let mut pre_depth = 0usize;

        for edge in doc.tree.root().traverse() {
            match edge {
                Edge::Open(node) => match node.value() {
                    Node::Element(el) => {
                        if skip_depth > 0 || self.is_boilerplate(el) {
                            skip_depth += 1;
                        } else if el.name() == "pre" {
                            pre_depth += 1;
                        }
                    }
                    Node::Text(t) if skip_depth == 0 => {
                        if pre_depth > 0 {
                            out.push_str(t);
                        } else {
                            push_collapsed(&mut out, t);
                        }
                    }
                    _ => {}
                },
                Edge::Close(node) => {
                    if let Node::Element(el) = node.value() {
                        if skip_depth > 0 {
                            skip_depth -= 1;
                            continue;
                        }
                        let name = el.name();
                        if name == "pre" {
                            pre_depth = pre_depth.saturating_sub(1);
                        }
                        if PARA_TAGS.contains(&name) {
                            out.push_str("\n\n");
                        } else if LINE_BREAK_TAGS.contains(&name) {
                            out.push('\n');
                        }
                    }
                }
            }
        }
        tidy_lines(&out)
    }
}

/// Append text with runs of whitespace collapsed to one space (HTML rendering
/// semantics outside `<pre>`).
fn push_collapsed(out: &mut String, text: &str) {
    let mut last_space = out.ends_with([' ', '\n']) || out.is_empty();
    for c in text.chars() {
        if c.is_whitespace() {
            if !last_space {
                out.push(' ');
                last_space = true;
            }
        } else {
            out.push(c);
            last_space = false;
        }
    }
}

/// Trim every line and collapse runs of blank lines into a single blank line,
/// so paragraph structure survives (Gopher's paragraph filters need it).
fn tidy_lines(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut blank_run = true; // suppress leading blank lines
    for line in s.lines() {
        let line = line.trim();
        if line.is_empty() {
            if !blank_run {
                out.push('\n');
                blank_run = true;
            }
        } else {
            out.push_str(line);
            out.push('\n');
            blank_run = false;
        }
    }
    while out.ends_with('\n') {
        out.pop();
    }
    out
}

/// Split an HTTP response into (status, content-type, body).
fn parse_http(body: &[u8]) -> Result<(u16, Option<String>, &[u8]), ExtractError> {
    let end = find_subslice(body, b"\r\n\r\n")
        .map(|i| (i, i + 4))
        .or_else(|| find_subslice(body, b"\n\n").map(|i| (i, i + 2)))
        .ok_or_else(|| ExtractError::Malformed("HTTP header terminator not found".into()))?;
    let head = std::str::from_utf8(&body[..end.0])
        .map_err(|_| ExtractError::Malformed("HTTP header is not UTF-8".into()))?;
    let mut lines = head.lines();
    let status_line = lines.next().unwrap_or("");
    let status = status_line
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse::<u16>().ok())
        .filter(|_| status_line.starts_with("HTTP/"))
        .ok_or_else(|| {
            ExtractError::Malformed(format!("bad HTTP status line: {status_line:.80}"))
        })?;
    let content_type = lines.find_map(|l| {
        let (k, v) = l.split_once(':')?;
        k.trim()
            .eq_ignore_ascii_case("content-type")
            .then(|| v.trim().to_ascii_lowercase())
    });
    Ok((status, content_type, &body[end.1..]))
}

fn find_subslice(hay: &[u8], needle: &[u8]) -> Option<usize> {
    hay.windows(needle.len()).position(|w| w == needle)
}

impl Extractor for HtmlExtractor {
    fn extract(&self, record: &RawRecord) -> Result<String, ExtractError> {
        let text = match record.kind {
            // Lossy decoding: non-UTF-8 pages (legacy charsets) get U+FFFD
            // replacements rather than being rejected. See README limitations.
            RecordKind::Conversion => String::from_utf8_lossy(&record.body).into_owned(),
            RecordKind::Resource => self.html_to_text(&String::from_utf8_lossy(&record.body)),
            RecordKind::Response => {
                let (status, ctype, payload) = parse_http(&record.body)?;
                if status != 200 {
                    return Err(ExtractError::Drop("extract.http_status"));
                }
                let is_html = ctype
                    .as_deref()
                    .is_none_or(|c| c.contains("html") || c.contains("xml"));
                if !is_html {
                    return Err(ExtractError::Drop("extract.not_html"));
                }
                self.html_to_text(&String::from_utf8_lossy(payload))
            }
        };
        if text.trim().is_empty() {
            return Err(ExtractError::Drop("extract.empty"));
        }
        Ok(text)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::doc::DocMeta;

    fn ex() -> HtmlExtractor {
        HtmlExtractor::new(&ExtractConfig::default())
    }

    fn rec(kind: RecordKind, body: &str) -> RawRecord {
        RawRecord {
            meta: DocMeta {
                shard: 0,
                seq: 0,
                url: "http://x/".into(),
                record_id: "r".into(),
                timestamp_ms: 0,
                cc_lang: None,
            },
            kind,
            body: body.as_bytes().to_vec(),
        }
    }

    #[test]
    fn removes_boilerplate_tags_and_classes() {
        let html = r#"<html><head><title>T</title><style>.a{}</style></head>
            <body class="has-sidebar">
            <nav><a href="/">Home</a> | <a>About</a></nav>
            <div class="sidebar-widget">Popular posts</div>
            <article><h1>Real   title</h1><p>First paragraph
            of text.</p><p>Second <b>bold</b> paragraph.</p></article>
            <script>var x = 1;</script>
            <footer>Copyright 2024</footer></body></html>"#;
        let text = ex().html_to_text(html);
        assert_eq!(
            text,
            "Real title\n\nFirst paragraph of text.\n\nSecond bold paragraph."
        );
    }

    #[test]
    fn preserves_pre_whitespace() {
        let text = ex().html_to_text("<body><pre>a  b\n  c</pre></body>");
        assert_eq!(text, "a  b\nc"); // lines are trimmed by tidy_lines
    }

    #[test]
    fn deep_nesting_does_not_overflow() {
        let html = format!("{}deep{}", "<div>".repeat(20_000), "</div>".repeat(20_000));
        assert_eq!(ex().html_to_text(&html), "deep");
    }

    #[test]
    fn http_envelope_is_parsed_and_filtered() {
        let ok = "HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\n\r\n<p>Hi there</p>";
        assert_eq!(
            ex().extract(&rec(RecordKind::Response, ok)).unwrap(),
            "Hi there"
        );

        let img = "HTTP/1.1 200 OK\r\nContent-Type: image/png\r\n\r\n\u{89}PNG";
        assert_eq!(
            ex().extract(&rec(RecordKind::Response, img)),
            Err(ExtractError::Drop("extract.not_html"))
        );

        let notfound = "HTTP/1.1 404 Not Found\r\nContent-Type: text/html\r\n\r\n<p>nope</p>";
        assert_eq!(
            ex().extract(&rec(RecordKind::Response, notfound)),
            Err(ExtractError::Drop("extract.http_status"))
        );

        let garbage = "this is not http at all";
        assert!(matches!(
            ex().extract(&rec(RecordKind::Response, garbage)),
            Err(ExtractError::Malformed(_))
        ));
    }

    #[test]
    fn wet_conversion_passes_through() {
        let r = rec(RecordKind::Conversion, "already\nplain text");
        assert_eq!(ex().extract(&r).unwrap(), "already\nplain text");
    }
}
