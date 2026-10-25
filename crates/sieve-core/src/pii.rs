//! PII redaction: emails, phone numbers and IP addresses → placeholder tokens.
//!
//! All enabled patterns are compiled into *one* alternation so each document is
//! scanned once; the `regex` crate turns that into a single automaton, so the
//! cost is close to one pass regardless of how many patterns are enabled.
//! Matches are validated in the replacement closure where a regex alone would
//! be too permissive (IPv4 octets must be ≤ 255).
//!
//! Word boundaries are ASCII (`(?-u:\b)`). The patterns only match ASCII
//! characters anyway, and a *Unicode* `\b` cannot be executed by the regex
//! crate's DFA engines on non-ASCII input, forcing a fallback to a far slower
//! engine on almost every real web page (see README "Profiling").
//!
//! This is pattern-based redaction: it has false positives (version strings
//! like `1.2.3.4` look like IPs) and false negatives (phone formats it does not
//! know, names, addresses). It is a baseline, not a guarantee.

use std::borrow::Cow;

use regex::{Captures, Regex};

use crate::config::PiiConfig;
use crate::error::{Result, SieveError};

/// Replacement for email addresses.
pub const EMAIL_TOKEN: &str = "<EMAIL_ADDRESS>";
/// Replacement for phone numbers.
pub const PHONE_TOKEN: &str = "<PHONE_NUMBER>";
/// Replacement for IP addresses.
pub const IP_TOKEN: &str = "<IP_ADDRESS>";

const EMAIL: &str =
    r"(?P<email>(?-u:\b)[A-Za-z0-9._%+-]+@[A-Za-z0-9-]+(?:\.[A-Za-z0-9-]+)*\.[A-Za-z]{2,}(?-u:\b))";
// Optional country code, then a 2–4 digit area code (optionally in
// parentheses), then two groups of 3–4 digits, with consistent-ish separators.
// Requiring separators avoids redacting plain long numbers (prices, IDs).
const PHONE: &str = r"(?P<phone>(?:\+\d{1,3}[ .-]?)?(?:\(\d{2,4}\)[ .-]?|(?-u:\b)\d{2,4}[ .-])\d{3,4}[ .-]\d{3,4}(?-u:\b))";
const IPV4: &str = r"(?P<ipv4>(?-u:\b)(?:\d{1,3}\.){3}\d{1,3}(?-u:\b))";
// Full 8-group form only; compressed forms (`::`) are a known gap.
const IPV6: &str = r"(?P<ipv6>(?-u:\b)(?:[0-9A-Fa-f]{1,4}:){7}[0-9A-Fa-f]{1,4}(?-u:\b))";

/// Counts of redacted spans by kind.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RedactionCounts {
    /// Emails replaced.
    pub email: u32,
    /// Phone numbers replaced.
    pub phone: u32,
    /// IPs replaced.
    pub ip: u32,
}

impl RedactionCounts {
    /// Total replacements.
    pub fn total(&self) -> u32 {
        self.email + self.phone + self.ip
    }
}

/// Replaces PII spans. A trait so an NER-based redactor could slot in.
pub trait Redactor: Send + Sync {
    /// Return the redacted text (borrowed if nothing changed) and counts.
    fn redact<'a>(&self, text: &'a str) -> (Cow<'a, str>, RedactionCounts);
}

/// Regex-based redactor.
pub struct RegexRedactor {
    re: Option<Regex>,
}

impl RegexRedactor {
    /// Compile the enabled patterns.
    pub fn new(cfg: &PiiConfig) -> Result<Self> {
        if !cfg.enabled {
            return Ok(Self { re: None });
        }
        let mut parts = Vec::new();
        // Order matters for overlapping alternatives: an email such as
        // `a.1.2.3@x.org` must win over the IPv4 inside it, and an IPv4 must win
        // over the phone pattern.
        if cfg.email {
            parts.push(EMAIL);
        }
        if cfg.ip {
            parts.push(IPV6);
            parts.push(IPV4);
        }
        if cfg.phone {
            parts.push(PHONE);
        }
        if parts.is_empty() {
            return Ok(Self { re: None });
        }
        let re = Regex::new(&parts.join("|")).map_err(|e| SieveError::Config {
            msg: format!("pii regex: {e}"),
        })?;
        Ok(Self { re: Some(re) })
    }
}

fn valid_ipv4(s: &str) -> bool {
    s.split('.')
        .all(|o| o.parse::<u16>().is_ok_and(|v| v <= 255))
}

impl Redactor for RegexRedactor {
    fn redact<'a>(&self, text: &'a str) -> (Cow<'a, str>, RedactionCounts) {
        let mut counts = RedactionCounts::default();
        let Some(re) = &self.re else {
            return (Cow::Borrowed(text), counts);
        };
        let out = re.replace_all(text, |c: &Captures<'_>| {
            if c.name("email").is_some() {
                counts.email += 1;
                EMAIL_TOKEN.to_string()
            } else if let Some(m) = c.name("ipv4") {
                if valid_ipv4(m.as_str()) {
                    counts.ip += 1;
                    IP_TOKEN.to_string()
                } else {
                    m.as_str().to_string()
                }
            } else if c.name("ipv6").is_some() {
                counts.ip += 1;
                IP_TOKEN.to_string()
            } else {
                counts.phone += 1;
                PHONE_TOKEN.to_string()
            }
        });
        (out, counts)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn r() -> RegexRedactor {
        RegexRedactor::new(&PiiConfig::default()).unwrap()
    }

    #[test]
    fn redacts_each_kind() {
        let (out, c) = r().redact(
            "Mail jane.doe+news@example.co.uk or call (555) 123-4567 / +44 20 7946 0958. \
             Server at 192.168.1.20 and fe80:0:0:0:202:b3ff:fe1e:8329.",
        );
        assert_eq!(
            out,
            "Mail <EMAIL_ADDRESS> or call <PHONE_NUMBER> / <PHONE_NUMBER>. \
             Server at <IP_ADDRESS> and <IP_ADDRESS>."
        );
        assert_eq!(
            c,
            RedactionCounts {
                email: 1,
                phone: 2,
                ip: 2
            }
        );
    }

    #[test]
    fn leaves_non_pii_alone() {
        let text = "In 2024 the price was 1500 dollars; see version 999.1.1.1 and 2023-01-28.";
        let (out, c) = r().redact(text);
        assert_eq!(out, text);
        assert_eq!(c.total(), 0);
        let (out, _) = r().redact("nothing to see here");
        assert!(
            matches!(out, Cow::Borrowed(_)),
            "no allocation when nothing matched"
        );
    }

    #[test]
    fn disabled_is_identity() {
        let red = RegexRedactor::new(&PiiConfig {
            enabled: false,
            ..Default::default()
        })
        .unwrap();
        assert_eq!(red.redact("a@b.com").0, "a@b.com");
    }
}
