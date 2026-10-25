//! Synchronous decoding: bytes → gunzip → WARC records.
//!
//! Runs on a dedicated OS thread per shard. Nothing here is `async`: gzip
//! inflation and header parsing are CPU work, and running them on a tokio worker
//! would stall every other I/O task scheduled on that worker.

use std::io::{BufRead, BufReader, Read};
use std::time::Instant;

use bytes::{Buf, Bytes};
use crossbeam_channel::Sender;
use flate2::bufread::MultiGzDecoder;
use tokio::sync::mpsc;
use warc::{RecordType, WarcHeader, WarcReader};

use crate::doc::{DeadLetter, DocMeta, RawRecord, RecordKind, ShardId, ShardStatus, SinkMsg};
use crate::ingest::source::SourceError;
use crate::metrics as m;

/// Adapts the async byte channel into a blocking [`Read`].
///
/// This is the explicit tokio → thread hand-off: the async fetcher calls
/// `send().await`, this side calls `blocking_recv()`. `tokio::mpsc` is used
/// (rather than crossbeam) precisely because it supports both an async and a
/// blocking end, so neither side has to fake the other's execution model.
pub struct ChannelReader {
    rx: mpsc::Receiver<std::io::Result<Bytes>>,
    current: Bytes,
}

impl ChannelReader {
    /// Wrap the receiving end of the fetcher's channel.
    pub fn new(rx: mpsc::Receiver<std::io::Result<Bytes>>) -> Self {
        Self {
            rx,
            current: Bytes::new(),
        }
    }
}

impl Read for ChannelReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        while self.current.is_empty() {
            match self.rx.blocking_recv() {
                Some(Ok(chunk)) => self.current = chunk,
                Some(Err(e)) => return Err(e),
                None => return Ok(0), // sender dropped after the last chunk: EOF
            }
        }
        let n = buf.len().min(self.current.len());
        buf[..n].copy_from_slice(&self.current[..n]);
        self.current.advance(n);
        Ok(n)
    }
}

/// What a decoder thread observed for its shard.
#[derive(Debug, Clone, Default)]
pub struct DecodeStats {
    /// Documents emitted downstream.
    pub docs: u64,
    /// Records dead-lettered.
    pub dead_letters: u64,
    /// Records ignored because their type carries no document (request, metadata, …).
    pub skipped: u64,
    /// Time spent decoding, in nanoseconds: wall time minus time blocked
    /// handing records downstream (that time is backpressure, not work).
    pub busy_ns: u64,
}

/// Decode one shard, sending documents to `docs` and dead letters plus the final
/// [`SinkMsg::ShardEnd`] to `sink`.
///
/// Both channels are bounded; a blocking `send` here is how backpressure from
/// the CPU stages reaches the decoder, which in turn stops draining the async
/// byte channel, which suspends the fetcher. One full queue anywhere slows the
/// whole chain down to the speed of its slowest stage instead of growing memory.
pub fn decode_shard(
    shard: ShardId,
    input: impl Read,
    max_record_bytes: usize,
    docs: &Sender<RawRecord>,
    sink: &Sender<SinkMsg>,
) -> DecodeStats {
    let start = Instant::now();
    let mut stats = DecodeStats::default();
    let decoded_ctr = metrics::counter!(m::STAGE_DOCS, "stage" => "ingest");
    let busy_ctr = metrics::counter!(m::STAGE_BUSY_NS, "stage" => "ingest");
    let mut blocked_ns = 0u64;
    let mut last_report = Instant::now();
    let dead_ctr = metrics::counter!(m::DEAD_LETTERS);

    let dead = |stats: &mut DecodeStats,
                reason: String,
                record_id: Option<String>,
                url: Option<String>| {
        stats.dead_letters += 1;
        dead_ctr.increment(1);
        let _ = sink.send(SinkMsg::DeadLetter(DeadLetter {
            shard,
            record_id,
            url,
            snippet: String::new(),
            reason,
        }));
    };

    let mut raw = BufReader::with_capacity(64 * 1024, input);
    let status = 'decode: {
        // Sniff the gzip magic instead of trusting the file extension.
        let is_gzip = match raw.fill_buf() {
            Ok(b) => b.starts_with(&[0x1f, 0x8b]),
            Err(e) => {
                let st = classify_io(&e);
                if let ShardStatus::Corrupt(reason) = &st {
                    dead(&mut stats, reason.clone(), None, None);
                }
                break 'decode st;
            }
        };
        // Common Crawl files are *multi-member* gzip (one member per record);
        // a plain GzDecoder would stop after the first record.
        let reader: Box<dyn BufRead> = if is_gzip {
            Box::new(BufReader::with_capacity(
                256 * 1024,
                MultiGzDecoder::new(raw),
            ))
        } else {
            Box::new(raw)
        };
        let mut warc = WarcReader::new(reader);
        let mut records = warc.stream_records();
        let lang_header = WarcHeader::from("WARC-Identified-Content-Language");

        loop {
            let mut record = match records.next_item() {
                None => break 'decode ShardStatus::Complete,
                Some(Ok(r)) => r,
                Some(Err(err)) => match err {
                    // A record with a missing/malformed *field*: the framing was
                    // fine and the parser already knows the body length, so the
                    // stream stays aligned. Dead-letter and continue.
                    warc::Error::MissingHeader(_) | warc::Error::MalformedHeader(..) => {
                        dead(&mut stats, format!("warc header: {err}"), None, None);
                        continue;
                    }
                    warc::Error::ReadData(io) => {
                        let st = classify_io(&io);
                        if let ShardStatus::Corrupt(reason) = &st {
                            dead(&mut stats, reason.clone(), None, None);
                        }
                        break 'decode st;
                    }
                    // Framing errors: we no longer know where the next record
                    // starts, so everything after this point is unreachable.
                    other => {
                        let reason = format!("warc framing: {other}");
                        dead(&mut stats, reason.clone(), None, None);
                        break 'decode ShardStatus::Corrupt(reason);
                    }
                },
            };

            let kind = match record.warc_type() {
                RecordType::Response => RecordKind::Response,
                RecordType::Conversion => RecordKind::Conversion,
                RecordType::Resource => RecordKind::Resource,
                _ => {
                    stats.skipped += 1;
                    continue; // body is skipped by the next `next_item` call
                }
            };
            let record_id = record.warc_id().to_string();
            let url = record.header(WarcHeader::TargetURI).map(|u| u.into_owned());
            let Some(url) = url else {
                dead(
                    &mut stats,
                    "missing WARC-Target-URI".into(),
                    Some(record_id),
                    None,
                );
                continue;
            };
            let len = record.content_length() as usize;
            if len > max_record_bytes {
                // Never buffered: the parser skips the body on the next call.
                dead(
                    &mut stats,
                    format!("record body {len} B exceeds max_record_bytes"),
                    Some(record_id),
                    Some(url),
                );
                continue;
            }
            let meta = DocMeta {
                shard,
                seq: stats.docs,
                timestamp_ms: record.date().timestamp_millis(),
                cc_lang: record.header(lang_header.clone()).map(|l| l.into_owned()),
                url,
                record_id,
            };
            let mut body = Vec::with_capacity(len);
            if let Err(io) = record.read_to_end(&mut body) {
                let st = classify_io(&io);
                if let ShardStatus::Corrupt(reason) = &st {
                    dead(
                        &mut stats,
                        reason.clone(),
                        Some(meta.record_id),
                        Some(meta.url),
                    );
                }
                break 'decode st;
            }
            stats.docs += 1;
            decoded_ctr.increment(1);
            let t_send = Instant::now();
            let sent = docs.send(RawRecord { meta, kind, body });
            let now = Instant::now();
            let waited = now.duration_since(t_send);
            blocked_ns += waited.as_nanos() as u64;
            // Report busy time incrementally so the dashboard sees a live rate.
            let since = now.duration_since(last_report).saturating_sub(waited);
            busy_ctr.increment(since.as_nanos() as u64);
            last_report = now;
            if sent.is_err() {
                break 'decode ShardStatus::SourceFailed("pipeline shut down".into());
            }
        }
    };

    stats.busy_ns = (start.elapsed().as_nanos() as u64).saturating_sub(blocked_ns);
    let _ = sink.send(SinkMsg::ShardEnd {
        shard,
        total: stats.docs + stats.dead_letters,
        status,
    });
    stats
}

/// Source errors are transient (retry the shard later); anything else that
/// surfaces as an I/O error while decoding is corruption in the bytes.
fn classify_io(err: &std::io::Error) -> ShardStatus {
    let is_source = err
        .get_ref()
        .is_some_and(|inner| inner.downcast_ref::<SourceError>().is_some());
    if is_source {
        ShardStatus::SourceFailed(err.to_string())
    } else {
        ShardStatus::Corrupt(format!("decode: {err}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    pub(crate) fn warc_record(kind: &str, url: Option<&str>, body: &str) -> Vec<u8> {
        let mut h = format!(
            "WARC/1.0\r\nWARC-Type: {kind}\r\nWARC-Date: 2024-02-20T21:40:38Z\r\n\
             WARC-Record-ID: <urn:uuid:{}>\r\nContent-Length: {}\r\n",
            xxhash_rust::xxh3::xxh3_64(body.as_bytes()),
            body.len()
        );
        if let Some(u) = url {
            h.push_str(&format!("WARC-Target-URI: {u}\r\n"));
        }
        h.push_str("\r\n");
        let mut v = h.into_bytes();
        v.extend_from_slice(body.as_bytes());
        v.extend_from_slice(b"\r\n\r\n");
        v
    }

    fn gz(data: &[u8]) -> Vec<u8> {
        let mut e = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
        e.write_all(data).unwrap();
        e.finish().unwrap()
    }

    fn run(input: Vec<u8>, max: usize) -> (Vec<RawRecord>, Vec<SinkMsg>, DecodeStats) {
        let (dtx, drx) = crossbeam_channel::unbounded();
        let (stx, srx) = crossbeam_channel::unbounded();
        let stats = decode_shard(7, std::io::Cursor::new(input), max, &dtx, &stx);
        drop((dtx, stx));
        (drx.iter().collect(), srx.iter().collect(), stats)
    }

    #[test]
    fn reads_multi_member_gzip_and_skips_non_doc_records() {
        let mut file = gz(&warc_record("warcinfo", None, "software: test"));
        file.extend(gz(&warc_record(
            "conversion",
            Some("http://a/"),
            "hello world",
        )));
        file.extend(gz(&warc_record(
            "request",
            Some("http://a/"),
            "GET / HTTP/1.1",
        )));
        file.extend(gz(&warc_record(
            "conversion",
            Some("http://b/"),
            "second doc",
        )));
        let (docs, sink, stats) = run(file, 1 << 20);
        assert_eq!(docs.len(), 2);
        assert_eq!(docs[1].body, b"second doc");
        assert_eq!(docs[1].meta.seq, 1);
        assert_eq!(stats.skipped, 2);
        assert!(matches!(
            sink.last(),
            Some(SinkMsg::ShardEnd {
                total: 2,
                status: ShardStatus::Complete,
                ..
            })
        ));
    }

    #[test]
    fn uncompressed_input_works() {
        let file = warc_record("conversion", Some("http://a/"), "plain");
        let (docs, _, _) = run(file, 1 << 20);
        assert_eq!(docs.len(), 1);
    }

    #[test]
    fn missing_uri_and_oversize_are_dead_lettered_and_stream_continues() {
        let mut file = warc_record("conversion", None, "no uri");
        file.extend(warc_record(
            "conversion",
            Some("http://big/"),
            &"x".repeat(100),
        ));
        file.extend(warc_record("conversion", Some("http://ok/"), "fine"));
        let (docs, sink, stats) = run(file, 50);
        assert_eq!(docs.len(), 1);
        assert_eq!(docs[0].meta.url, "http://ok/");
        assert_eq!(stats.dead_letters, 2);
        let total = sink.iter().find_map(|m| match m {
            SinkMsg::ShardEnd { total, .. } => Some(*total),
            _ => None,
        });
        assert_eq!(total, Some(3), "dead letters count toward the shard total");
    }

    #[test]
    fn truncated_gzip_is_corrupt_but_keeps_earlier_records() {
        let mut file = gz(&warc_record("conversion", Some("http://a/"), "first"));
        let second = gz(&warc_record(
            "conversion",
            Some("http://b/"),
            &"y".repeat(5000),
        ));
        file.extend_from_slice(&second[..second.len() / 2]);
        let (docs, sink, _) = run(file, 1 << 20);
        assert_eq!(docs.len(), 1);
        assert!(matches!(
            sink.last(),
            Some(SinkMsg::ShardEnd {
                status: ShardStatus::Corrupt(_),
                ..
            })
        ));
    }

    #[test]
    fn source_errors_are_classified_as_transient() {
        let (tx, rx) = mpsc::channel(4);
        let first = gz(&warc_record("conversion", Some("http://a/"), "first"));
        tx.try_send(Ok(Bytes::from(first))).unwrap();
        tx.try_send(Err(std::io::Error::other(SourceError(
            "connection reset".into(),
        ))))
        .unwrap();
        drop(tx);
        let (dtx, drx) = crossbeam_channel::unbounded();
        let (stx, srx) = crossbeam_channel::unbounded();
        decode_shard(0, ChannelReader::new(rx), 1 << 20, &dtx, &stx);
        drop((dtx, stx));
        assert_eq!(drx.iter().count(), 1);
        let last = srx.iter().last();
        assert!(
            matches!(
                last,
                Some(SinkMsg::ShardEnd {
                    status: ShardStatus::SourceFailed(_),
                    ..
                })
            ),
            "{last:?}"
        );
    }
}
