//! Resolve input specs into [`ShardSpec`]s and stream their bytes.

use std::path::Path as FsPath;
use std::sync::Arc;

use bytes::Bytes;
use futures::StreamExt;
use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjPath;
use object_store::{ObjectStore, ObjectStoreExt};
use tokio::sync::mpsc;
use url::Url;

use crate::doc::ShardSpec;
use crate::error::{Result, SieveError};

/// File name suffixes recognised as WARC/WET input when listing a directory or prefix.
const INPUT_SUFFIXES: &[&str] = &[".warc.gz", ".wet.gz", ".warc", ".wet", ".warc.wet.gz"];

/// An error from the byte *source* (as opposed to the decoder). Wrapped inside
/// `io::Error` as it crosses the `Read` boundary so the decoder can tell
/// "network died" (retry later) from "gzip is corrupt" (dead-letter, move on).
#[derive(Debug, thiserror::Error)]
#[error("source error: {0}")]
pub struct SourceError(pub String);

fn is_input_name(name: &str) -> bool {
    INPUT_SUFFIXES.iter().any(|s| name.ends_with(s))
}

/// Resolve each input into one or more shards, sorted by location so shard ids
/// are stable for a given input set.
///
/// Accepted forms:
/// * a local file or directory (directories are listed non-recursively),
/// * `file://`, `s3://bucket/key`, `http(s)://host/path` URLs; a URL ending in
///   `/` is treated as a prefix and listed.
///
/// S3 credentials and region come from the usual `AWS_*` environment variables.
pub async fn resolve_inputs(inputs: &[String]) -> Result<Vec<ShardSpec>> {
    let mut found: Vec<(String, Arc<dyn ObjectStore>, ObjPath)> = Vec::new();
    let local: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new());

    for input in inputs {
        if input.contains("://") {
            let url = Url::parse(input).map_err(|e| SieveError::Input(format!("{input}: {e}")))?;
            let opts = std::env::vars()
                .filter(|(k, _)| k.starts_with("AWS_"))
                .map(|(k, v)| (k.to_ascii_lowercase(), v));
            let (store, path) = object_store::parse_url_opts(&url, opts)?;
            let store: Arc<dyn ObjectStore> = Arc::from(store);
            if input.ends_with('/') {
                let mut listing = store.list(Some(&path));
                while let Some(meta) = listing.next().await {
                    let meta = meta?;
                    if is_input_name(meta.location.as_ref()) {
                        let loc = format!("{}{}", input, meta.location.filename().unwrap_or(""));
                        found.push((loc, store.clone(), meta.location));
                    }
                }
            } else {
                found.push((input.clone(), store, path));
            }
        } else {
            let p = FsPath::new(input);
            let abs =
                std::fs::canonicalize(p).map_err(|e| SieveError::Input(format!("{input}: {e}")))?;
            if abs.is_dir() {
                let entries = std::fs::read_dir(&abs)
                    .map_err(|e| SieveError::Input(format!("{input}: {e}")))?;
                for entry in entries.flatten() {
                    let fp = entry.path();
                    let name = fp.file_name().and_then(|n| n.to_str()).unwrap_or("");
                    if fp.is_file() && is_input_name(name) {
                        let op = ObjPath::from_filesystem_path(&fp)
                            .map_err(|e| SieveError::Input(e.to_string()))?;
                        found.push((fp.display().to_string(), local.clone(), op));
                    }
                }
            } else {
                let op = ObjPath::from_filesystem_path(&abs)
                    .map_err(|e| SieveError::Input(e.to_string()))?;
                found.push((abs.display().to_string(), local.clone(), op));
            }
        }
    }

    found.sort_by(|a, b| a.0.cmp(&b.0));
    found.dedup_by(|a, b| a.0 == b.0);
    if found.is_empty() {
        return Err(SieveError::Input("no input shards found".into()));
    }
    Ok(found
        .into_iter()
        .enumerate()
        .map(|(i, (location, store, path))| ShardSpec {
            id: i as u32,
            stem: shard_stem(&location),
            location,
            store,
            path,
        })
        .collect())
}

/// Filesystem-safe, collision-resistant output name for a shard: the input's
/// base name without WARC/gzip extensions, plus 8 hex digits of a hash of the
/// full location (two prefixes may contain the same file name).
pub fn shard_stem(location: &str) -> String {
    let base = location.rsplit('/').next().unwrap_or(location);
    let mut stem = base.to_string();
    for suffix in [".gz", ".wet", ".warc"] {
        if let Some(s) = stem.strip_suffix(suffix) {
            stem = s.to_string();
        }
    }
    let clean: String = stem
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.' {
                c
            } else {
                '_'
            }
        })
        .collect();
    let h = xxhash_rust::xxh3::xxh3_64(location.as_bytes()) as u32;
    format!("{clean}-{h:08x}")
}

/// Stream one shard's bytes into `tx`. Runs on the tokio runtime.
///
/// `tx` is bounded, so `send().await` suspends this task when the decoder
/// thread falls behind: that is the first link of the backpressure chain, and it
/// is what stops a fast disk or network from filling RAM.
///
/// Errors are forwarded in-band as `Err(SourceError)` so the decoder sees them
/// at the exact byte position where the stream broke.
pub async fn fetch_shard(spec: &ShardSpec, tx: mpsc::Sender<std::io::Result<Bytes>>) {
    let result = match spec.store.get(&spec.path).await {
        Ok(r) => r,
        Err(e) => {
            let _ = tx.send(Err(source_io_error(e.to_string()))).await;
            return;
        }
    };
    let mut stream = result.into_stream();
    while let Some(chunk) = stream.next().await {
        let item = chunk.map_err(|e| source_io_error(e.to_string()));
        let failed = item.is_err();
        metrics::counter!(crate::metrics::BYTES_READ)
            .increment(item.as_ref().map_or(0, |b| b.len() as u64));
        if tx.send(item).await.is_err() || failed {
            // Receiver gone (decoder stopped early) or source failed: stop fetching.
            return;
        }
    }
}

fn source_io_error(msg: String) -> std::io::Error {
    std::io::Error::other(SourceError(msg))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stem_strips_extensions_and_is_unique_per_location() {
        let a = shard_stem("s3://cc/x/CC-MAIN-1-00000.warc.wet.gz");
        let b = shard_stem("s3://cc/y/CC-MAIN-1-00000.warc.wet.gz");
        assert!(a.starts_with("CC-MAIN-1-00000-"), "{a}");
        assert_ne!(a, b);
    }

    #[tokio::test]
    async fn resolves_directory_and_sorts() {
        let dir = tempfile::tempdir().unwrap();
        for name in ["b.warc.gz", "a.wet.gz", "ignore.txt"] {
            std::fs::write(dir.path().join(name), b"x").unwrap();
        }
        let shards = resolve_inputs(&[dir.path().display().to_string()])
            .await
            .unwrap();
        assert_eq!(shards.len(), 2);
        assert!(shards[0].location.ends_with("a.wet.gz"));
        assert_eq!(shards[1].id, 1);
    }

    #[tokio::test]
    async fn missing_input_is_an_error() {
        assert!(resolve_inputs(&["/definitely/not/here.warc.gz".into()])
            .await
            .is_err());
    }
}
