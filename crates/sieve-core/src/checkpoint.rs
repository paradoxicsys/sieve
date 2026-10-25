//! Shard-level checkpointing and the output directory layout.
//!
//! A shard is *committed* when, in this order:
//! 1. its data, rejects, dead-letter and dedup-delta files have been written to
//!    `*.tmp`, fsynced, and renamed to their final names, and
//! 2. the checkpoint file listing it has been atomically replaced.
//!
//! Step 2 is the commit point. A crash before it leaves at most some final-named
//! files for a shard the checkpoint does not list; a resumed run reprocesses
//! that shard and the renames overwrite them (rename is atomic and replaces the
//! destination), so the output is the same as if the crash never happened:
//! the commit is idempotent.

use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::doc::Fingerprint;
use crate::error::{io_err, Result, SieveError};

/// Checkpoint format version; bump on incompatible changes.
pub const CHECKPOINT_VERSION: u32 = 1;

/// Paths of everything the pipeline writes.
#[derive(Debug, Clone)]
pub struct OutputLayout {
    /// Root output directory.
    pub root: PathBuf,
}

impl OutputLayout {
    /// Create the directory tree.
    pub fn create(root: &Path) -> Result<Self> {
        for sub in ["data", "rejects", "dead_letter", "dedup_state"] {
            let d = root.join(sub);
            fs::create_dir_all(&d).map_err(io_err(&d))?;
        }
        Ok(Self {
            root: root.to_path_buf(),
        })
    }

    /// Kept documents for a shard.
    pub fn data(&self, stem: &str) -> PathBuf {
        self.root.join("data").join(format!("{stem}.parquet"))
    }
    /// Dropped documents for a shard.
    pub fn rejects(&self, stem: &str) -> PathBuf {
        self.root.join("rejects").join(format!("{stem}.parquet"))
    }
    /// Malformed records for a shard.
    pub fn dead_letter(&self, stem: &str) -> PathBuf {
        self.root.join("dead_letter").join(format!("{stem}.jsonl"))
    }
    /// Dedup fingerprints of the shard's kept docs.
    pub fn dedup_delta(&self, stem: &str) -> PathBuf {
        self.root.join("dedup_state").join(format!("{stem}.bin"))
    }
    /// The checkpoint file.
    pub fn checkpoint(&self) -> PathBuf {
        self.root.join("_checkpoint.json")
    }
    /// The run manifest.
    pub fn manifest(&self) -> PathBuf {
        self.root.join("_manifest.json")
    }

    /// Remove `*.tmp` files left behind by a crashed run. They belong to
    /// uncommitted shards, which will be reprocessed anyway.
    pub fn remove_stale_tmp(&self) -> Result<usize> {
        let mut n = 0;
        for sub in ["", "data", "rejects", "dead_letter", "dedup_state"] {
            let dir = self.root.join(sub);
            let Ok(entries) = fs::read_dir(&dir) else {
                continue;
            };
            for e in entries.flatten() {
                let p = e.path();
                if p.extension().is_some_and(|x| x == "tmp") {
                    fs::remove_file(&p).map_err(io_err(&p))?;
                    n += 1;
                }
            }
        }
        Ok(n)
    }
}

/// Temp path next to `final_path` (same directory → same filesystem, which is
/// what makes `rename` atomic).
pub fn tmp_path(final_path: &Path) -> PathBuf {
    let mut s = final_path.as_os_str().to_owned();
    s.push(".tmp");
    PathBuf::from(s)
}

/// Rename `tmp` over `dst` and fsync the directory so the rename itself is
/// durable (on POSIX a rename lives in the directory entry, not the file).
pub fn commit_file(tmp: &Path, dst: &Path) -> Result<()> {
    fs::rename(tmp, dst).map_err(io_err(dst))?;
    if let Some(dir) = dst.parent() {
        // Best effort: not every platform allows fsync on a directory handle.
        if let Ok(d) = File::open(dir) {
            let _ = d.sync_all();
        }
    }
    Ok(())
}

/// Write bytes to `dst` atomically: tmp file, fsync, rename.
pub fn atomic_write(dst: &Path, bytes: &[u8]) -> Result<()> {
    let tmp = tmp_path(dst);
    {
        let mut f = File::create(&tmp).map_err(io_err(&tmp))?;
        f.write_all(bytes).map_err(io_err(&tmp))?;
        f.sync_all().map_err(io_err(&tmp))?;
    }
    commit_file(&tmp, dst)
}

/// What happened to one committed shard. Also the per-shard section of the
/// run manifest, so totals can be recomputed from the checkpoint alone.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct ShardRecord {
    /// Output stem.
    pub stem: String,
    /// `"complete"` or `"corrupt"` (read stopped early on bad data).
    pub status: String,
    /// Corruption detail, if any.
    pub error: Option<String>,
    /// Documents that entered the pipeline (excludes dead letters).
    pub docs_in: u64,
    /// Rows written to the data file.
    pub kept: u64,
    /// Dead-lettered records.
    pub dead_letters: u64,
    /// Drops by rule name (includes `dedup.exact`, `dedup.near`).
    pub drops: BTreeMap<String, u64>,
    /// PII spans redacted.
    pub pii_redactions: u64,
    /// Unix time of commit.
    pub committed_at_unix: u64,
}

/// The durable record of which shards are done.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Checkpoint {
    /// Format version.
    pub version: u32,
    /// [`crate::config::Config::output_fingerprint`] as hex; a resume with a
    /// different value is refused.
    pub config_fingerprint: String,
    /// Committed shards keyed by input location. `BTreeMap` for a stable,
    /// diffable file.
    pub shards: BTreeMap<String, ShardRecord>,
}

impl Checkpoint {
    /// A new, empty checkpoint.
    pub fn new(fingerprint: u64) -> Self {
        Self {
            version: CHECKPOINT_VERSION,
            config_fingerprint: format!("{fingerprint:016x}"),
            shards: BTreeMap::new(),
        }
    }

    /// Load from disk if present.
    pub fn load(path: &Path) -> Result<Option<Self>> {
        match fs::read(path) {
            Ok(bytes) => {
                let ck: Checkpoint = serde_json::from_slice(&bytes)?;
                if ck.version != CHECKPOINT_VERSION {
                    return Err(SieveError::Checkpoint(format!(
                        "checkpoint version {} is not supported (expected {CHECKPOINT_VERSION})",
                        ck.version
                    )));
                }
                Ok(Some(ck))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(io_err(path)(e)),
        }
    }

    /// Persist atomically. Rewrites the whole file: O(shards) per commit, which
    /// is negligible next to processing a shard (~100 MB of input).
    pub fn save(&self, path: &Path) -> Result<()> {
        atomic_write(path, &serde_json::to_vec_pretty(self)?)
    }

    /// Fail unless this checkpoint was produced by an equivalent config.
    pub fn ensure_fingerprint(&self, fingerprint: u64) -> Result<()> {
        let want = format!("{fingerprint:016x}");
        if self.config_fingerprint != want {
            return Err(SieveError::Checkpoint(format!(
                "output was produced with config fingerprint {} but this run's is {want}; \
                 filter/dedup settings changed, so resuming would mix incompatible shards. \
                 Use a fresh output directory.",
                self.config_fingerprint
            )));
        }
        Ok(())
    }
}

const DELTA_MAGIC: &[u8; 4] = b"SVDD";

/// Write the fingerprints of a shard's kept docs. On resume these are loaded
/// back into the dedup index, so later shards still dedup against documents
/// kept before the crash. Append-only per shard: total restore cost is
/// O(kept docs), unlike snapshotting the whole index at every commit.
pub fn write_dedup_delta(path: &Path, bands: usize, fps: &[Fingerprint]) -> Result<()> {
    let tmp = tmp_path(path);
    {
        let f = File::create(&tmp).map_err(io_err(&tmp))?;
        let mut w = BufWriter::new(f);
        let mut put = |b: &[u8]| w.write_all(b).map_err(io_err(&tmp));
        put(DELTA_MAGIC)?;
        put(&(bands as u32).to_le_bytes())?;
        put(&(fps.len() as u64).to_le_bytes())?;
        for fp in fps {
            put(&fp.exact.to_le_bytes())?;
            for b in &fp.bands {
                put(&b.to_le_bytes())?;
            }
        }
        let f = w.into_inner().map_err(|e| io_err(&tmp)(e.into_error()))?;
        f.sync_all().map_err(io_err(&tmp))?;
    }
    commit_file(&tmp, path)
}

/// Read a shard's dedup delta.
pub fn read_dedup_delta(path: &Path) -> Result<Vec<Fingerprint>> {
    let f = File::open(path).map_err(io_err(path))?;
    let mut r = BufReader::new(f);
    let mut u64buf = [0u8; 8];
    let mut u32buf = [0u8; 4];
    let mut magic = [0u8; 4];
    let corrupt = |what: &str| SieveError::Checkpoint(format!("{}: {what}", path.display()));
    r.read_exact(&mut magic).map_err(io_err(path))?;
    if &magic != DELTA_MAGIC {
        return Err(corrupt("bad magic"));
    }
    r.read_exact(&mut u32buf).map_err(io_err(path))?;
    let bands = u32::from_le_bytes(u32buf) as usize;
    r.read_exact(&mut u64buf).map_err(io_err(path))?;
    let n = u64::from_le_bytes(u64buf) as usize;
    let mut out = Vec::with_capacity(n.min(1 << 24));
    for _ in 0..n {
        r.read_exact(&mut u64buf)
            .map_err(|_| corrupt("truncated"))?;
        let exact = u64::from_le_bytes(u64buf);
        let mut bs = Vec::with_capacity(bands);
        for _ in 0..bands {
            r.read_exact(&mut u64buf)
                .map_err(|_| corrupt("truncated"))?;
            bs.push(u64::from_le_bytes(u64buf));
        }
        out.push(Fingerprint { exact, bands: bs });
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn delta_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("d.bin");
        let fps = vec![
            Fingerprint {
                exact: 1,
                bands: vec![2, 3],
            },
            Fingerprint {
                exact: u64::MAX,
                bands: vec![0, 9],
            },
        ];
        write_dedup_delta(&p, 2, &fps).unwrap();
        assert_eq!(read_dedup_delta(&p).unwrap(), fps);
        assert!(!tmp_path(&p).exists());
    }

    #[test]
    fn truncated_delta_is_detected() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("d.bin");
        write_dedup_delta(
            &p,
            2,
            &[Fingerprint {
                exact: 1,
                bands: vec![2, 3],
            }],
        )
        .unwrap();
        let bytes = fs::read(&p).unwrap();
        fs::write(&p, &bytes[..bytes.len() - 3]).unwrap();
        assert!(read_dedup_delta(&p).is_err());
    }

    #[test]
    fn checkpoint_round_trip_and_fingerprint_guard() {
        let dir = tempfile::tempdir().unwrap();
        let layout = OutputLayout::create(dir.path()).unwrap();
        let mut ck = Checkpoint::new(0xabc);
        ck.shards.insert(
            "s3://x/a.warc.gz".into(),
            ShardRecord {
                kept: 5,
                ..Default::default()
            },
        );
        ck.save(&layout.checkpoint()).unwrap();
        let back = Checkpoint::load(&layout.checkpoint()).unwrap().unwrap();
        assert_eq!(back.shards["s3://x/a.warc.gz"].kept, 5);
        back.ensure_fingerprint(0xabc).unwrap();
        assert!(back.ensure_fingerprint(0xabd).is_err());
    }

    #[test]
    fn stale_tmp_files_are_removed() {
        let dir = tempfile::tempdir().unwrap();
        let layout = OutputLayout::create(dir.path()).unwrap();
        fs::write(tmp_path(&layout.data("x")), b"partial").unwrap();
        fs::write(layout.data("y"), b"committed").unwrap();
        assert_eq!(layout.remove_stale_tmp().unwrap(), 1);
        assert!(layout.data("y").exists());
    }
}
