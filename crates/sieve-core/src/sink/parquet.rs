//! Parquet writers for kept documents and rejects.
//!
//! Columnar + zstd is the standard interchange format for LLM pre-training data
//! (FineWeb, Dolma, RedPajama all ship Parquet): readers can scan only the
//! columns they need (e.g. `lang` and `word_count` to plan a mixture without
//! touching `text`), and per-column compression works far better on homogeneous
//! values than row-wise gzip JSONL.

use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::{
    ArrayRef, Float32Builder, Float64Builder, StringBuilder, TimestampMillisecondBuilder,
    UInt32Builder,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;

use crate::config::SinkConfig;
use crate::doc::{CleanDoc, Drop};
use crate::error::{io_err, Result};

/// Schema of the kept-documents files.
pub fn data_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("url", DataType::Utf8, false),
        Field::new("record_id", DataType::Utf8, false),
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
            false,
        ),
        Field::new("lang", DataType::Utf8, true),
        Field::new("lang_score", DataType::Float32, false),
        Field::new("cc_lang", DataType::Utf8, true),
        Field::new("text", DataType::Utf8, false),
        Field::new("word_count", DataType::UInt32, false),
        Field::new("mean_word_len", DataType::Float32, false),
        Field::new("symbol_word_ratio", DataType::Float32, false),
        Field::new("dup_line_frac", DataType::Float32, false),
        Field::new("dup_para_frac", DataType::Float32, false),
        Field::new("alpha_words_frac", DataType::Float32, false),
        Field::new("stop_words", DataType::UInt32, false),
        Field::new("c4_lines_removed", DataType::UInt32, false),
        Field::new("pii_redactions", DataType::UInt32, false),
        Field::new("source_shard", DataType::Utf8, false),
    ]))
}

/// Schema of the rejects files: enough to audit any filter decision.
pub fn rejects_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("url", DataType::Utf8, false),
        Field::new("record_id", DataType::Utf8, false),
        Field::new("rule", DataType::Utf8, false),
        Field::new("value", DataType::Float64, true),
    ]))
}

fn open(path: &Path, schema: SchemaRef, cfg: &SinkConfig, level: i32) -> Result<ArrowWriter<File>> {
    let file = File::create(path).map_err(io_err(path))?;
    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(level)?))
        // Size-based row groups: a fixed row count would make memory depend on
        // document length, which varies by two orders of magnitude.
        .set_max_row_group_bytes(Some(cfg.row_group_bytes.max(1)))
        .set_max_row_group_row_count(None)
        .build();
    Ok(ArrowWriter::try_new(file, schema, Some(props))?)
}

/// Flush the Arrow builders once they hold this much text, so a batch's Utf8
/// offsets (i32) can never overflow regardless of `batch_rows`.
const MAX_BATCH_TEXT_BYTES: usize = 64 * 1024 * 1024;

/// Streams [`CleanDoc`]s into a temporary Parquet file.
pub struct DataWriter {
    tmp: PathBuf,
    writer: ArrowWriter<File>,
    batch_rows: usize,
    text_bytes: usize,
    rows: u64,
    url: StringBuilder,
    record_id: StringBuilder,
    ts: TimestampMillisecondBuilder,
    lang: StringBuilder,
    lang_score: Float32Builder,
    cc_lang: StringBuilder,
    text: StringBuilder,
    word_count: UInt32Builder,
    mean_word_len: Float32Builder,
    symbol_word_ratio: Float32Builder,
    dup_line_frac: Float32Builder,
    dup_para_frac: Float32Builder,
    alpha_words_frac: Float32Builder,
    stop_words: UInt32Builder,
    c4_lines_removed: UInt32Builder,
    pii: UInt32Builder,
    source: StringBuilder,
    pending: usize,
}

impl DataWriter {
    /// Create the temp file.
    pub fn create(tmp: PathBuf, cfg: &SinkConfig) -> Result<Self> {
        let writer = open(&tmp, data_schema(), cfg, cfg.zstd_level)?;
        Ok(Self {
            tmp,
            writer,
            batch_rows: cfg.batch_rows.max(1),
            text_bytes: 0,
            rows: 0,
            url: StringBuilder::new(),
            record_id: StringBuilder::new(),
            ts: TimestampMillisecondBuilder::new().with_timezone("UTC"),
            lang: StringBuilder::new(),
            lang_score: Float32Builder::new(),
            cc_lang: StringBuilder::new(),
            text: StringBuilder::new(),
            word_count: UInt32Builder::new(),
            mean_word_len: Float32Builder::new(),
            symbol_word_ratio: Float32Builder::new(),
            dup_line_frac: Float32Builder::new(),
            dup_para_frac: Float32Builder::new(),
            alpha_words_frac: Float32Builder::new(),
            stop_words: UInt32Builder::new(),
            c4_lines_removed: UInt32Builder::new(),
            pii: UInt32Builder::new(),
            source: StringBuilder::new(),
            pending: 0,
        })
    }

    /// Append one document.
    pub fn push(&mut self, d: &CleanDoc, source: &str) -> Result<()> {
        let s = &d.signals;
        self.url.append_value(&d.meta.url);
        self.record_id.append_value(&d.meta.record_id);
        self.ts.append_value(d.meta.timestamp_ms);
        self.lang.append_option(s.lang.as_deref());
        self.lang_score.append_value(s.lang_score);
        self.cc_lang.append_option(d.meta.cc_lang.as_deref());
        self.text.append_value(&d.text);
        self.word_count.append_value(s.word_count);
        self.mean_word_len.append_value(s.mean_word_len);
        self.symbol_word_ratio.append_value(s.symbol_word_ratio);
        self.dup_line_frac.append_value(s.dup_line_frac);
        self.dup_para_frac.append_value(s.dup_para_frac);
        self.alpha_words_frac.append_value(s.alpha_words_frac);
        self.stop_words.append_value(s.stop_words);
        self.c4_lines_removed.append_value(s.c4_lines_removed);
        self.pii.append_value(d.pii_redactions);
        self.source.append_value(source);
        self.pending += 1;
        self.text_bytes += d.text.len();
        if self.pending >= self.batch_rows || self.text_bytes >= MAX_BATCH_TEXT_BYTES {
            self.flush_batch()?;
        }
        Ok(())
    }

    fn flush_batch(&mut self) -> Result<()> {
        if self.pending == 0 {
            return Ok(());
        }
        let cols: Vec<ArrayRef> = vec![
            Arc::new(self.url.finish()),
            Arc::new(self.record_id.finish()),
            Arc::new(self.ts.finish()),
            Arc::new(self.lang.finish()),
            Arc::new(self.lang_score.finish()),
            Arc::new(self.cc_lang.finish()),
            Arc::new(self.text.finish()),
            Arc::new(self.word_count.finish()),
            Arc::new(self.mean_word_len.finish()),
            Arc::new(self.symbol_word_ratio.finish()),
            Arc::new(self.dup_line_frac.finish()),
            Arc::new(self.dup_para_frac.finish()),
            Arc::new(self.alpha_words_frac.finish()),
            Arc::new(self.stop_words.finish()),
            Arc::new(self.c4_lines_removed.finish()),
            Arc::new(self.pii.finish()),
            Arc::new(self.source.finish()),
        ];
        let batch = RecordBatch::try_new(data_schema(), cols)?;
        self.writer.write(&batch)?;
        self.rows += self.pending as u64;
        self.pending = 0;
        self.text_bytes = 0;
        Ok(())
    }

    /// Write the footer and fsync. Returns (temp path, rows). The caller renames.
    pub fn finish(mut self) -> Result<(PathBuf, u64)> {
        self.flush_batch()?;
        self.writer.finish()?;
        self.writer.inner().sync_all().map_err(io_err(&self.tmp))?;
        Ok((self.tmp, self.rows))
    }
}

/// Streams [`Drop`]s into a temporary Parquet file.
pub struct RejectsWriter {
    tmp: PathBuf,
    writer: ArrowWriter<File>,
    batch_rows: usize,
    url: StringBuilder,
    record_id: StringBuilder,
    rule: StringBuilder,
    value: Float64Builder,
    pending: usize,
    rows: u64,
}

impl RejectsWriter {
    /// Create the temp file.
    pub fn create(tmp: PathBuf, cfg: &SinkConfig) -> Result<Self> {
        // Rejects are small and written once; a cheap zstd level is plenty.
        let writer = open(&tmp, rejects_schema(), cfg, 1)?;
        Ok(Self {
            tmp,
            writer,
            batch_rows: cfg.batch_rows.max(1) * 4,
            url: StringBuilder::new(),
            record_id: StringBuilder::new(),
            rule: StringBuilder::new(),
            value: Float64Builder::new(),
            pending: 0,
            rows: 0,
        })
    }

    /// Append one rejection.
    pub fn push(&mut self, d: &Drop) -> Result<()> {
        self.url.append_value(&d.meta.url);
        self.record_id.append_value(&d.meta.record_id);
        self.rule.append_value(d.rule);
        if d.value.is_nan() {
            self.value.append_null();
        } else {
            self.value.append_value(d.value);
        }
        self.pending += 1;
        if self.pending >= self.batch_rows {
            self.flush_batch()?;
        }
        Ok(())
    }

    fn flush_batch(&mut self) -> Result<()> {
        if self.pending == 0 {
            return Ok(());
        }
        let cols: Vec<ArrayRef> = vec![
            Arc::new(self.url.finish()),
            Arc::new(self.record_id.finish()),
            Arc::new(self.rule.finish()),
            Arc::new(self.value.finish()),
        ];
        self.writer
            .write(&RecordBatch::try_new(rejects_schema(), cols)?)?;
        self.rows += self.pending as u64;
        self.pending = 0;
        Ok(())
    }

    /// Write the footer and fsync. Returns (temp path, rows).
    pub fn finish(mut self) -> Result<(PathBuf, u64)> {
        self.flush_batch()?;
        self.writer.finish()?;
        self.writer.inner().sync_all().map_err(io_err(&self.tmp))?;
        Ok((self.tmp, self.rows))
    }
}
