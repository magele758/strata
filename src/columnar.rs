//! Async columnar learner. The engine feeds committed logical ops; this
//! module never reads heap pages.
//!
//! On disk, under the replica directory:
//! - `manifest.bin`: magic `STRM`, version, then a bincode manifest of
//!   `applied_lsn`, the data LSN high-water mark, and per-table watermarks.
//! - `seg-NNNN.parquet`: append-only Parquet segments of insert/delete events.
//!   Value vectors are bincode blobs so heterogeneous SQL types round-trip.

use std::collections::HashMap;
use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BinaryArray, BinaryBuilder, RecordBatch, UInt16Array, UInt32Array,
    UInt64Array, UInt8Array,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::arrow_writer::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use crate::types::{CommittedOp, LogicalOp};
use crate::value::Value;

const MANIFEST_NAME: &str = "manifest.bin";
const MANIFEST_MAGIC: &[u8; 4] = b"STRM";
const MANIFEST_VERSION: u32 = 1;
const KIND_INSERT: u8 = 1;
const KIND_DELETE: u8 = 2;
const WRITE_BATCH: usize = 8_192;

#[derive(Clone, Debug)]
struct Version {
    commit_ts: u64,
    delete_ts: Option<u64>,
    rid_page: u32,
    rid_slot: u16,
    values: Vec<Value>,
}

#[derive(Clone, Debug, Default)]
struct TableData {
    watermark: u64,
    flushed: bool,
    rows: Vec<Version>,
}

#[derive(Clone, Debug)]
struct Event {
    table_id: u32,
    kind: u8,
    lsn: u64,
    commit_ts: u64,
    rid_page: u32,
    rid_slot: u16,
    values: Vec<Value>,
}

#[derive(Serialize, Deserialize)]
struct Manifest {
    applied_lsn: u64,
    data_lsn: u64,
    next_seg: u64,
    watermarks: Vec<(u32, u64)>,
    segments: Vec<String>,
}

/// Columnar replica of committed logical ops. Safe to reopen after a crash:
/// a segment is visible only after the manifest that names it is durable.
#[derive(Debug)]
pub struct ColumnarReplica {
    dir: PathBuf,
    applied_lsn: u64,
    /// Highest op LSN whose row events are durable. Independent of
    /// `applied_lsn` so a crash between `apply` and `set_applied_lsn`
    /// does not duplicate rows on replay.
    data_lsn: u64,
    next_seg: u64,
    segments: Vec<String>,
    tables: HashMap<u32, TableData>,
    /// `set_applied_lsn` cannot return `Result`. A failed manifest write is
    /// remembered and retried on the next mutating call.
    manifest_error: Option<String>,
}

impl ColumnarReplica {
    pub fn open(dir: &Path) -> Result<Self> {
        std::fs::create_dir_all(dir)?;
        let manifest_path = dir.join(MANIFEST_NAME);
        let manifest = read_manifest(&manifest_path)?;
        let (applied_lsn, data_lsn, next_seg, segments, watermarks) = match manifest {
            Some(m) => (
                m.applied_lsn,
                m.data_lsn,
                m.next_seg,
                m.segments,
                m.watermarks,
            ),
            None => (0, 0, 0, Vec::new(), Vec::new()),
        };
        for name in &segments {
            if !segment_name_ok(name) {
                return Err(Error::Corrupt {
                    detail: format!("columnar segment name {name}"),
                });
            }
        }
        cleanup_orphans(dir, &segments)?;

        let mut tables: HashMap<u32, TableData> = HashMap::new();
        for (table_id, watermark) in watermarks {
            let table = tables.entry(table_id).or_default();
            table.flushed = true;
            if watermark > table.watermark {
                table.watermark = watermark;
            }
        }

        let mut replayed_lsn = 0u64;
        for name in &segments {
            let path = dir.join(name);
            if !path.is_file() {
                return Err(Error::Corrupt {
                    detail: format!("missing columnar segment {name}"),
                });
            }
            replay_segment(&path, &mut tables, &mut replayed_lsn)?;
        }

        Ok(Self {
            dir: dir.to_path_buf(),
            applied_lsn,
            data_lsn: data_lsn.max(replayed_lsn),
            next_seg,
            segments,
            tables,
            manifest_error: None,
        })
    }

    pub fn applied_lsn(&self) -> u64 {
        self.applied_lsn
    }

    pub fn set_applied_lsn(&mut self, lsn: u64) {
        self.applied_lsn = lsn;
        match self.write_manifest() {
            Ok(()) => self.manifest_error = None,
            Err(err) => self.manifest_error = Some(err.to_string()),
        }
    }

    pub fn watermark(&self, table_id: u32) -> u64 {
        self.tables
            .get(&table_id)
            .map(|table| table.watermark)
            .unwrap_or(0)
    }

    pub fn apply(&mut self, ops: &[CommittedOp]) -> Result<()> {
        self.retry_manifest()?;
        let floor = self.applied_lsn.max(self.data_lsn);
        let mut pending: Vec<&CommittedOp> = ops.iter().filter(|op| op.lsn > floor).collect();
        if pending.is_empty() {
            return Ok(());
        }
        pending.sort_by_key(|op| op.lsn);
        pending.dedup_by_key(|op| op.lsn);

        let mut tables = self.tables.clone();
        let mut events = Vec::new();
        let mut max_lsn = self.data_lsn;
        for op in &pending {
            max_lsn = max_lsn.max(op.lsn);
            let batch = events_for(op);
            for event in &batch {
                apply_event(&mut tables, event)?;
            }
            events.extend(batch);
        }
        if events.is_empty() {
            return Ok(());
        }

        let seg_id = self.next_seg;
        let name = format!("seg-{seg_id:016x}.parquet");
        let final_path = self.dir.join(&name);
        let tmp_path = self.dir.join(format!(".{name}.tmp"));
        if let Err(err) = write_segment_file(&tmp_path, &events) {
            let _ = std::fs::remove_file(&tmp_path);
            return Err(err);
        }
        if let Err(err) = sync_file(&tmp_path) {
            let _ = std::fs::remove_file(&tmp_path);
            return Err(err);
        }
        if let Err(err) = std::fs::rename(&tmp_path, &final_path) {
            let _ = std::fs::remove_file(&tmp_path);
            return Err(err.into());
        }

        let mut segments = self.segments.clone();
        segments.push(name);
        let saved = Saved {
            data_lsn: self.data_lsn,
            next_seg: self.next_seg,
            segments: std::mem::replace(&mut self.segments, segments),
            tables: std::mem::replace(&mut self.tables, tables),
        };
        self.data_lsn = max_lsn;
        self.next_seg = seg_id + 1;
        if let Err(err) = self.write_manifest() {
            self.data_lsn = saved.data_lsn;
            self.next_seg = saved.next_seg;
            self.segments = saved.segments;
            self.tables = saved.tables;
            let _ = std::fs::remove_file(&final_path);
            return Err(err);
        }
        Ok(())
    }

    pub fn scan(
        &self,
        table_id: u32,
        snapshot_ts: u64,
    ) -> Result<Option<Vec<Vec<Value>>>> {
        let Some(table) = self.tables.get(&table_id) else {
            return Ok(None);
        };
        if !table.flushed {
            return Ok(None);
        }
        let mut rows = Vec::new();
        for version in &table.rows {
            if visible(version, snapshot_ts) {
                rows.push(version.values.clone());
            }
        }
        Ok(Some(rows))
    }

    fn retry_manifest(&mut self) -> Result<()> {
        if self.manifest_error.is_none() {
            return Ok(());
        }
        self.write_manifest()?;
        self.manifest_error = None;
        Ok(())
    }

    fn write_manifest(&self) -> Result<()> {
        let manifest = Manifest {
            applied_lsn: self.applied_lsn,
            data_lsn: self.data_lsn,
            next_seg: self.next_seg,
            watermarks: watermark_pairs(&self.tables),
            segments: self.segments.clone(),
        };
        let payload = bincode::serialize(&manifest)?;
        let mut bytes = Vec::with_capacity(8 + payload.len());
        bytes.extend_from_slice(MANIFEST_MAGIC);
        bytes.extend_from_slice(&MANIFEST_VERSION.to_le_bytes());
        bytes.extend_from_slice(&payload);

        let tmp = self.dir.join(".manifest.bin.tmp");
        let final_path = self.dir.join(MANIFEST_NAME);
        {
            let mut file = File::create(&tmp)?;
            file.write_all(&bytes)?;
            file.sync_all()?;
        }
        std::fs::rename(&tmp, &final_path)?;
        sync_dir(&self.dir);
        Ok(())
    }
}

struct Saved {
    data_lsn: u64,
    next_seg: u64,
    segments: Vec<String>,
    tables: HashMap<u32, TableData>,
}

fn visible(version: &Version, snapshot_ts: u64) -> bool {
    if version.commit_ts > snapshot_ts {
        return false;
    }
    match version.delete_ts {
        Some(delete_ts) if delete_ts <= snapshot_ts => false,
        _ => true,
    }
}

fn touch(tables: &mut HashMap<u32, TableData>, table_id: u32, commit_ts: u64) -> &mut TableData {
    let table = tables.entry(table_id).or_default();
    table.flushed = true;
    if commit_ts > table.watermark {
        table.watermark = commit_ts;
    }
    table
}

fn mark_deleted(table: &mut TableData, page: u32, slot: u16, ts: u64) {
    let mut best: Option<usize> = None;
    for (i, row) in table.rows.iter().enumerate() {
        if row.rid_page != page || row.rid_slot != slot || row.delete_ts.is_some() {
            continue;
        }
        if row.commit_ts > ts {
            continue;
        }
        let replace = match best {
            None => true,
            Some(prev) => row.commit_ts >= table.rows[prev].commit_ts,
        };
        if replace {
            best = Some(i);
        }
    }
    if let Some(i) = best {
        table.rows[i].delete_ts = Some(ts);
    }
}

fn apply_event(tables: &mut HashMap<u32, TableData>, event: &Event) -> Result<()> {
    match event.kind {
        KIND_INSERT => {
            let table = touch(tables, event.table_id, event.commit_ts);
            table.rows.push(Version {
                commit_ts: event.commit_ts,
                delete_ts: None,
                rid_page: event.rid_page,
                rid_slot: event.rid_slot,
                values: event.values.clone(),
            });
            Ok(())
        }
        KIND_DELETE => {
            let table = touch(tables, event.table_id, event.commit_ts);
            mark_deleted(table, event.rid_page, event.rid_slot, event.commit_ts);
            Ok(())
        }
        other => Err(Error::Corrupt {
            detail: format!("columnar segment kind {other}"),
        }),
    }
}

fn events_for(op: &CommittedOp) -> Vec<Event> {
    match &op.op {
        LogicalOp::Insert {
            table_id,
            rid_page,
            rid_slot,
            values,
        } => vec![Event {
            table_id: *table_id,
            kind: KIND_INSERT,
            lsn: op.lsn,
            commit_ts: op.commit_ts,
            rid_page: *rid_page,
            rid_slot: *rid_slot,
            values: values.clone(),
        }],
        LogicalOp::Delete {
            table_id,
            rid_page,
            rid_slot,
            values,
        } => vec![Event {
            table_id: *table_id,
            kind: KIND_DELETE,
            lsn: op.lsn,
            commit_ts: op.commit_ts,
            rid_page: *rid_page,
            rid_slot: *rid_slot,
            values: values.clone(),
        }],
        LogicalOp::Update {
            table_id,
            old_page,
            old_slot,
            new_page,
            new_slot,
            values,
        } => vec![
            Event {
                table_id: *table_id,
                kind: KIND_DELETE,
                lsn: op.lsn,
                commit_ts: op.commit_ts,
                rid_page: *old_page,
                rid_slot: *old_slot,
                values: Vec::new(),
            },
            Event {
                table_id: *table_id,
                kind: KIND_INSERT,
                lsn: op.lsn,
                commit_ts: op.commit_ts,
                rid_page: *new_page,
                rid_slot: *new_slot,
                values: values.clone(),
            },
        ],
    }
}

fn watermark_pairs(tables: &HashMap<u32, TableData>) -> Vec<(u32, u64)> {
    let mut pairs: Vec<(u32, u64)> = tables
        .iter()
        .filter(|(_, table)| table.flushed)
        .map(|(id, table)| (*id, table.watermark))
        .collect();
    pairs.sort_unstable_by_key(|pair| pair.0);
    pairs
}

fn segment_name_ok(name: &str) -> bool {
    let Some(rest) = name.strip_prefix("seg-") else {
        return false;
    };
    let Some(hex) = rest.strip_suffix(".parquet") else {
        return false;
    };
    !hex.is_empty()
        && hex.len() <= 16
        && hex.bytes().all(|b| b.is_ascii_hexdigit())
        && !name.contains('/')
        && !name.contains('\\')
}

fn segment_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("table_id", DataType::UInt32, false),
        Field::new("kind", DataType::UInt8, false),
        Field::new("lsn", DataType::UInt64, false),
        Field::new("commit_ts", DataType::UInt64, false),
        Field::new("rid_page", DataType::UInt32, false),
        Field::new("rid_slot", DataType::UInt16, false),
        Field::new("values", DataType::Binary, false),
    ]))
}

fn events_to_batch(schema: &SchemaRef, events: &[Event]) -> Result<RecordBatch> {
    let mut table_ids = Vec::with_capacity(events.len());
    let mut kinds = Vec::with_capacity(events.len());
    let mut lsns = Vec::with_capacity(events.len());
    let mut commit_ts = Vec::with_capacity(events.len());
    let mut rid_pages = Vec::with_capacity(events.len());
    let mut rid_slots = Vec::with_capacity(events.len());
    let mut values_builder = BinaryBuilder::with_capacity(events.len(), events.len() * 64);
    for event in events {
        table_ids.push(event.table_id);
        kinds.push(event.kind);
        lsns.push(event.lsn);
        commit_ts.push(event.commit_ts);
        rid_pages.push(event.rid_page);
        rid_slots.push(event.rid_slot);
        let encoded = bincode::serialize(&event.values)?;
        values_builder.append_value(&encoded);
    }
    let columns: Vec<ArrayRef> = vec![
        Arc::new(UInt32Array::from(table_ids)),
        Arc::new(UInt8Array::from(kinds)),
        Arc::new(UInt64Array::from(lsns)),
        Arc::new(UInt64Array::from(commit_ts)),
        Arc::new(UInt32Array::from(rid_pages)),
        Arc::new(UInt16Array::from(rid_slots)),
        Arc::new(values_builder.finish()),
    ];
    RecordBatch::try_new(Arc::clone(schema), columns).map_err(ext_err)
}

fn write_segment_file(path: &Path, events: &[Event]) -> Result<()> {
    let schema = segment_schema();
    let file = File::create(path)?;
    let props = WriterProperties::builder()
        .set_dictionary_enabled(false)
        .set_compression(Compression::UNCOMPRESSED)
        .build();
    let mut writer = ArrowWriter::try_new(file, Arc::clone(&schema), Some(props)).map_err(ext_err)?;
    for chunk in events.chunks(WRITE_BATCH) {
        let batch = events_to_batch(&schema, chunk)?;
        writer.write(&batch).map_err(ext_err)?;
    }
    writer.close().map_err(ext_err)?;
    Ok(())
}

fn replay_segment(
    path: &Path,
    tables: &mut HashMap<u32, TableData>,
    max_lsn: &mut u64,
) -> Result<()> {
    let file = File::open(path)?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(ext_err)?
        .build()
        .map_err(ext_err)?;
    for batch in reader {
        let batch = batch.map_err(ext_err)?;
        ingest_batch(&batch, tables, max_lsn)?;
    }
    Ok(())
}

fn ingest_batch(
    batch: &RecordBatch,
    tables: &mut HashMap<u32, TableData>,
    max_lsn: &mut u64,
) -> Result<()> {
    let n = batch.num_rows();
    let table_ids = primitive::<UInt32Array>(batch, "table_id", n)?;
    let kinds = primitive::<UInt8Array>(batch, "kind", n)?;
    let lsns = primitive::<UInt64Array>(batch, "lsn", n)?;
    let commit_ts = primitive::<UInt64Array>(batch, "commit_ts", n)?;
    let rid_pages = primitive::<UInt32Array>(batch, "rid_page", n)?;
    let rid_slots = primitive::<UInt16Array>(batch, "rid_slot", n)?;
    let values = binary_col(batch, "values", n)?;
    for i in 0..n {
        ensure_present(values, i, "values")?;
        let decoded: Vec<Value> = bincode::deserialize(values.value(i))?;
        ensure_present(lsns, i, "lsn")?;
        let lsn = lsns.value(i);
        if lsn > *max_lsn {
            *max_lsn = lsn;
        }
        ensure_present(table_ids, i, "table_id")?;
        ensure_present(kinds, i, "kind")?;
        ensure_present(commit_ts, i, "commit_ts")?;
        ensure_present(rid_pages, i, "rid_page")?;
        ensure_present(rid_slots, i, "rid_slot")?;
        let event = Event {
            table_id: table_ids.value(i),
            kind: kinds.value(i),
            lsn,
            commit_ts: commit_ts.value(i),
            rid_page: rid_pages.value(i),
            rid_slot: rid_slots.value(i),
            values: decoded,
        };
        apply_event(tables, &event)?;
    }
    Ok(())
}

fn primitive<'a, T>(batch: &'a RecordBatch, name: &str, n: usize) -> Result<&'a T>
where
    T: Array + 'static,
{
    let idx = batch
        .schema()
        .index_of(name)
        .map_err(|err| Error::Corrupt {
            detail: format!("columnar segment missing {name}: {err}"),
        })?;
    let col = batch.column(idx);
    if col.len() != n {
        return Err(Error::Corrupt {
            detail: format!("columnar segment column {name} length"),
        });
    }
    col.as_any().downcast_ref::<T>().ok_or_else(|| Error::Corrupt {
        detail: format!("columnar segment column {name} type {}", col.data_type()),
    })
}

fn binary_col<'a>(batch: &'a RecordBatch, name: &str, n: usize) -> Result<&'a BinaryArray> {
    primitive::<BinaryArray>(batch, name, n)
}

fn ensure_present(arr: &dyn Array, i: usize, name: &str) -> Result<()> {
    if arr.is_null(i) {
        return Err(Error::Corrupt {
            detail: format!("null {name} in columnar segment"),
        });
    }
    Ok(())
}

fn read_manifest(path: &Path) -> Result<Option<Manifest>> {
    if !path.exists() {
        return Ok(None);
    }
    let bytes = std::fs::read(path)?;
    if bytes.len() < 8 || &bytes[0..4] != MANIFEST_MAGIC {
        return Err(Error::Corrupt {
            detail: "columnar manifest magic".into(),
        });
    }
    let version = u32::from_le_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]);
    if version != MANIFEST_VERSION {
        return Err(Error::Corrupt {
            detail: format!("columnar manifest version {version}"),
        });
    }
    let manifest: Manifest = bincode::deserialize(&bytes[8..])?;
    Ok(Some(manifest))
}

fn cleanup_orphans(dir: &Path, keep: &[String]) -> Result<()> {
    let kept: std::collections::HashSet<&str> = keep.iter().map(|s| s.as_str()).collect();
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        let drop_tmp = name.starts_with('.') && name.ends_with(".tmp");
        let drop_seg = name.starts_with("seg-") && name.ends_with(".parquet") && !kept.contains(name);
        if drop_tmp || drop_seg {
            let _ = std::fs::remove_file(entry.path());
        }
    }
    Ok(())
}

fn sync_file(path: &Path) -> Result<()> {
    let file = File::open(path)?;
    file.sync_all()?;
    Ok(())
}

fn sync_dir(dir: &Path) {
    if let Ok(file) = File::open(dir) {
        let _ = file.sync_all();
    }
}

fn ext_err(err: impl std::fmt::Display) -> Error {
    Error::Corrupt {
        detail: err.to_string(),
    }
}
