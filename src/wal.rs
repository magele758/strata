use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use crate::error::{Error, Result};
use crate::types::{CommittedOp, LogicalOp, PAGE_SIZE, WAL_MAGIC, FORMAT_VERSION};

const WAL_HEADER: u64 = 32;
const REC_HEADER: usize = 24;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WalKind {
    Mutation = 1,
    Commit = 2,
    Abort = 3,
    Checkpoint = 4,
}

impl WalKind {
    fn from_u16(v: u16) -> Result<Self> {
        match v {
            1 => Ok(Self::Mutation),
            2 => Ok(Self::Commit),
            3 => Ok(Self::Abort),
            4 => Ok(Self::Checkpoint),
            other => Err(Error::Corrupt {
                detail: format!("bad wal kind {other}"),
            }),
        }
    }
}

#[derive(Clone, Debug)]
pub struct PageImage {
    pub page_id: u32,
    pub before: Vec<u8>,
    pub after: Vec<u8>,
}

#[derive(Clone, Debug)]
pub struct WalRecord {
    pub lsn: u64,
    pub kind: WalKind,
    pub txn_id: u64,
    pub commit_ts: u64,
    pub pages: Vec<PageImage>,
    pub catalog_before: Vec<u8>,
    pub catalog_after: Vec<u8>,
    pub logical: Option<LogicalOp>,
    pub ops: Vec<LogicalOp>,
}

pub struct Wal {
    file: File,
    path: PathBuf,
    next_lsn: u64,
    flushed_lsn: u64,
    syncs: u64,
}

impl Wal {
    pub fn create(path: &Path) -> Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(path)?;
        let mut wal = Self {
            file,
            path: path.to_path_buf(),
            next_lsn: 1,
            flushed_lsn: 0,
            syncs: 0,
        };
        wal.write_header()?;
        wal.file.sync_all()?;
        Ok(wal)
    }

    pub fn open(path: &Path) -> Result<(Self, Vec<WalRecord>)> {
        let mut file = OpenOptions::new().read(true).write(true).open(path)?;
        let mut header = [0u8; WAL_HEADER as usize];
        file.read_exact(&mut header)?;
        let magic = u32::from_le_bytes(header[0..4].try_into().unwrap());
        if magic != WAL_MAGIC {
            return Err(Error::Corrupt {
                detail: format!("bad wal magic {magic:#x}"),
            });
        }
        let mut records = Vec::new();
        let mut cursor = WAL_HEADER;
        let file_len = file.metadata()?.len();
        loop {
            if cursor + REC_HEADER as u64 > file_len {
                break;
            }
            file.seek(SeekFrom::Start(cursor))?;
            let mut hdr = [0u8; REC_HEADER];
            if file.read_exact(&mut hdr).is_err() {
                break;
            }
            let payload_len = u32::from_le_bytes(hdr[0..4].try_into().unwrap()) as u64;
            let stored_crc = u32::from_le_bytes(hdr[4..8].try_into().unwrap());
            let lsn = u64::from_le_bytes(hdr[8..16].try_into().unwrap());
            let kind = u16::from_le_bytes(hdr[16..18].try_into().unwrap());
            let total = cursor + REC_HEADER as u64 + payload_len;
            if total > file_len {
                break;
            }
            let mut payload = vec![0u8; payload_len as usize];
            if file.read_exact(&mut payload).is_err() {
                break;
            }
            let crc = record_crc(lsn, kind, &payload);
            if crc != stored_crc {
                break;
            }
            let kind = match WalKind::from_u16(kind) {
                Ok(k) => k,
                Err(_) => break,
            };
            let rec = match decode_record(lsn, kind, &payload) {
                Ok(r) => r,
                Err(_) => break,
            };
            records.push(rec);
            cursor = total;
        }
        if cursor < file_len {
            file.set_len(cursor)?;
            file.sync_all()?;
        }
        let next_lsn = records.last().map(|r| r.lsn + 1).unwrap_or(1);
        let wal = Self {
            file,
            path: path.to_path_buf(),
            next_lsn,
            flushed_lsn: records.last().map(|r| r.lsn).unwrap_or(0),
            syncs: 0,
        };
        Ok((wal, records))
    }

    pub fn append(&mut self, rec: &WalRecord) -> Result<u64> {
        let lsn = self.next_lsn;
        let payload = encode_payload(rec)?;
        let kind = rec.kind as u16;
        let crc = record_crc(lsn, kind, &payload);
        let mut hdr = [0u8; REC_HEADER];
        hdr[0..4].copy_from_slice(&(payload.len() as u32).to_le_bytes());
        hdr[4..8].copy_from_slice(&crc.to_le_bytes());
        hdr[8..16].copy_from_slice(&lsn.to_le_bytes());
        hdr[16..18].copy_from_slice(&kind.to_le_bytes());
        self.file.seek(SeekFrom::End(0))?;
        self.file.write_all(&hdr)?;
        self.file.write_all(&payload)?;
        self.next_lsn += 1;
        Ok(lsn)
    }

    pub fn flush(&mut self) -> Result<()> {
        self.file.sync_all()?;
        self.flushed_lsn = self.next_lsn.saturating_sub(1);
        self.syncs += 1;
        Ok(())
    }

    pub fn flushed_lsn(&self) -> u64 {
        self.flushed_lsn
    }

    pub fn syncs(&self) -> u64 {
        self.syncs
    }

    pub fn next_lsn(&self) -> u64 {
        self.next_lsn
    }

    /// Replace the log with a single checkpoint record. LSN keeps increasing.
    pub fn recycle_checkpoint(&mut self, catalog: &[u8]) -> Result<u64> {
        let lsn = self.next_lsn;
        let rec = WalRecord {
            lsn,
            kind: WalKind::Checkpoint,
            txn_id: 0,
            commit_ts: 0,
            pages: Vec::new(),
            catalog_before: Vec::new(),
            catalog_after: catalog.to_vec(),
            logical: None,
            ops: Vec::new(),
        };
        let tmp = self.path.with_extension("log.new");
        {
            let mut fresh = Wal::create(&tmp)?;
            fresh.next_lsn = lsn;
            fresh.append(&rec)?;
            fresh.flush()?;
        }
        std::fs::rename(&tmp, &self.path)?;
        self.file = OpenOptions::new().read(true).write(true).open(&self.path)?;
        self.next_lsn = lsn + 1;
        self.flushed_lsn = lsn;
        Ok(lsn)
    }

    fn write_header(&mut self) -> Result<()> {
        let mut hdr = [0u8; WAL_HEADER as usize];
        hdr[0..4].copy_from_slice(&WAL_MAGIC.to_le_bytes());
        hdr[4..8].copy_from_slice(&FORMAT_VERSION.to_le_bytes());
        self.file.seek(SeekFrom::Start(0))?;
        self.file.write_all(&hdr)?;
        Ok(())
    }
}

pub fn wal_path(dir: &Path) -> PathBuf {
    dir.join("wal.log")
}

fn record_crc(lsn: u64, kind: u16, payload: &[u8]) -> u32 {
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(&lsn.to_le_bytes());
    hasher.update(&kind.to_le_bytes());
    hasher.update(payload);
    hasher.finalize()
}

fn encode_payload(rec: &WalRecord) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    match rec.kind {
        WalKind::Mutation => {
            out.extend_from_slice(&rec.txn_id.to_le_bytes());
            out.extend_from_slice(&(rec.pages.len() as u32).to_le_bytes());
            for page in &rec.pages {
                if page.before.len() != PAGE_SIZE || page.after.len() != PAGE_SIZE {
                    return Err(Error::Corrupt {
                        detail: "page image is not 4096 bytes".into(),
                    });
                }
                out.extend_from_slice(&page.page_id.to_le_bytes());
                out.extend_from_slice(&page.before);
                out.extend_from_slice(&page.after);
            }
            write_blob(&mut out, &rec.catalog_before);
            write_blob(&mut out, &rec.catalog_after);
            match &rec.logical {
                None => out.extend_from_slice(&0u32.to_le_bytes()),
                Some(op) => {
                    let bytes = bincode::serialize(op)?;
                    out.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
                    out.extend_from_slice(&bytes);
                }
            }
        }
        WalKind::Commit => {
            out.extend_from_slice(&rec.txn_id.to_le_bytes());
            out.extend_from_slice(&rec.commit_ts.to_le_bytes());
            out.extend_from_slice(&(rec.ops.len() as u32).to_le_bytes());
            for op in &rec.ops {
                let bytes = bincode::serialize(op)?;
                write_blob(&mut out, &bytes);
            }
        }
        WalKind::Abort => {
            out.extend_from_slice(&rec.txn_id.to_le_bytes());
        }
        WalKind::Checkpoint => {
            write_blob(&mut out, &rec.catalog_after);
        }
    }
    Ok(out)
}

fn decode_record(lsn: u64, kind: WalKind, payload: &[u8]) -> Result<WalRecord> {
    let mut rec = WalRecord {
        lsn,
        kind,
        txn_id: 0,
        commit_ts: 0,
        pages: Vec::new(),
        catalog_before: Vec::new(),
        catalog_after: Vec::new(),
        logical: None,
        ops: Vec::new(),
    };
    let mut at = 0;
    match kind {
        WalKind::Mutation => {
            rec.txn_id = take_u64(payload, &mut at)?;
            let n = take_u32(payload, &mut at)? as usize;
            for _ in 0..n {
                let page_id = take_u32(payload, &mut at)?;
                let before = take_exact(payload, &mut at, PAGE_SIZE)?;
                let after = take_exact(payload, &mut at, PAGE_SIZE)?;
                rec.pages.push(PageImage {
                    page_id,
                    before,
                    after,
                });
            }
            rec.catalog_before = take_blob(payload, &mut at)?;
            rec.catalog_after = take_blob(payload, &mut at)?;
            let logical_len = take_u32(payload, &mut at)? as usize;
            if logical_len > 0 {
                let bytes = take_exact(payload, &mut at, logical_len)?;
                rec.logical = Some(bincode::deserialize(&bytes)?);
            }
        }
        WalKind::Commit => {
            rec.txn_id = take_u64(payload, &mut at)?;
            rec.commit_ts = take_u64(payload, &mut at)?;
            let n = take_u32(payload, &mut at)?;
            for _ in 0..n {
                let bytes = take_blob(payload, &mut at)?;
                rec.ops.push(bincode::deserialize(&bytes)?);
            }
        }
        WalKind::Abort => {
            rec.txn_id = take_u64(payload, &mut at)?;
        }
        WalKind::Checkpoint => {
            rec.catalog_after = take_blob(payload, &mut at)?;
        }
    }
    Ok(rec)
}

pub fn commit_ops(records: &[WalRecord], after_lsn: u64) -> Vec<CommittedOp> {
    let mut out = Vec::new();
    for rec in records {
        if rec.kind != WalKind::Commit || rec.lsn <= after_lsn {
            continue;
        }
        for op in &rec.ops {
            out.push(CommittedOp {
                lsn: rec.lsn,
                commit_ts: rec.commit_ts,
                op: op.clone(),
            });
        }
    }
    out
}

fn write_blob(out: &mut Vec<u8>, bytes: &[u8]) {
    out.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
    out.extend_from_slice(bytes);
}

fn take_u32(buf: &[u8], at: &mut usize) -> Result<u32> {
    let bytes = take_exact(buf, at, 4)?;
    Ok(u32::from_le_bytes(bytes.try_into().unwrap()))
}

fn take_u64(buf: &[u8], at: &mut usize) -> Result<u64> {
    let bytes = take_exact(buf, at, 8)?;
    Ok(u64::from_le_bytes(bytes.try_into().unwrap()))
}

fn take_blob(buf: &[u8], at: &mut usize) -> Result<Vec<u8>> {
    let n = take_u32(buf, at)? as usize;
    take_exact(buf, at, n)
}

fn take_exact(buf: &[u8], at: &mut usize, n: usize) -> Result<Vec<u8>> {
    let end = at.checked_add(n).ok_or_else(|| Error::Corrupt {
        detail: "wal overflow".into(),
    })?;
    if end > buf.len() {
        return Err(Error::Corrupt {
            detail: "truncated wal payload".into(),
        });
    }
    let out = buf[*at..end].to_vec();
    *at = end;
    Ok(out)
}
