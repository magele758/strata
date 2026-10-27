use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use crate::value::Value;

pub const PAGE_SIZE: usize = 4096;
pub const HEADER_LEN: usize = 32;
pub const PAYLOAD_LEN: usize = PAGE_SIZE - HEADER_LEN;
pub const MAGIC: u32 = 0x5354_5241;
pub const WAL_MAGIC: u32 = 0x5354_524C;
pub const FORMAT_VERSION: u32 = 1;
pub const MAX_TUPLE: usize = 8 * 1024 * 1024;
pub const MAX_KEY: usize = 1024;
pub const MAX_INDEX_VAL: usize = 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PageId(pub u32);

impl PageId {
    pub const META: PageId = PageId(0);
    pub fn is_none(self) -> bool {
        self.0 == 0
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Rid {
    pub page: u32,
    pub slot: u16,
}

impl Rid {
    pub fn encode(self) -> [u8; 6] {
        let mut out = [0u8; 6];
        out[..4].copy_from_slice(&self.page.to_le_bytes());
        out[4..].copy_from_slice(&self.slot.to_le_bytes());
        out
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < 6 {
            return Err(Error::Corrupt {
                detail: "short rid".into(),
            });
        }
        let page = u32::from_le_bytes(bytes[0..4].try_into().unwrap());
        let slot = u16::from_le_bytes(bytes[4..6].try_into().unwrap());
        Ok(Rid { page, slot })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u16)]
pub enum PageKind {
    Meta = 1,
    Freelist = 2,
    Heap = 3,
    BtreeLeaf = 4,
    BtreeInternal = 5,
    Overflow = 6,
    Inverted = 7,
    Hnsw = 8,
}

impl PageKind {
    pub fn from_u16(v: u16) -> Result<Self> {
        Ok(match v {
            1 => Self::Meta,
            2 => Self::Freelist,
            3 => Self::Heap,
            4 => Self::BtreeLeaf,
            5 => Self::BtreeInternal,
            6 => Self::Overflow,
            7 => Self::Inverted,
            8 => Self::Hnsw,
            other => {
                return Err(Error::Corrupt {
                    detail: format!("unknown page kind {other}"),
                })
            }
        })
    }
}

#[derive(Clone, Debug)]
pub struct Page {
    bytes: [u8; PAGE_SIZE],
}

impl Page {
    pub fn blank(id: PageId, kind: PageKind) -> Self {
        let mut page = Self {
            bytes: [0; PAGE_SIZE],
        };
        page.bytes[0..4].copy_from_slice(&MAGIC.to_le_bytes());
        page.set_id(id);
        page.set_kind(kind);
        page
    }

    pub fn duplicate(&self) -> Self {
        Self { bytes: self.bytes }
    }

    pub fn from_sealed(bytes: [u8; PAGE_SIZE]) -> Result<Self> {
        let page = Self { bytes };
        page.verify()?;
        Ok(page)
    }

    pub fn as_bytes(&self) -> &[u8; PAGE_SIZE] {
        &self.bytes
    }

    pub fn id(&self) -> PageId {
        PageId(u32::from_le_bytes(self.bytes[8..12].try_into().unwrap()))
    }

    pub fn set_id(&mut self, id: PageId) {
        self.bytes[8..12].copy_from_slice(&id.0.to_le_bytes());
    }

    pub fn kind(&self) -> Result<PageKind> {
        PageKind::from_u16(u16::from_le_bytes(self.bytes[12..14].try_into().unwrap()))
    }

    pub fn set_kind(&mut self, kind: PageKind) {
        self.bytes[12..14].copy_from_slice(&(kind as u16).to_le_bytes());
    }

    pub fn lsn(&self) -> u64 {
        u64::from_le_bytes(self.bytes[16..24].try_into().unwrap())
    }

    pub fn set_lsn(&mut self, lsn: u64) {
        self.bytes[16..24].copy_from_slice(&lsn.to_le_bytes());
    }

    /// Sibling, freelist link, or overflow link. 0 means none.
    pub fn next(&self) -> u32 {
        u32::from_le_bytes(self.bytes[24..28].try_into().unwrap())
    }

    pub fn set_next(&mut self, id: u32) {
        self.bytes[24..28].copy_from_slice(&id.to_le_bytes());
    }

    /// Internal-node leftmost child, or heap flags. 0 means none.
    pub fn extra(&self) -> u32 {
        u32::from_le_bytes(self.bytes[28..32].try_into().unwrap())
    }

    pub fn set_extra(&mut self, v: u32) {
        self.bytes[28..32].copy_from_slice(&v.to_le_bytes());
    }

    pub fn payload(&self) -> &[u8] {
        &self.bytes[HEADER_LEN..]
    }

    pub fn payload_mut(&mut self) -> &mut [u8] {
        &mut self.bytes[HEADER_LEN..]
    }

    pub fn seal(&mut self) {
        self.bytes[4..8].fill(0);
        let sum = crc32fast::hash(&self.bytes);
        self.bytes[4..8].copy_from_slice(&sum.to_le_bytes());
    }

    pub fn verify(&self) -> Result<()> {
        let magic = u32::from_le_bytes(self.bytes[0..4].try_into().unwrap());
        if magic != MAGIC {
            return Err(Error::Corrupt {
                detail: format!("bad page magic {magic:#x}"),
            });
        }
        let stored = u32::from_le_bytes(self.bytes[4..8].try_into().unwrap());
        let mut tmp = self.bytes;
        tmp[4..8].fill(0);
        let sum = crc32fast::hash(&tmp);
        if sum != stored {
            return Err(Error::Corrupt {
                detail: format!("page checksum mismatch id={}", self.id().0),
            });
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum DataType {
    Int,
    Float,
    Bool,
    Text,
    Vector(u32),
}

impl DataType {
    pub fn name(&self) -> String {
        match self {
            DataType::Int => "INT".into(),
            DataType::Float => "FLOAT".into(),
            DataType::Bool => "BOOL".into(),
            DataType::Text => "TEXT".into(),
            DataType::Vector(n) => format!("VECTOR({n})"),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum IndexMethod {
    Btree,
    Inverted,
    Hnsw,
}

impl IndexMethod {
    pub fn parse(s: &str) -> Result<Self> {
        match s.to_ascii_lowercase().as_str() {
            "btree" => Ok(Self::Btree),
            "inverted" | "bm25" => Ok(Self::Inverted),
            "hnsw" => Ok(Self::Hnsw),
            other => Err(Error::Plan(format!("unknown index method {other}"))),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Btree => "btree",
            Self::Inverted => "inverted",
            Self::Hnsw => "hnsw",
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ColumnDef {
    pub name: String,
    pub ty: DataType,
    pub nullable: bool,
    pub primary_key: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IndexDef {
    pub id: u32,
    pub name: String,
    pub columns: Vec<String>,
    pub method: IndexMethod,
    pub root: u32,
    pub unique: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TableDef {
    pub id: u32,
    pub name: String,
    pub columns: Vec<ColumnDef>,
    pub heap_root: u32,
    pub indexes: Vec<IndexDef>,
    pub row_count: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Catalog {
    pub version: u32,
    pub next_table_id: u32,
    pub next_index_id: u32,
    pub next_txn_id: u64,
    pub next_commit_ts: u64,
    pub tables: Vec<TableDef>,
}

impl Catalog {
    pub fn empty() -> Self {
        Self {
            version: FORMAT_VERSION,
            next_table_id: 1,
            next_index_id: 1,
            next_txn_id: 1,
            next_commit_ts: 0,
            tables: Vec::new(),
        }
    }

    pub fn encode(&self) -> Result<Vec<u8>> {
        Ok(bincode::serialize(self)?)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.is_empty() {
            return Ok(Self::empty());
        }
        Ok(bincode::deserialize(bytes)?)
    }

    pub fn table(&self, name: &str) -> Result<&TableDef> {
        self.tables
            .iter()
            .find(|t| t.name == name)
            .ok_or(Error::NotFound)
    }

    pub fn table_mut(&mut self, name: &str) -> Result<&mut TableDef> {
        self.tables
            .iter_mut()
            .find(|t| t.name == name)
            .ok_or(Error::NotFound)
    }

    pub fn index(&self, name: &str) -> Result<(&TableDef, &IndexDef)> {
        for t in &self.tables {
            if let Some(ix) = t.indexes.iter().find(|i| i.name == name) {
                return Ok((t, ix));
            }
        }
        Err(Error::NotFound)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum LogicalOp {
    Insert {
        table_id: u32,
        rid_page: u32,
        rid_slot: u16,
        values: Vec<Value>,
    },
    Delete {
        table_id: u32,
        rid_page: u32,
        rid_slot: u16,
        values: Vec<Value>,
    },
    Update {
        table_id: u32,
        old_page: u32,
        old_slot: u16,
        new_page: u32,
        new_slot: u16,
        values: Vec<Value>,
    },
}

#[derive(Clone, Debug)]
pub struct CommittedOp {
    pub lsn: u64,
    pub commit_ts: u64,
    pub op: LogicalOp,
}

#[derive(Clone, Debug)]
pub struct Session {
    pub txn_id: Option<u64>,
    pub snapshot_ts: u64,
}

impl Session {
    pub fn new() -> Self {
        Self {
            txn_id: None,
            snapshot_ts: 0,
        }
    }
}

impl Default for Session {
    fn default() -> Self {
        Self::new()
    }
}

pub fn read_u16(buf: &[u8], at: usize) -> Result<u16> {
    let end = at.checked_add(2).ok_or_else(|| Error::Corrupt {
        detail: "u16 overflow".into(),
    })?;
    let slice = buf.get(at..end).ok_or_else(|| Error::Corrupt {
        detail: "short u16".into(),
    })?;
    Ok(u16::from_le_bytes(slice.try_into().unwrap()))
}

pub fn read_u32(buf: &[u8], at: usize) -> Result<u32> {
    let end = at.checked_add(4).ok_or_else(|| Error::Corrupt {
        detail: "u32 overflow".into(),
    })?;
    let slice = buf.get(at..end).ok_or_else(|| Error::Corrupt {
        detail: "short u32".into(),
    })?;
    Ok(u32::from_le_bytes(slice.try_into().unwrap()))
}

pub fn read_u64(buf: &[u8], at: usize) -> Result<u64> {
    let end = at.checked_add(8).ok_or_else(|| Error::Corrupt {
        detail: "u64 overflow".into(),
    })?;
    let slice = buf.get(at..end).ok_or_else(|| Error::Corrupt {
        detail: "short u64".into(),
    })?;
    Ok(u64::from_le_bytes(slice.try_into().unwrap()))
}

pub fn write_u16(buf: &mut [u8], at: usize, v: u16) {
    buf[at..at + 2].copy_from_slice(&v.to_le_bytes());
}

pub fn write_u32(buf: &mut [u8], at: usize, v: u32) {
    buf[at..at + 4].copy_from_slice(&v.to_le_bytes());
}

pub fn write_u64(buf: &mut [u8], at: usize, v: u64) {
    buf[at..at + 8].copy_from_slice(&v.to_le_bytes());
}
