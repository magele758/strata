use std::fmt;

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use crate::types::{DataType, Rid};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Value {
    Null,
    Int(i64),
    Float(f64),
    Bool(bool),
    Text(String),
    Vector(Vec<f32>),
}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Value::Null, Value::Null) => true,
            (Value::Int(a), Value::Int(b)) => a == b,
            (Value::Float(a), Value::Float(b)) => a.to_bits() == b.to_bits(),
            (Value::Bool(a), Value::Bool(b)) => a == b,
            (Value::Text(a), Value::Text(b)) => a == b,
            (Value::Vector(a), Value::Vector(b)) => a == b,
            _ => false,
        }
    }
}

impl Value {
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    pub fn coerce(self, ty: &DataType) -> Result<Self> {
        match (self, ty) {
            (Value::Null, _) => Ok(Value::Null),
            (Value::Int(v), DataType::Int) => Ok(Value::Int(v)),
            (Value::Int(v), DataType::Float) => Ok(Value::Float(v as f64)),
            (Value::Float(v), DataType::Float) => {
                if v.is_nan() {
                    return Err(Error::Type("NaN is not a value".into()));
                }
                Ok(Value::Float(v))
            }
            (Value::Bool(v), DataType::Bool) => Ok(Value::Bool(v)),
            (Value::Text(s), DataType::Text) => Ok(Value::Text(s)),
            (Value::Text(s), DataType::Vector(n)) => Ok(Value::Vector(parse_vector(&s, *n)?)),
            (Value::Vector(v), DataType::Vector(n)) => {
                if v.len() != *n as usize {
                    return Err(Error::Type(format!(
                        "vector dim {} != {}",
                        v.len(),
                        n
                    )));
                }
                Ok(Value::Vector(v))
            }
            (other, ty) => Err(Error::Type(format!(
                "cannot coerce {} to {}",
                other.type_name(),
                ty.name()
            ))),
        }
    }

    pub fn type_name(&self) -> &'static str {
        match self {
            Value::Null => "null",
            Value::Int(_) => "int",
            Value::Float(_) => "float",
            Value::Bool(_) => "bool",
            Value::Text(_) => "text",
            Value::Vector(_) => "vector",
        }
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Null => write!(f, "NULL"),
            Value::Int(v) => write!(f, "{v}"),
            Value::Float(v) => write!(f, "{v}"),
            Value::Bool(v) => write!(f, "{v}"),
            Value::Text(v) => write!(f, "{v}"),
            Value::Vector(v) => {
                write!(f, "[")?;
                for (i, x) in v.iter().enumerate() {
                    if i > 0 {
                        write!(f, ",")?;
                    }
                    write!(f, "{x}")?;
                }
                write!(f, "]")
            }
        }
    }
}

pub fn parse_vector(text: &str, expect: u32) -> Result<Vec<f32>> {
    let s = text.trim();
    let inner = s
        .strip_prefix('[')
        .and_then(|x| x.strip_suffix(']'))
        .ok_or_else(|| Error::Type(format!("vector literal `{s}` must be [n,n,...]")))?;
    if inner.trim().is_empty() {
        if expect == 0 {
            return Ok(Vec::new());
        }
        return Err(Error::Type(format!("expected vector dim {expect}")));
    }
    let mut out = Vec::new();
    for part in inner.split(',') {
        let n: f32 = part
            .trim()
            .parse()
            .map_err(|_| Error::Type(format!("bad vector component `{part}`")))?;
        if n.is_nan() {
            return Err(Error::Type("NaN is not a value".into()));
        }
        out.push(n);
    }
    if out.len() != expect as usize {
        return Err(Error::Type(format!(
            "vector dim {} != {expect}",
            out.len()
        )));
    }
    Ok(out)
}

/// Order-preserving, prefix-free encoding. Null sorts last within a column.
pub fn encode_key(values: &[Value]) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    for v in values {
        encode_one(&mut out, v)?;
    }
    Ok(out)
}

pub fn encode_key_with_rid(values: &[Value], rid: Rid) -> Result<Vec<u8>> {
    let mut out = encode_key(values)?;
    out.extend_from_slice(&rid.encode());
    Ok(out)
}

fn encode_one(out: &mut Vec<u8>, v: &Value) -> Result<()> {
    match v {
        Value::Null => out.push(0xFF),
        Value::Int(n) => {
            out.push(0x01);
            let bits = (*n as u64) ^ (1 << 63);
            out.extend_from_slice(&bits.to_be_bytes());
        }
        Value::Float(n) => {
            if n.is_nan() {
                return Err(Error::Type("NaN is not a value".into()));
            }
            out.push(0x02);
            out.extend_from_slice(&order_f64(*n).to_be_bytes());
        }
        Value::Bool(b) => {
            out.push(0x03);
            out.push(u8::from(*b));
        }
        Value::Text(s) => {
            out.push(0x04);
            let bytes = s.as_bytes();
            if bytes.len() > u32::MAX as usize {
                return Err(Error::Limit("text key too long".into()));
            }
            out.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
            out.extend_from_slice(bytes);
        }
        Value::Vector(_) => {
            return Err(Error::Type(
                "vector columns use an hnsw index, not a btree key".into(),
            ))
        }
    }
    Ok(())
}

fn order_f64(n: f64) -> u64 {
    let bits = n.to_bits();
    if bits & (1 << 63) == 0 {
        bits ^ (1 << 63)
    } else {
        !bits
    }
}

pub fn key_successor(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut out = prefix.to_vec();
    for byte in out.iter_mut().rev() {
        if *byte != 0xFF {
            *byte += 1;
            return Some(out);
        }
        *byte = 0;
    }
    None
}

#[derive(Clone, Debug)]
pub struct Config {
    pub buffer_pages: usize,
    pub columnar_flush_every: u64,
    pub password: Option<String>,
    pub lock_wait_ms: Option<u64>,
    pub max_connections: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            buffer_pages: 128,
            columnar_flush_every: 32,
            password: None,
            lock_wait_ms: None,
            max_connections: 128,
        }
    }
}

#[derive(Clone, Debug)]
pub enum ExecuteResult {
    Rows {
        columns: Vec<String>,
        rows: Vec<Vec<Value>>,
    },
    Exec {
        affected: u64,
        message: String,
    },
}

impl ExecuteResult {
    pub fn to_text(&self) -> String {
        match self {
            ExecuteResult::Exec { message, .. } => message.clone(),
            ExecuteResult::Rows { columns, rows } => format_table(columns, rows),
        }
    }
}

pub fn format_table(columns: &[String], rows: &[Vec<Value>]) -> String {
    if columns.is_empty() {
        return format!("({} rows)", rows.len());
    }
    let mut widths: Vec<usize> = columns.iter().map(|c| c.len()).collect();
    let rendered: Vec<Vec<String>> = rows
        .iter()
        .map(|row| {
            row.iter()
                .map(|v| v.to_string())
                .collect()
        })
        .collect();
    for row in &rendered {
        for (i, cell) in row.iter().enumerate() {
            if i < widths.len() {
                widths[i] = widths[i].max(cell.len());
            }
        }
    }
    let mut out = String::new();
    for (i, col) in columns.iter().enumerate() {
        if i > 0 {
            out.push_str(" | ");
        }
        out.push_str(&format!("{col:<width$}", width = widths[i]));
    }
    out.push('\n');
    for (i, w) in widths.iter().enumerate() {
        if i > 0 {
            out.push_str("-+-");
        }
        out.push_str(&"-".repeat(*w));
    }
    out.push('\n');
    for row in &rendered {
        for (i, cell) in row.iter().enumerate() {
            if i > 0 {
                out.push_str(" | ");
            }
            let w = widths.get(i).copied().unwrap_or(cell.len());
            out.push_str(&format!("{cell:<width$}", width = w));
        }
        out.push('\n');
    }
    out.push_str(&format!("({} rows)", rows.len()));
    out
}

#[derive(Clone, Debug)]
pub struct DbStats {
    pub pages: u32,
    pub wal_lsn: u64,
    pub buffered: usize,
    pub tables: usize,
    pub commit_ts: u64,
    pub columnar_applied_lsn: u64,
}
