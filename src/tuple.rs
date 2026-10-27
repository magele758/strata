//! On-disk tuple codec. A tuple is a 32-byte MVCC header followed by a
//! null bitmap and tagged values. Names are compared by the SQL layer;
//! this module only sees values.

use crate::error::{Error, Result};
use crate::types::MAX_TUPLE;
use crate::value::Value;

/// MVCC identity plus the decoded column values, in column order.
#[derive(Clone, Debug)]
pub struct DecodedTuple {
    pub xmin_txn: u64,
    pub xmin_ts: u64,
    pub xmax_txn: u64,
    pub xmax_ts: u64,
    pub values: Vec<Value>,
}

const HEADER_LEN: usize = 32;

/// Encode a tuple. `Error::Limit` if the result would exceed [`MAX_TUPLE`].
/// `Error::Type` if a float or vector component is NaN.
pub fn encode_tuple(
    xmin_txn: u64,
    xmin_ts: u64,
    xmax_txn: u64,
    xmax_ts: u64,
    values: &[Value],
) -> Result<Vec<u8>> {
    if values.len() > u16::MAX as usize {
        return Err(Error::Limit("too many tuple attributes".into()));
    }
    let nattrs = values.len() as u16;
    let bitmap_len = bitmap_len(values.len());
    let mut total = HEADER_LEN + 2 + bitmap_len;
    for value in values {
        total = total.checked_add(value_len(value)?).ok_or_else(over_limit)?;
        if total > MAX_TUPLE {
            return Err(over_limit());
        }
    }

    let mut out = Vec::with_capacity(total);
    out.extend_from_slice(&xmin_txn.to_le_bytes());
    out.extend_from_slice(&xmin_ts.to_le_bytes());
    out.extend_from_slice(&xmax_txn.to_le_bytes());
    out.extend_from_slice(&xmax_ts.to_le_bytes());
    out.extend_from_slice(&nattrs.to_le_bytes());
    let bitmap_at = out.len();
    out.resize(bitmap_at + bitmap_len, 0);
    for (i, value) in values.iter().enumerate() {
        if value.is_null() {
            // LSB of byte 0 is attribute 0. A set bit means NULL.
            out[bitmap_at + i / 8] |= 1 << (i % 8);
            continue;
        }
        write_value(&mut out, value)?;
        if out.len() > MAX_TUPLE {
            return Err(over_limit());
        }
    }
    if out.len() != total {
        return Err(Error::Corrupt {
            detail: "tuple encoder length mismatch".into(),
        });
    }
    if out.len() > MAX_TUPLE {
        return Err(over_limit());
    }
    Ok(out)
}

/// Decode a tuple. Short or malformed input is `Error::Corrupt`.
/// A buffer longer than [`MAX_TUPLE`] is `Error::Limit`. NaN is `Error::Type`.
pub fn decode_tuple(bytes: &[u8]) -> Result<DecodedTuple> {
    if bytes.len() > MAX_TUPLE {
        return Err(over_limit());
    }
    let mut cur = Cursor { bytes, at: 0 };
    if cur.rest() < HEADER_LEN + 2 {
        return Err(short());
    }
    let xmin_txn = cur.u64()?;
    let xmin_ts = cur.u64()?;
    let xmax_txn = cur.u64()?;
    let xmax_ts = cur.u64()?;
    let nattrs = cur.u16()? as usize;
    let bitmap_len = bitmap_len(nattrs);
    let bitmap = cur.take(bitmap_len)?.to_vec();
    let mut values = Vec::with_capacity(nattrs);
    for i in 0..nattrs {
        let is_null = bitmap
            .get(i / 8)
            .map(|b| b & (1 << (i % 8)) != 0)
            .unwrap_or(false);
        if is_null {
            values.push(Value::Null);
            continue;
        }
        values.push(read_value(&mut cur)?);
    }
    if cur.rest() != 0 {
        return Err(Error::Corrupt {
            detail: "trailing tuple bytes".into(),
        });
    }
    Ok(DecodedTuple {
        xmin_txn,
        xmin_ts,
        xmax_txn,
        xmax_ts,
        values,
    })
}

fn bitmap_len(nattrs: usize) -> usize {
    nattrs.saturating_add(7) / 8
}

fn value_len(value: &Value) -> Result<usize> {
    match value {
        Value::Null => Ok(0),
        Value::Int(_) => Ok(1 + 8),
        Value::Float(n) => {
            if n.is_nan() {
                return Err(nan());
            }
            Ok(1 + 8)
        }
        Value::Bool(_) => Ok(1 + 1),
        Value::Text(s) => {
            if s.len() > u32::MAX as usize {
                return Err(Error::Limit("text too long".into()));
            }
            Ok(1 + 4 + s.len())
        }
        Value::Vector(v) => {
            if v.iter().any(|x| x.is_nan()) {
                return Err(nan());
            }
            if v.len() > u32::MAX as usize {
                return Err(Error::Limit("vector too long".into()));
            }
            Ok(1 + 4 + v.len() * 4)
        }
    }
}

fn write_value(out: &mut Vec<u8>, value: &Value) -> Result<()> {
    match value {
        Value::Null => Ok(()),
        Value::Int(n) => {
            out.push(1);
            out.extend_from_slice(&n.to_le_bytes());
            Ok(())
        }
        Value::Float(n) => {
            if n.is_nan() {
                return Err(nan());
            }
            out.push(2);
            out.extend_from_slice(&n.to_le_bytes());
            Ok(())
        }
        Value::Bool(b) => {
            out.push(3);
            out.push(u8::from(*b));
            Ok(())
        }
        Value::Text(s) => {
            let bytes = s.as_bytes();
            if bytes.len() > u32::MAX as usize {
                return Err(Error::Limit("text too long".into()));
            }
            out.push(4);
            out.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
            out.extend_from_slice(bytes);
            Ok(())
        }
        Value::Vector(v) => {
            if v.len() > u32::MAX as usize {
                return Err(Error::Limit("vector too long".into()));
            }
            if v.iter().any(|x| x.is_nan()) {
                return Err(nan());
            }
            out.push(5);
            out.extend_from_slice(&(v.len() as u32).to_le_bytes());
            for x in v {
                out.extend_from_slice(&x.to_le_bytes());
            }
            Ok(())
        }
    }
}

fn read_value(cur: &mut Cursor<'_>) -> Result<Value> {
    let tag = cur.u8()?;
    match tag {
        1 => Ok(Value::Int(cur.i64()?)),
        2 => {
            let n = cur.f64()?;
            if n.is_nan() {
                return Err(nan());
            }
            Ok(Value::Float(n))
        }
        3 => {
            let b = cur.u8()?;
            match b {
                0 => Ok(Value::Bool(false)),
                1 => Ok(Value::Bool(true)),
                _ => Err(Error::Corrupt {
                    detail: format!("bad bool byte {b}"),
                }),
            }
        }
        4 => {
            let n = cur.u32()? as usize;
            let raw = cur.take(n)?;
            let text = std::str::from_utf8(raw).map_err(|_| Error::Corrupt {
                detail: "tuple text is not utf-8".into(),
            })?;
            Ok(Value::Text(text.to_owned()))
        }
        5 => {
            let dim = cur.u32()? as usize;
            let nbytes = dim.checked_mul(4).ok_or_else(|| Error::Corrupt {
                detail: "vector dimension overflow".into(),
            })?;
            if nbytes > cur.rest() {
                return Err(short());
            }
            let mut comps = Vec::with_capacity(dim);
            for _ in 0..dim {
                let x = cur.f32()?;
                if x.is_nan() {
                    return Err(nan());
                }
                comps.push(x);
            }
            Ok(Value::Vector(comps))
        }
        other => Err(Error::Corrupt {
            detail: format!("unknown tuple tag {other}"),
        }),
    }
}

struct Cursor<'a> {
    bytes: &'a [u8],
    at: usize,
}

impl<'a> Cursor<'a> {
    fn rest(&self) -> usize {
        self.bytes.len().saturating_sub(self.at)
    }

    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        let end = self.at.checked_add(n).ok_or_else(|| Error::Corrupt {
            detail: "tuple offset overflow".into(),
        })?;
        if end > self.bytes.len() {
            return Err(short());
        }
        let slice = &self.bytes[self.at..end];
        self.at = end;
        Ok(slice)
    }

    fn u8(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }

    fn u16(&mut self) -> Result<u16> {
        let s = self.take(2)?;
        Ok(u16::from_le_bytes(s.try_into().unwrap()))
    }

    fn u32(&mut self) -> Result<u32> {
        let s = self.take(4)?;
        Ok(u32::from_le_bytes(s.try_into().unwrap()))
    }

    fn u64(&mut self) -> Result<u64> {
        let s = self.take(8)?;
        Ok(u64::from_le_bytes(s.try_into().unwrap()))
    }

    fn i64(&mut self) -> Result<i64> {
        Ok(self.u64()? as i64)
    }

    fn f64(&mut self) -> Result<f64> {
        Ok(f64::from_bits(self.u64()?))
    }

    fn f32(&mut self) -> Result<f32> {
        let s = self.take(4)?;
        Ok(f32::from_le_bytes(s.try_into().unwrap()))
    }
}

fn short() -> Error {
    Error::Corrupt {
        detail: "short tuple".into(),
    }
}

fn over_limit() -> Error {
    Error::Limit(format!("tuple exceeds {MAX_TUPLE} bytes"))
}

fn nan() -> Error {
    Error::Type("NaN is not a value".into())
}
