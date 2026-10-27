//! Length-prefixed little-endian wire protocol.
//!
//! If `password` is set, the first client message is tag `1`, a `u32` length,
//! and that many password bytes. The server replies `0` and continues, or `1`
//! and closes.
//!
//! After that, each request is a `u32` length and that many UTF-8 SQL bytes.
//! A socket read of zero bytes closes the connection. Responses:
//!
//! - `0` rows: `u32` column count, each name as `u32` length + bytes, then
//!   records of `1` followed by one value per column, ended by `0`.
//! - `1` exec: `u64` affected, `u32` length + message bytes.
//! - `2` error: `u32` length + message bytes.
//!
//! Value tags: `0` null, `1` `i64`, `2` `f64`, `3` `u8` bool, `4` `u32` length
//! + UTF-8, `5` `u32` count + `f32`s.

use std::io::{self, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;

use crate::engine::Database;
use crate::error::{Error, Result};
use crate::types::Session;
use crate::value::{ExecuteResult, Value};

const MAX_SQL: u32 = 32 * 1024 * 1024;
const MAX_PASSWORD: u32 = 64 * 1024;

const AUTH_TAG: u8 = 1;
const TAG_ROWS: u8 = 0;
const TAG_EXEC: u8 = 1;
const TAG_ERROR: u8 = 2;

/// Bind `addr` and serve `db` until the process stops. One thread per connection.
pub fn serve(db: Arc<Database>, addr: &str, password: Option<String>) -> Result<()> {
    let listener = TcpListener::bind(addr)?;
    loop {
        match listener.accept() {
            Ok((stream, _)) => spawn_connection(stream, Arc::clone(&db), password.clone()),
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) => eprintln!("strata: accept: {e}"),
        }
    }
}

fn spawn_connection(stream: TcpStream, db: Arc<Database>, password: Option<String>) {
    let built = std::thread::Builder::new()
        .name("strata-conn".into())
        .spawn(move || {
            let run = std::panic::AssertUnwindSafe(|| connection(stream, db, password));
            match std::panic::catch_unwind(run) {
                Ok(Ok(())) => {}
                Ok(Err(e)) => eprintln!("strata: {e}"),
                Err(_) => eprintln!("strata: connection handler panicked"),
            }
        });
    if let Err(e) = built {
        eprintln!("strata: spawn: {e}");
    }
}

fn connection(mut stream: TcpStream, db: Arc<Database>, password: Option<String>) -> Result<()> {
    let _ = stream.set_nodelay(true);
    if let Some(expected) = password.as_deref() {
        match authenticate(&mut stream, expected) {
            Ok(true) => {}
            Ok(false) => return Ok(()),
            Err(e) if is_disconnect(&e) => return Ok(()),
            Err(e) => return Err(e.into()),
        }
    }

    let mut open = OpenTxn {
        db: &db,
        session: db.session(),
    };
    sql_loop(&mut stream, open.db, &mut open.session)
}

/// Rolls an abandoned transaction back so a dropped client cannot keep the writer lock.
struct OpenTxn<'a> {
    db: &'a Database,
    session: Session,
}

impl Drop for OpenTxn<'_> {
    fn drop(&mut self) {
        if self.session.txn_id.is_some() {
            let _ = self.db.rollback_session(&mut self.session);
        }
    }
}

fn authenticate(stream: &mut TcpStream, expected: &str) -> io::Result<bool> {
    let Some(tag) = read_byte(stream)? else {
        return Ok(false);
    };
    if tag != AUTH_TAG {
        let _ = stream.write_all(&[1]);
        return Ok(false);
    }
    let got = match read_frame(stream, MAX_PASSWORD) {
        Ok(Some(bytes)) => bytes,
        Ok(None) => {
            let _ = stream.write_all(&[1]);
            return Ok(false);
        }
        Err(e) if e.kind() == io::ErrorKind::InvalidData || is_disconnect(&e) => {
            let _ = stream.write_all(&[1]);
            return Ok(false);
        }
        Err(e) => return Err(e),
    };
    if !passwords_equal(expected.as_bytes(), &got) {
        stream.write_all(&[1])?;
        return Ok(false);
    }
    stream.write_all(&[0])?;
    Ok(true)
}

fn sql_loop(stream: &mut TcpStream, db: &Database, session: &mut Session) -> Result<()> {
    loop {
        let frame = match read_frame(stream, MAX_SQL) {
            Ok(Some(bytes)) => bytes,
            Ok(None) => return Ok(()),
            Err(e) => return finish_read(stream, e),
        };
        let sql = match String::from_utf8(frame) {
            Ok(s) => s,
            Err(_) => {
                if !send_error(stream, "sql is not utf-8")? {
                    return Ok(());
                }
                continue;
            }
        };
        let sent = match db.execute_session(session, &sql) {
            Ok(result) => match write_result(stream, &result) {
                Ok(()) => true,
                Err(e) if e.kind() == io::ErrorKind::InvalidData => {
                    send_error(stream, &e.to_string())?
                }
                Err(e) if is_disconnect(&e) => false,
                Err(e) => return Err(e.into()),
            },
            Err(e) => send_error(stream, &e.to_string())?,
        };
        if !sent {
            return Ok(());
        }
    }
}

fn finish_read(stream: &mut TcpStream, err: io::Error) -> Result<()> {
    if err.kind() == io::ErrorKind::InvalidData {
        let _ = write_error(stream, &err.to_string());
        return Ok(());
    }
    if is_disconnect(&err) {
        return Ok(());
    }
    Err(Error::Io(err))
}

/// `Ok(false)` means the peer went away while the reply was written.
fn send_error(stream: &mut TcpStream, message: &str) -> Result<bool> {
    match write_error(stream, message) {
        Ok(()) => Ok(true),
        Err(e) if is_disconnect(&e) => Ok(false),
        Err(e) => Err(e.into()),
    }
}

fn read_byte(stream: &mut TcpStream) -> io::Result<Option<u8>> {
    let mut buf = [0u8; 1];
    loop {
        match stream.read(&mut buf) {
            Ok(0) => return Ok(None),
            Ok(_) => return Ok(Some(buf[0])),
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(e),
        }
    }
}

/// `Ok(None)` is a clean EOF before any length byte. The body is read in full.
fn read_frame(stream: &mut TcpStream, max: u32) -> io::Result<Option<Vec<u8>>> {
    let Some(len) = read_u32(stream)? else {
        return Ok(None);
    };
    if len > max {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("frame length {len} exceeds {max}"),
        ));
    }
    let mut buf = vec![0u8; len as usize];
    stream.read_exact(&mut buf)?;
    Ok(Some(buf))
}

fn read_u32(stream: &mut TcpStream) -> io::Result<Option<u32>> {
    let mut buf = [0u8; 4];
    let mut got = 0;
    while got < 4 {
        match stream.read(&mut buf[got..]) {
            Ok(0) if got == 0 => return Ok(None),
            Ok(0) => {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "truncated length",
                ));
            }
            Ok(n) => got += n,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(e),
        }
    }
    Ok(Some(u32::from_le_bytes(buf)))
}

fn write_result(stream: &mut TcpStream, result: &ExecuteResult) -> io::Result<()> {
    match result {
        ExecuteResult::Rows { columns, rows } => {
            rows_fit(columns, rows)?;
            let mut out = io::BufWriter::new(&mut *stream);
            put_u8(&mut out, TAG_ROWS)?;
            put_u32(&mut out, columns.len() as u32)?;
            for name in columns {
                put_bytes(&mut out, name.as_bytes())?;
            }
            for row in rows {
                put_u8(&mut out, 1)?;
                for col in 0..columns.len() {
                    put_value(&mut out, row.get(col).unwrap_or(&Value::Null))?;
                }
            }
            put_u8(&mut out, 0)?;
            out.flush()
        }
        ExecuteResult::Exec { affected, message } => {
            let mut out = io::BufWriter::new(&mut *stream);
            put_u8(&mut out, TAG_EXEC)?;
            put_u64(&mut out, *affected)?;
            put_bytes(&mut out, truncate_utf8(message, u32::MAX as usize))?;
            out.flush()
        }
    }
}

fn write_error(stream: &mut TcpStream, message: &str) -> io::Result<()> {
    let mut out = io::BufWriter::new(&mut *stream);
    put_u8(&mut out, TAG_ERROR)?;
    put_bytes(&mut out, truncate_utf8(message, u32::MAX as usize))?;
    out.flush()
}

fn rows_fit(columns: &[String], rows: &[Vec<Value>]) -> io::Result<()> {
    if u32::try_from(columns.len()).is_err() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "too many columns",
        ));
    }
    for name in columns {
        if name.len() > u32::MAX as usize {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "column name too long",
            ));
        }
    }
    for row in rows {
        for value in row.iter().take(columns.len()) {
            match value {
                Value::Text(s) if s.len() > u32::MAX as usize => {
                    return Err(io::Error::new(io::ErrorKind::InvalidData, "text too long"));
                }
                Value::Vector(xs) if u32::try_from(xs.len()).is_err() => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "vector exceeds u32::MAX",
                    ));
                }
                _ => {}
            }
        }
    }
    Ok(())
}

fn put_value(w: &mut impl Write, value: &Value) -> io::Result<()> {
    match value {
        Value::Null => put_u8(w, 0),
        Value::Int(n) => {
            put_u8(w, 1)?;
            w.write_all(&n.to_le_bytes())
        }
        Value::Float(n) => {
            put_u8(w, 2)?;
            w.write_all(&n.to_le_bytes())
        }
        Value::Bool(b) => w.write_all(&[3, u8::from(*b)]),
        Value::Text(s) => {
            put_u8(w, 4)?;
            put_bytes(w, s.as_bytes())
        }
        Value::Vector(xs) => {
            put_u8(w, 5)?;
            let count = u32::try_from(xs.len()).map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidData, "vector exceeds u32::MAX")
            })?;
            put_u32(w, count)?;
            for x in xs {
                w.write_all(&x.to_le_bytes())?;
            }
            Ok(())
        }
    }
}

fn put_u8(w: &mut impl Write, v: u8) -> io::Result<()> {
    w.write_all(&[v])
}

fn put_u32(w: &mut impl Write, v: u32) -> io::Result<()> {
    w.write_all(&v.to_le_bytes())
}

fn put_u64(w: &mut impl Write, v: u64) -> io::Result<()> {
    w.write_all(&v.to_le_bytes())
}

fn put_bytes(w: &mut impl Write, bytes: &[u8]) -> io::Result<()> {
    let len = u32::try_from(bytes.len()).map_err(|_| {
        io::Error::new(io::ErrorKind::InvalidData, "payload exceeds u32::MAX")
    })?;
    put_u32(w, len)?;
    w.write_all(bytes)
}

fn truncate_utf8(s: &str, max: usize) -> &[u8] {
    if s.len() <= max {
        return s.as_bytes();
    }
    let mut end = max;
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    &s.as_bytes()[..end]
}

fn passwords_equal(expected: &[u8], got: &[u8]) -> bool {
    let mut diff = expected.len() ^ got.len();
    let n = expected.len().max(got.len());
    for i in 0..n {
        let a = expected.get(i).copied().unwrap_or(0);
        let b = got.get(i).copied().unwrap_or(0);
        diff |= usize::from(a ^ b);
    }
    diff == 0
}

fn is_disconnect(err: &io::Error) -> bool {
    matches!(
        err.kind(),
        io::ErrorKind::UnexpectedEof
            | io::ErrorKind::ConnectionReset
            | io::ErrorKind::ConnectionAborted
            | io::ErrorKind::BrokenPipe
            | io::ErrorKind::NotConnected
    )
}
