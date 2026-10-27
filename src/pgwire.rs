//! Enough of the PostgreSQL v3 frontend/backend protocol for simple query.
//! Extended query, COPY, and SCRAM are not implemented.

use std::io::{self, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::thread;

use crate::engine::Database;
use crate::error::{Error, Result};
use crate::value::{ExecuteResult, Value};

const SSL_REQUEST: i32 = 80877103;
const PROTOCOL_3: i32 = 196608;
const MAX_STARTUP: u32 = 64 * 1024;
const MAX_MESSAGE: u32 = 32 * 1024 * 1024;

/// Bind `addr` and serve `db` until the process stops. One thread per connection.
/// A bad client closes that connection and does not stop the accept loop.
pub fn serve_simple(db: Arc<Database>, addr: &str) -> Result<()> {
    let listener = TcpListener::bind(addr).map_err(Error::Io)?;
    loop {
        match listener.accept() {
            Ok((stream, _)) => {
                let db = Arc::clone(&db);
                thread::spawn(move || {
                    let _ = connection(stream, db);
                });
            }
            Err(err) if is_transient_accept(&err) => continue,
            Err(err) => return Err(Error::Io(err)),
        }
    }
}

fn is_transient_accept(err: &io::Error) -> bool {
    matches!(
        err.kind(),
        io::ErrorKind::ConnectionAborted
            | io::ErrorKind::ConnectionReset
            | io::ErrorKind::Interrupted
            | io::ErrorKind::WouldBlock
            | io::ErrorKind::TimedOut
    )
}

fn connection(mut stream: TcpStream, db: Arc<Database>) -> Result<()> {
    let _ = stream.set_nodelay(true);
    if let Err(err) = startup(&mut stream) {
        let _ = write_error(&mut stream, &err.to_string());
        return Err(err);
    }
    loop {
        let tag = match read_byte(&mut stream) {
            Ok(Some(b)) => b,
            Ok(None) => return Ok(()),
            Err(err) => return Err(Error::Io(err)),
        };
        match tag {
            b'Q' => {
                let sql = match read_simple_query(&mut stream) {
                    Ok(sql) => sql,
                    Err(err) => return Err(Error::Io(err)),
                };
                if let Err(err) = answer_query(&mut stream, &db, &sql) {
                    return Err(err);
                }
            }
            b'X' => return Ok(()),
            _ => return Ok(()),
        }
    }
}

fn startup(stream: &mut TcpStream) -> Result<()> {
    let mut head = [0u8; 8];
    stream.read_exact(&mut head)?;
    let mut len = u32::from_be_bytes([head[0], head[1], head[2], head[3]]);
    let mut code = i32::from_be_bytes([head[4], head[5], head[6], head[7]]);
    if len == 8 && code == SSL_REQUEST {
        stream.write_all(&[b'N'])?;
        stream.flush()?;
        stream.read_exact(&mut head)?;
        len = u32::from_be_bytes([head[0], head[1], head[2], head[3]]);
        code = i32::from_be_bytes([head[4], head[5], head[6], head[7]]);
    }
    if code != PROTOCOL_3 || len < 8 || len > MAX_STARTUP {
        return Err(Error::Io(io::Error::new(
            io::ErrorKind::InvalidData,
            "unsupported startup",
        )));
    }
    let rest = (len - 8) as usize;
    if rest > 0 {
        let mut params = vec![0u8; rest];
        stream.read_exact(&mut params)?;
    }
    write_auth_ok(stream)?;
    write_parameter_status(stream, "server_version", "strata")?;
    write_backend_key(stream)?;
    write_ready(stream)?;
    stream.flush()?;
    Ok(())
}

fn answer_query(stream: &mut TcpStream, db: &Database, sql: &str) -> Result<()> {
    match db.execute(sql) {
        Ok(ExecuteResult::Rows { columns, rows }) => {
            write_row_description(stream, &columns)?;
            for row in &rows {
                write_data_row(stream, row, columns.len())?;
            }
            write_command_complete(stream, &format!("SELECT {}", rows.len()))?;
            write_ready(stream)?;
        }
        Ok(ExecuteResult::Exec { message, .. }) => {
            write_command_complete(stream, &message)?;
            write_ready(stream)?;
        }
        Err(err) => {
            write_error(stream, &err.to_string())?;
            write_ready(stream)?;
        }
    }
    stream.flush()?;
    Ok(())
}

fn read_simple_query(stream: &mut TcpStream) -> io::Result<String> {
    let len = read_u32(stream)?;
    if len < 5 || len > MAX_MESSAGE {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "bad query length",
        ));
    }
    let mut body = vec![0u8; (len - 4) as usize];
    stream.read_exact(&mut body)?;
    let end = body.iter().position(|b| *b == 0).unwrap_or(body.len());
    let sql = String::from_utf8_lossy(&body[..end]).into_owned();
    Ok(sql)
}

fn read_byte(stream: &mut TcpStream) -> io::Result<Option<u8>> {
    let mut buf = [0u8; 1];
    match stream.read(&mut buf) {
        Ok(0) => Ok(None),
        Ok(_) => Ok(Some(buf[0])),
        Err(err) if err.kind() == io::ErrorKind::UnexpectedEof => Ok(None),
        Err(err) => Err(err),
    }
}

fn read_u32(stream: &mut TcpStream) -> io::Result<u32> {
    let mut buf = [0u8; 4];
    stream.read_exact(&mut buf)?;
    Ok(u32::from_be_bytes(buf))
}

fn write_auth_ok(stream: &mut TcpStream) -> Result<()> {
    let mut msg = Vec::with_capacity(9);
    msg.push(b'R');
    msg.extend_from_slice(&8u32.to_be_bytes());
    msg.extend_from_slice(&0i32.to_be_bytes());
    stream.write_all(&msg)?;
    Ok(())
}

fn write_parameter_status(stream: &mut TcpStream, name: &str, value: &str) -> Result<()> {
    let payload_len = 4 + name.len() + 1 + value.len() + 1;
    let mut msg = Vec::with_capacity(1 + payload_len);
    msg.push(b'S');
    msg.extend_from_slice(&(payload_len as u32).to_be_bytes());
    msg.extend_from_slice(name.as_bytes());
    msg.push(0);
    msg.extend_from_slice(value.as_bytes());
    msg.push(0);
    stream.write_all(&msg)?;
    Ok(())
}

fn write_backend_key(stream: &mut TcpStream) -> Result<()> {
    let mut msg = Vec::with_capacity(13);
    msg.push(b'K');
    msg.extend_from_slice(&12u32.to_be_bytes());
    msg.extend_from_slice(&0i32.to_be_bytes());
    msg.extend_from_slice(&0i32.to_be_bytes());
    stream.write_all(&msg)?;
    Ok(())
}

fn write_ready(stream: &mut TcpStream) -> Result<()> {
    stream.write_all(&[b'Z', 0, 0, 0, 5, b'I'])?;
    Ok(())
}

fn write_row_description(stream: &mut TcpStream, columns: &[String]) -> Result<()> {
    if columns.len() > i16::MAX as usize {
        return Err(Error::Io(io::Error::new(
            io::ErrorKind::InvalidData,
            "too many columns",
        )));
    }
    let mut body = Vec::new();
    body.extend_from_slice(&(columns.len() as i16).to_be_bytes());
    for name in columns {
        let name = cstr_bytes(name);
        body.extend_from_slice(&name);
        body.push(0);
        body.extend_from_slice(&0i32.to_be_bytes()); // table oid
        body.extend_from_slice(&0i16.to_be_bytes()); // attribute number
        body.extend_from_slice(&25i32.to_be_bytes()); // text
        body.extend_from_slice(&(-1i16).to_be_bytes());
        body.extend_from_slice(&(-1i32).to_be_bytes());
        body.extend_from_slice(&0i16.to_be_bytes()); // text format
    }
    write_typed(stream, b'T', &body)
}

fn write_data_row(stream: &mut TcpStream, row: &[Value], ncols: usize) -> Result<()> {
    let mut body = Vec::new();
    body.extend_from_slice(&(ncols as i16).to_be_bytes());
    for i in 0..ncols {
        match row.get(i) {
            Some(Value::Null) | None => {
                body.extend_from_slice(&(-1i32).to_be_bytes());
            }
            Some(value) => {
                let text = value.to_string();
                let bytes = text.as_bytes();
                let len = i32::try_from(bytes.len()).unwrap_or(i32::MAX);
                let take = len as usize;
                body.extend_from_slice(&len.to_be_bytes());
                body.extend_from_slice(&bytes[..take]);
            }
        }
    }
    write_typed(stream, b'D', &body)
}

fn write_command_complete(stream: &mut TcpStream, tag: &str) -> Result<()> {
    let tag = cstr_bytes(tag);
    let mut body = Vec::with_capacity(tag.len() + 1);
    body.extend_from_slice(&tag);
    body.push(0);
    write_typed(stream, b'C', &body)
}

fn write_error(stream: &mut TcpStream, message: &str) -> Result<()> {
    let message = cstr_bytes(message);
    let mut body = Vec::new();
    body.push(b'S');
    body.extend_from_slice(b"ERROR");
    body.push(0);
    body.push(b'M');
    body.extend_from_slice(&message);
    body.push(0);
    body.push(0);
    write_typed(stream, b'E', &body)
}

fn write_typed(stream: &mut TcpStream, tag: u8, body: &[u8]) -> Result<()> {
    let len = 4u32
        .checked_add(u32::try_from(body.len()).unwrap_or(u32::MAX))
        .unwrap_or(u32::MAX);
    stream.write_all(&[tag])?;
    stream.write_all(&len.to_be_bytes())?;
    stream.write_all(body)?;
    Ok(())
}

fn cstr_bytes(s: &str) -> Vec<u8> {
    s.bytes().filter(|b| *b != 0).collect()
}
