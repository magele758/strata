use std::fmt;
use std::io;

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug)]
pub enum Error {
    Io(io::Error),
    Corrupt { detail: String },
    NotFound,
    Conflict,
    Exists(String),
    Parse(String),
    Plan(String),
    Type(String),
    Recovery(String),
    Full,
    Closed,
    Auth,
    Limit(String),
    Txn(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Io(e) => write!(f, "io: {e}"),
            Error::Corrupt { detail } => write!(f, "corrupt: {detail}"),
            Error::NotFound => write!(f, "not found"),
            Error::Conflict => write!(f, "write-write conflict"),
            Error::Exists(s) => write!(f, "already exists: {s}"),
            Error::Parse(s) => write!(f, "parse: {s}"),
            Error::Plan(s) => write!(f, "plan: {s}"),
            Error::Type(s) => write!(f, "type: {s}"),
            Error::Recovery(s) => write!(f, "recovery: {s}"),
            Error::Full => write!(f, "buffer pool has no unpinned frame"),
            Error::Closed => write!(f, "database is closed"),
            Error::Auth => write!(f, "authentication failed"),
            Error::Limit(s) => write!(f, "limit: {s}"),
            Error::Txn(s) => write!(f, "transaction: {s}"),
        }
    }
}

impl std::error::Error for Error {}

impl From<io::Error> for Error {
    fn from(value: io::Error) -> Self {
        Error::Io(value)
    }
}

impl From<bincode::Error> for Error {
    fn from(value: bincode::Error) -> Self {
        Error::Corrupt {
            detail: value.to_string(),
        }
    }
}
