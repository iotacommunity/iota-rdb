use mysql;
use std::{error, fmt, result};

#[derive(Debug)]
pub enum Error {
  Mysql(mysql::Error),
  RecordNotFound(u64),
  ColumnNotFound,
  AddressChecksumToTrits,
}

pub type Result<T> = result::Result<T, Error>;

impl fmt::Display for Error {
  fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
    match *self {
      Error::Mysql(ref err) => write!(f, "MySQL error: {}", err),
      Error::RecordNotFound(id) => write!(f, "Record not found ({})", id),
      Error::ColumnNotFound => write!(f, "Column not found"),
      Error::AddressChecksumToTrits => {
        write!(f, "can't convert address checksum to trits")
      }
    }
  }
}

impl error::Error for Error {
  fn description(&self) -> &str {
    match *self {
      Error::Mysql(ref err) => err.description(),
      Error::RecordNotFound(_) => "Record not found",
      Error::ColumnNotFound => "Column not found",
      Error::AddressChecksumToTrits => "Can't convert to trits",
    }
  }

  fn cause(&self) -> Option<&error::Error> {
    match *self {
      Error::Mysql(ref err) => Some(err),
      Error::RecordNotFound(_) |
      Error::ColumnNotFound |
      Error::AddressChecksumToTrits => None,
    }
  }
}

impl From<mysql::Error> for Error {
  fn from(err: mysql::Error) -> Error {
    Error::Mysql(err)
  }
}
