use mysql;
use query;
use std::{error, fmt, result};

#[derive(Debug)]
pub enum Error {
  Query(query::Error),
  RecordNotFound,
  ColumnNotFound,
  AddressChecksumToTrits,
}

pub type Result<T> = result::Result<T, Error>;

impl fmt::Display for Error {
  fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
    match *self {
      Error::Query(ref err) => write!(f, "Query error: {}", err),
      Error::RecordNotFound => write!(f, "Record not found"),
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
      Error::Query(ref err) => err.description(),
      Error::RecordNotFound => "Record not found",
      Error::ColumnNotFound => "Column not found",
      Error::AddressChecksumToTrits => "Can't convert to trits",
    }
  }

  fn cause(&self) -> Option<&error::Error> {
    match *self {
      Error::Query(ref err) => Some(err),
      Error::RecordNotFound |
      Error::ColumnNotFound |
      Error::AddressChecksumToTrits => None,
    }
  }
}

impl From<query::Error> for Error {
  fn from(err: query::Error) -> Error {
    Error::Query(err)
  }
}

impl From<mysql::Error> for Error {
  fn from(err: mysql::Error) -> Error {
    Error::Query(query::Error::from(err))
  }
}
