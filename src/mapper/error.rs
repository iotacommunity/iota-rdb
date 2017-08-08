use mysql;
use query;
use record;
use std::{error, fmt, result};

#[derive(Debug)]
pub enum Error {
  Locked,
  Query(query::Error),
  Record(record::Error),
  NullHashToTrits,
}

pub type Result<T> = result::Result<T, Error>;

impl fmt::Display for Error {
  fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
    match *self {
      Error::Locked => write!(f, "can't obtain a lock"),
      Error::Query(ref err) => write!(f, "Query error: {}", err),
      Error::Record(ref err) => write!(f, "Record error: {}", err),
      Error::NullHashToTrits => write!(f, "Can't convert null_hash to trits"),
    }
  }
}

impl error::Error for Error {
  fn description(&self) -> &str {
    match *self {
      Error::Locked => "Can't obtain a lock",
      Error::Query(ref err) => err.description(),
      Error::Record(ref err) => err.description(),
      Error::NullHashToTrits => "Can't convert to trits",
    }
  }

  fn cause(&self) -> Option<&error::Error> {
    match *self {
      Error::Query(ref err) => Some(err),
      Error::Record(ref err) => Some(err),
      Error::Locked | Error::NullHashToTrits => None,
    }
  }
}

impl From<record::Error> for Error {
  fn from(err: record::Error) -> Error {
    Error::Record(err)
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
