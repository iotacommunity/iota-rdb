use mysql;
use query;
use std::{error, fmt, result};

#[derive(Debug)]
pub enum Error {
  Query(query::Error),
  NullHashToTrits,
}

pub type Result<T> = result::Result<T, Error>;

impl fmt::Display for Error {
  fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
    match *self {
      Error::Query(ref err) => write!(f, "Query error: {}", err),
      Error::NullHashToTrits => write!(f, "can't convert null_hash to trits"),
    }
  }
}

impl error::Error for Error {
  fn description(&self) -> &str {
    match *self {
      Error::Query(ref err) => err.description(),
      Error::NullHashToTrits => "Can't convert to trits",
    }
  }

  fn cause(&self) -> Option<&error::Error> {
    match *self {
      Error::Query(ref err) => Some(err),
      Error::NullHashToTrits => None,
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
