use mysql;
use query;
use std::{error, fmt, result, time};
use transaction;

#[derive(Debug)]
pub enum Error {
  Transaction(transaction::Error),
  Query(query::Error),
  SystemTime(time::SystemTimeError),
  NullHashToTrits,
}

pub type Result<T> = result::Result<T, Error>;

impl fmt::Display for Error {
  fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
    match *self {
      Error::Transaction(ref err) => write!(f, "Transaction error: {}", err),
      Error::Query(ref err) => write!(f, "Query error: {}", err),
      Error::SystemTime(ref err) => write!(f, "SystemTime error: {}", err),
      Error::NullHashToTrits => write!(f, "can't convert null_hash to trits"),
    }
  }
}

impl error::Error for Error {
  fn description(&self) -> &str {
    match *self {
      Error::Transaction(ref err) => err.description(),
      Error::Query(ref err) => err.description(),
      Error::SystemTime(ref err) => err.description(),
      Error::NullHashToTrits => "Can't convert to trits",
    }
  }

  fn cause(&self) -> Option<&error::Error> {
    match *self {
      Error::Transaction(ref err) => Some(err),
      Error::Query(ref err) => Some(err),
      Error::SystemTime(ref err) => Some(err),
      Error::NullHashToTrits => None,
    }
  }
}

impl From<query::Error> for Error {
  fn from(err: query::Error) -> Error {
    Error::Query(err)
  }
}

impl From<transaction::Error> for Error {
  fn from(err: transaction::Error) -> Error {
    Error::Transaction(err)
  }
}

impl From<time::SystemTimeError> for Error {
  fn from(err: time::SystemTimeError) -> Error {
    Error::SystemTime(err)
  }
}

impl From<mysql::Error> for Error {
  fn from(err: mysql::Error) -> Error {
    Error::Query(query::Error::from(err))
  }
}
