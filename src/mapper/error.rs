use mysql;
use query;
use std::{error, fmt, result};

#[derive(Debug)]
pub enum Error {
  Query(query::Error),
}

pub type Result<T> = result::Result<T, Error>;

impl fmt::Display for Error {
  fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
    match *self {
      Error::Query(ref err) => write!(f, "Query error: {}", err),
    }
  }
}

impl error::Error for Error {
  fn description(&self) -> &str {
    match *self {
      Error::Query(ref err) => err.description(),
    }
  }

  fn cause(&self) -> Option<&error::Error> {
    match *self {
      Error::Query(ref err) => Some(err),
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
