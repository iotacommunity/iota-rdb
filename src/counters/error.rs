use mysql;
use std::{error, fmt, result};

#[derive(Debug)]
pub enum Error {
  Mysql(mysql::Error),
  IdColumnNotFound,
}

pub type Result<T> = result::Result<T, Error>;

impl fmt::Display for Error {
  fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
    match *self {
      Error::Mysql(ref err) => write!(f, "Mysql error: {}", err),
      Error::IdColumnNotFound => write!(f, "ID column not found"),
    }
  }
}

impl error::Error for Error {
  fn description(&self) -> &str {
    match *self {
      Error::Mysql(ref err) => err.description(),
      Error::IdColumnNotFound => "ID column not found",
    }
  }

  fn cause(&self) -> Option<&error::Error> {
    match *self {
      Error::Mysql(ref err) => Some(err),
      Error::IdColumnNotFound => None,
    }
  }
}

impl From<mysql::Error> for Error {
  fn from(err: mysql::Error) -> Error {
    Error::Mysql(err)
  }
}
