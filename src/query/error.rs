use mysql;
use std::{error, fmt, result};

#[derive(Debug)]
pub enum Error {
  Mysql(mysql::Error),
}

pub type Result<T> = result::Result<T, Error>;

impl fmt::Display for Error {
  fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
    match *self {
      Error::Mysql(ref err) => write!(f, "Mysql error: {}", err),
    }
  }
}

impl error::Error for Error {
  fn description(&self) -> &str {
    match *self {
      Error::Mysql(ref err) => err.description(),
    }
  }

  fn cause(&self) -> Option<&error::Error> {
    match *self {
      Error::Mysql(ref err) => Some(err),
    }
  }
}

impl From<mysql::Error> for Error {
  fn from(err: mysql::Error) -> Error {
    Error::Mysql(err)
  }
}
