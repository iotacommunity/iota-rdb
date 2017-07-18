use mapper;
use mysql;
use std::{error, fmt, result};

#[derive(Debug)]
pub enum Error {
  Mapper(mapper::Error),
  Mysql(mysql::Error),
}

pub type Result<T> = result::Result<T, Error>;

impl fmt::Display for Error {
  fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
    match *self {
      Error::Mapper(ref err) => write!(f, "Mapper error: {}", err),
      Error::Mysql(ref err) => write!(f, "Mysql error: {}", err),
    }
  }
}

impl error::Error for Error {
  fn description(&self) -> &str {
    match *self {
      Error::Mapper(ref err) => err.description(),
      Error::Mysql(ref err) => err.description(),
    }
  }

  fn cause(&self) -> Option<&error::Error> {
    match *self {
      Error::Mapper(ref err) => Some(err),
      Error::Mysql(ref err) => Some(err),
    }
  }
}

impl From<mapper::Error> for Error {
  fn from(err: mapper::Error) -> Error {
    Error::Mapper(err)
  }
}

impl From<mysql::Error> for Error {
  fn from(err: mysql::Error) -> Error {
    Error::Mysql(err)
  }
}
