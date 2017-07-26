use mysql;
use std::{error, fmt, result};

#[derive(Debug)]
pub enum Error {
  Mysql(mysql::Error),
  RecordNotFound,
  ColumnNotFound,
}

pub type Result<T> = result::Result<T, Error>;

impl fmt::Display for Error {
  fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
    match *self {
      Error::Mysql(ref err) => write!(f, "Mysql error: {}", err),
      Error::RecordNotFound => write!(f, "Record not found"),
      Error::ColumnNotFound => write!(f, "Column not found"),
    }
  }
}

impl error::Error for Error {
  fn description(&self) -> &str {
    match *self {
      Error::Mysql(ref err) => err.description(),
      Error::RecordNotFound => "Record not found",
      Error::ColumnNotFound => "Column not found",
    }
  }

  fn cause(&self) -> Option<&error::Error> {
    match *self {
      Error::Mysql(ref err) => Some(err),
      Error::RecordNotFound | Error::ColumnNotFound => None,
    }
  }
}

impl From<mysql::Error> for Error {
  fn from(err: mysql::Error) -> Error {
    Error::Mysql(err)
  }
}
