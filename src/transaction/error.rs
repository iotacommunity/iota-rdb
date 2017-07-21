use mapper;
use mysql;
use std::{error, fmt, num, result, time};

#[derive(Debug)]
pub enum Error {
  Mapper(mapper::Error),
  SystemTime(time::SystemTimeError),
  ParseInt(num::ParseIntError),
}

pub type Result<T> = result::Result<T, Error>;

impl fmt::Display for Error {
  fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
    match *self {
      Error::Mapper(ref err) => write!(f, "Mapper error: {}", err),
      Error::SystemTime(ref err) => write!(f, "SystemTime error: {}", err),
      Error::ParseInt(ref err) => write!(f, "ParseInt error: {}", err),
    }
  }
}

impl error::Error for Error {
  fn description(&self) -> &str {
    match *self {
      Error::Mapper(ref err) => err.description(),
      Error::SystemTime(ref err) => err.description(),
      Error::ParseInt(ref err) => err.description(),
    }
  }

  fn cause(&self) -> Option<&error::Error> {
    match *self {
      Error::Mapper(ref err) => Some(err),
      Error::SystemTime(ref err) => Some(err),
      Error::ParseInt(ref err) => Some(err),
    }
  }
}

impl From<mapper::Error> for Error {
  fn from(err: mapper::Error) -> Error {
    Error::Mapper(err)
  }
}

impl From<num::ParseIntError> for Error {
  fn from(err: num::ParseIntError) -> Error {
    Error::ParseInt(err)
  }
}

impl From<time::SystemTimeError> for Error {
  fn from(err: time::SystemTimeError) -> Error {
    Error::SystemTime(err)
  }
}

impl From<mysql::Error> for Error {
  fn from(err: mysql::Error) -> Error {
    Error::Mapper(mapper::Error::from(err))
  }
}
