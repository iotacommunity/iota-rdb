use mapper;
use message;
use mysql;
use std::{error, fmt, result, time};

#[derive(Debug)]
pub enum Error {
  Message(message::Error),
  Mapper(mapper::Error),
  SystemTime(time::SystemTimeError),
}

pub type Result<T> = result::Result<T, Error>;

impl fmt::Display for Error {
  fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
    match *self {
      Error::Message(ref err) => write!(f, "Message error: {}", err),
      Error::Mapper(ref err) => write!(f, "Mapper error: {}", err),
      Error::SystemTime(ref err) => write!(f, "SystemTime error: {}", err),
    }
  }
}

impl error::Error for Error {
  fn description(&self) -> &str {
    match *self {
      Error::Message(ref err) => err.description(),
      Error::Mapper(ref err) => err.description(),
      Error::SystemTime(ref err) => err.description(),
    }
  }

  fn cause(&self) -> Option<&error::Error> {
    match *self {
      Error::Message(ref err) => Some(err),
      Error::Mapper(ref err) => Some(err),
      Error::SystemTime(ref err) => Some(err),
    }
  }
}

impl From<mapper::Error> for Error {
  fn from(err: mapper::Error) -> Error {
    Error::Mapper(err)
  }
}

impl From<message::Error> for Error {
  fn from(err: message::Error) -> Error {
    Error::Message(err)
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
