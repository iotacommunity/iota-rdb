use message;
use mysql;
use record;
use std::{error, fmt, result, time};

#[derive(Debug)]
pub enum Error {
  Message(message::Error),
  Record(record::Error),
  SystemTime(time::SystemTimeError),
}

pub type Result<T> = result::Result<T, Error>;

impl fmt::Display for Error {
  fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
    match *self {
      Error::Message(ref err) => write!(f, "Message error: {}", err),
      Error::Record(ref err) => write!(f, "Record error: {}", err),
      Error::SystemTime(ref err) => write!(f, "SystemTime error: {}", err),
    }
  }
}

impl error::Error for Error {
  fn description(&self) -> &str {
    match *self {
      Error::Message(ref err) => err.description(),
      Error::Record(ref err) => err.description(),
      Error::SystemTime(ref err) => err.description(),
    }
  }

  fn cause(&self) -> Option<&error::Error> {
    match *self {
      Error::Message(ref err) => Some(err),
      Error::Record(ref err) => Some(err),
      Error::SystemTime(ref err) => Some(err),
    }
  }
}

impl From<record::Error> for Error {
  fn from(err: record::Error) -> Error {
    Error::Record(err)
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
    Error::Record(record::Error::from(err))
  }
}
