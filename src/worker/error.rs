use mapper;
use message;
use mysql;
use query;
use record;
use std::{error, fmt, result, time};

#[derive(Debug)]
pub enum Error {
  Message(message::Error),
  Query(query::Error),
  Record(record::Error),
  Mapper(mapper::Error),
  SystemTime(time::SystemTimeError),
}

pub type Result<T> = result::Result<T, Error>;

impl fmt::Display for Error {
  fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
    match *self {
      Error::Message(ref err) => write!(f, "Message error: {}", err),
      Error::Query(ref err) => write!(f, "Query error: {}", err),
      Error::Record(ref err) => write!(f, "Record error: {}", err),
      Error::Mapper(ref err) => write!(f, "Mapper error: {}", err),
      Error::SystemTime(ref err) => write!(f, "SystemTime error: {}", err),
    }
  }
}

impl error::Error for Error {
  fn description(&self) -> &str {
    match *self {
      Error::Message(ref err) => err.description(),
      Error::Query(ref err) => err.description(),
      Error::Record(ref err) => err.description(),
      Error::Mapper(ref err) => err.description(),
      Error::SystemTime(ref err) => err.description(),
    }
  }

  fn cause(&self) -> Option<&error::Error> {
    match *self {
      Error::Message(ref err) => Some(err),
      Error::Query(ref err) => Some(err),
      Error::Record(ref err) => Some(err),
      Error::Mapper(ref err) => Some(err),
      Error::SystemTime(ref err) => Some(err),
    }
  }
}

impl From<query::Error> for Error {
  fn from(err: query::Error) -> Error {
    Error::Query(err)
  }
}

impl From<record::Error> for Error {
  fn from(err: record::Error) -> Error {
    Error::Record(err)
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
    Error::Query(query::Error::from(err))
  }
}
