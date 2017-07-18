use std::{error, fmt, result};

#[derive(Debug)]
pub enum Error {
}

pub type Result<T> = result::Result<T, Error>;

impl fmt::Display for Error {
  fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
    match *self {
    }
  }
}

impl error::Error for Error {
  fn description(&self) -> &str {
    match *self {
    }
  }

  fn cause(&self) -> Option<&error::Error> {
    match *self {
    }
  }
}
