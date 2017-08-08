use std::{error, fmt, num, result};

#[derive(Debug)]
pub enum Error {
  ParseInt(num::ParseIntError),
  ParseFloat(num::ParseFloatError),
}

pub type Result<T> = result::Result<T, Error>;

impl fmt::Display for Error {
  fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
    match *self {
      Error::ParseInt(ref err) => write!(f, "ParseInt error: {}", err),
      Error::ParseFloat(ref err) => write!(f, "ParseFloat error: {}", err),
    }
  }
}

impl error::Error for Error {
  fn description(&self) -> &str {
    match *self {
      Error::ParseInt(ref err) => err.description(),
      Error::ParseFloat(ref err) => err.description(),
    }
  }

  fn cause(&self) -> Option<&error::Error> {
    match *self {
      Error::ParseInt(ref err) => Some(err),
      Error::ParseFloat(ref err) => Some(err),
    }
  }
}

impl From<num::ParseIntError> for Error {
  fn from(err: num::ParseIntError) -> Error {
    Error::ParseInt(err)
  }
}

impl From<num::ParseFloatError> for Error {
  fn from(err: num::ParseFloatError) -> Error {
    Error::ParseFloat(err)
  }
}
