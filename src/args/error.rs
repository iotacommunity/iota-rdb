use std::{error, fmt, num, result};

#[derive(Debug)]
pub enum Error {
  ArgNotFound,
  MilestoneStartIndexParseInt(num::ParseIntError),
  MilestoneStartIndexToTrits,
}

pub type Result<T> = result::Result<T, Error>;

impl fmt::Display for Error {
  fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
    match *self {
      Error::ArgNotFound => write!(f, "Argument not found"),
      Error::MilestoneStartIndexParseInt(ref err) => {
        write!(f, "{} (milestone-start-index)", err)
      }
      Error::MilestoneStartIndexToTrits => {
        write!(f, "can't convert to trits (milestone-start-index)")
      }
    }
  }
}

impl error::Error for Error {
  fn description(&self) -> &str {
    match *self {
      Error::ArgNotFound => "Argument not found",
      Error::MilestoneStartIndexParseInt(ref err) => err.description(),
      Error::MilestoneStartIndexToTrits => "Can't convert to trits",
    }
  }

  fn cause(&self) -> Option<&error::Error> {
    match *self {
      Error::ArgNotFound | Error::MilestoneStartIndexToTrits => None,
      Error::MilestoneStartIndexParseInt(ref err) => Some(err),
    }
  }
}
