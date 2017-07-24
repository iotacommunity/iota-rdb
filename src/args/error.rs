use std::{error, fmt, num, result};

#[derive(Debug)]
pub enum Error {
  ArgNotFound,
  WriteThreadsParseInt(num::ParseIntError),
  ApproveThreadsParseInt(num::ParseIntError),
  SolidateThreadsParseInt(num::ParseIntError),
  MilestoneStartIndexParseInt(num::ParseIntError),
  MilestoneStartIndexToTrits,
}

pub type Result<T> = result::Result<T, Error>;

impl fmt::Display for Error {
  fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
    match *self {
      Error::ArgNotFound => write!(f, "Argument not found"),
      Error::WriteThreadsParseInt(ref err) => {
        write!(f, "{} (write-threads)", err)
      }
      Error::ApproveThreadsParseInt(ref err) => {
        write!(f, "{} (approve-threads)", err)
      }
      Error::SolidateThreadsParseInt(ref err) => {
        write!(f, "{} (solidate-threads)", err)
      }
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
      Error::WriteThreadsParseInt(ref err) |
      Error::ApproveThreadsParseInt(ref err) |
      Error::SolidateThreadsParseInt(ref err) |
      Error::MilestoneStartIndexParseInt(ref err) => err.description(),
      Error::MilestoneStartIndexToTrits => "Can't convert to trits",
    }
  }

  fn cause(&self) -> Option<&error::Error> {
    match *self {
      Error::ArgNotFound | Error::MilestoneStartIndexToTrits => None,
      Error::WriteThreadsParseInt(ref err) |
      Error::ApproveThreadsParseInt(ref err) |
      Error::SolidateThreadsParseInt(ref err) |
      Error::MilestoneStartIndexParseInt(ref err) => Some(err),
    }
  }
}
