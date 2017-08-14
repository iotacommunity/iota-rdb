use std::{error, fmt, num, result};

#[derive(Debug)]
pub enum Error {
  ArgNotFound,
  UpdateIntervalParseInt(num::ParseIntError),
  CalculationThreadsParseInt(num::ParseIntError),
  CalculationLimitParseInt(num::ParseIntError),
  GenerationLimitParseInt(num::ParseIntError),
  MilestoneStartIndexParseInt(num::ParseIntError),
  MilestoneStartIndexToTrits,
}

pub type Result<T> = result::Result<T, Error>;

impl fmt::Display for Error {
  fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
    match *self {
      Error::ArgNotFound => write!(f, "Argument not found"),
      Error::UpdateIntervalParseInt(ref err) => {
        write!(f, "{} (update-interval)", err)
      }
      Error::CalculationThreadsParseInt(ref err) => {
        write!(f, "{} (calculation-threads)", err)
      }
      Error::CalculationLimitParseInt(ref err) => {
        write!(f, "{} (calculation-limit)", err)
      }
      Error::GenerationLimitParseInt(ref err) => {
        write!(f, "{} (generation-limit)", err)
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
      Error::UpdateIntervalParseInt(ref err) |
      Error::CalculationThreadsParseInt(ref err) |
      Error::CalculationLimitParseInt(ref err) |
      Error::GenerationLimitParseInt(ref err) |
      Error::MilestoneStartIndexParseInt(ref err) => err.description(),
      Error::MilestoneStartIndexToTrits => "Can't convert to trits",
    }
  }

  fn cause(&self) -> Option<&error::Error> {
    match *self {
      Error::ArgNotFound | Error::MilestoneStartIndexToTrits => None,
      Error::UpdateIntervalParseInt(ref err) |
      Error::CalculationThreadsParseInt(ref err) |
      Error::CalculationLimitParseInt(ref err) |
      Error::GenerationLimitParseInt(ref err) |
      Error::MilestoneStartIndexParseInt(ref err) => Some(err),
    }
  }
}
