mod error;

pub use self::error::{Error, Result};
use clap::ArgMatches;
use iota_trytes::TRITS_PER_TRYTE;
use iota_trytes::num::int2trits;
use iota_trytes::string::trits_to_string;
use transaction::TAG_LENGTH;

const MILESTONE_START_INDEX_TRITS: u8 = (TAG_LENGTH * TRITS_PER_TRYTE) as u8;

pub struct Args<'a> {
  zmq_uri: &'a str,
  mysql_uri: &'a str,
  write_threads_count: usize,
  approve_threads_count: usize,
  solidate_threads_count: usize,
  milestone_address: &'a str,
  milestone_start_index: String,
  verbose: bool,
}

impl<'a> Args<'a> {
  pub fn parse(matches: &'a ArgMatches<'a>) -> Result<Self> {
    let zmq_uri = matches.value_of("zmq_uri").ok_or(Error::ArgNotFound)?;
    let mysql_uri = matches.value_of("mysql_uri").ok_or(Error::ArgNotFound)?;
    let write_threads_count = matches
      .value_of("write_threads_count")
      .ok_or(Error::ArgNotFound)?
      .parse()
      .map_err(Error::WriteThreadsParseInt)?;
    let approve_threads_count = matches
      .value_of("approve_threads_count")
      .ok_or(Error::ArgNotFound)?
      .parse()
      .map_err(Error::ApproveThreadsParseInt)?;
    let solidate_threads_count = matches
      .value_of("solidate_threads_count")
      .ok_or(Error::ArgNotFound)?
      .parse()
      .map_err(Error::SolidateThreadsParseInt)?;
    let milestone_address = matches.value_of("milestone_address").ok_or(
      Error::ArgNotFound,
    )?;
    let milestone_start_index =
      Self::convert_trits(matches
        .value_of("milestone_start_index")
        .ok_or(Error::ArgNotFound)?
        .parse()
        .map_err(Error::MilestoneStartIndexParseInt)?)?;
    let verbose = matches.is_present("VERBOSE");

    Ok(Self {
      zmq_uri,
      mysql_uri,
      write_threads_count,
      approve_threads_count,
      solidate_threads_count,
      milestone_address,
      milestone_start_index,
      verbose,
    })
  }

  pub fn zmq_uri(&self) -> &str {
    self.zmq_uri
  }

  pub fn mysql_uri(&self) -> &str {
    self.mysql_uri
  }

  pub fn write_threads_count(&self) -> usize {
    self.write_threads_count
  }

  pub fn approve_threads_count(&self) -> usize {
    self.approve_threads_count
  }

  pub fn solidate_threads_count(&self) -> usize {
    self.solidate_threads_count
  }

  pub fn milestone_address(&self) -> &str {
    self.milestone_address
  }

  pub fn milestone_start_index(&self) -> &str {
    &self.milestone_start_index
  }

  pub fn verbose(&self) -> bool {
    self.verbose
  }

  fn convert_trits(number: isize) -> Result<String> {
    let mut trits = int2trits(number, MILESTONE_START_INDEX_TRITS);
    while trits.len() < MILESTONE_START_INDEX_TRITS as usize {
      trits.push(0);
    }
    Ok(trits_to_string(&trits).ok_or(
      Error::MilestoneStartIndexToTrits,
    )?)
  }
}
