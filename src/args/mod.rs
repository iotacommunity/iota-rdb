mod error;

pub use self::error::{Error, Result};
use clap::ArgMatches;
use transaction::TAG_LENGTH;
use utils;

pub struct Args<'a> {
  zmq_uri: &'a str,
  mysql_uri: &'a str,
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
    let milestone_address = matches
      .value_of("milestone_address")
      .ok_or(Error::ArgNotFound)?;
    let milestone_start_index = utils::trits_string(
      matches
        .value_of("milestone_start_index")
        .ok_or(Error::ArgNotFound)?
        .parse()
        .map_err(Error::MilestoneStartIndexParseInt)?,
      TAG_LENGTH,
    ).ok_or(Error::MilestoneStartIndexToTrits)?;
    let verbose = matches.is_present("VERBOSE");

    Ok(Self {
      zmq_uri,
      mysql_uri,
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
}
