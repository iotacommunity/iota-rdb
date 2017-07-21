mod error;

pub use self::error::{Error, Result};
use clap::ArgMatches;

pub struct Args<'a> {
  zmq_uri: &'a str,
  mysql_uri: &'a str,
  write_threads_count: usize,
  approve_threads_count: usize,
  milestone_address: &'a str,
  milestone_start_index: &'a str,
  verbose: bool,
}

impl<'a> Args<'a> {
  pub fn parse(matches: &'a ArgMatches<'a>) -> Result<Self> {
    Ok(Self {
      zmq_uri: matches.value_of("zmq_uri").ok_or(Error::ArgNotFound)?,
      mysql_uri: matches.value_of("mysql_uri").ok_or(Error::ArgNotFound)?,
      write_threads_count: matches
        .value_of("write_threads_count")
        .ok_or(Error::ArgNotFound)?
        .parse()
        .map_err(Error::WriteThreadsParseInt)?,
      approve_threads_count: matches
        .value_of("approve_threads_count")
        .ok_or(Error::ArgNotFound)?
        .parse()
        .map_err(Error::ApproveThreadsParseInt)?,
      milestone_address: matches.value_of("milestone_address").ok_or(
        Error::ArgNotFound,
      )?,
      milestone_start_index: matches.value_of("milestone_start_index").ok_or(
        Error::ArgNotFound,
      )?,
      verbose: matches.is_present("VERBOSE"),
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

  pub fn milestone_address(&self) -> &str {
    self.milestone_address
  }

  pub fn milestone_start_index(&self) -> &str {
    self.milestone_start_index
  }

  pub fn verbose(&self) -> bool {
    self.verbose
  }
}
