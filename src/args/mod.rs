mod error;

pub use self::error::{Error, Result};
use clap::ArgMatches;
use transaction::TAG_LENGTH;
use utils;

pub struct Args<'a> {
  pub zmq_uri: &'a str,
  pub mysql_uri: &'a str,
  pub milestone_address: &'a str,
  pub milestone_start_index: String,
  pub verbose: bool,
}

impl<'a> Args<'a> {
  pub fn parse(matches: &'a ArgMatches<'a>) -> Result<Self> {
    let zmq_uri = matches.value_of("zmq_uri").ok_or(Error::ArgNotFound)?;
    let mysql_uri = matches.value_of("mysql_uri").ok_or(Error::ArgNotFound)?;
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
      milestone_address,
      milestone_start_index,
      verbose,
    })
  }
}
