mod error;

pub use self::error::{Error, Result};
use clap::ArgMatches;
use message::transaction_message::TAG_LENGTH;
use utils;

pub struct Args<'a> {
  pub zmq_uri: &'a str,
  pub mysql_uri: &'a str,
  pub update_interval: u64,
  pub calculation_threads: usize,
  pub calculation_limit: usize,
  pub generation_limit: usize,
  pub milestone_address: &'a str,
  pub milestone_start_index: String,
  pub log_config: &'a str,
}

impl<'a> Args<'a> {
  pub fn parse(matches: &'a ArgMatches<'a>) -> Result<Self> {
    let zmq_uri = matches.value_of("zmq_uri").ok_or(Error::ArgNotFound)?;
    let mysql_uri = matches.value_of("mysql_uri").ok_or(Error::ArgNotFound)?;
    let update_interval = matches
      .value_of("update_interval")
      .ok_or(Error::ArgNotFound)?
      .parse()
      .map_err(Error::UpdateIntervalParseInt)?;
    let calculation_threads = matches
      .value_of("calculation_threads")
      .ok_or(Error::ArgNotFound)?
      .parse()
      .map_err(Error::CalculationThreadsParseInt)?;
    let calculation_limit = matches
      .value_of("calculation_limit")
      .ok_or(Error::ArgNotFound)?
      .parse()
      .map_err(Error::CalculationLimitParseInt)?;
    let generation_limit = matches
      .value_of("generation_limit")
      .ok_or(Error::ArgNotFound)?
      .parse()
      .map_err(Error::GenerationLimitParseInt)?;
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
    let log_config = matches.value_of("log_config").ok_or(Error::ArgNotFound)?;

    Ok(Self {
      zmq_uri,
      mysql_uri,
      update_interval,
      calculation_threads,
      calculation_limit,
      generation_limit,
      milestone_address,
      milestone_start_index,
      log_config,
    })
  }
}
