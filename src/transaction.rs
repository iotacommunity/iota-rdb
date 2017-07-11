use std::num::ParseIntError;

#[derive(Debug)]
pub struct Transaction<'a> {
  hash: &'a str,
  address_hash: &'a str,
  value: i64,
  tag: &'a str,
  timestamp: i64,
  current_index: i32,
  last_index: i32,
  bundle_hash: &'a str,
  trunk_hash: &'a str,
  branch_hash: &'a str,
}

impl<'a> Transaction<'a> {
  pub fn parse(source: &'a str) -> Result<Self, ParseIntError> {
    let chunks: Vec<&'a str> = source.split(' ').collect();
    Ok(Transaction {
      hash: chunks[1],
      address_hash: chunks[2],
      value: chunks[3].parse()?,
      tag: chunks[4],
      timestamp: chunks[5].parse()?,
      current_index: chunks[6].parse()?,
      last_index: chunks[7].parse()?,
      bundle_hash: chunks[8],
      trunk_hash: chunks[9],
      branch_hash: chunks[10],
    })
  }
}
