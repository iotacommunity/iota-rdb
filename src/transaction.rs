use mapper::Mapper;
use std::num::ParseIntError;
use std::time::{SystemTime, UNIX_EPOCH};

const MAX_TAG_LENGTH: usize = 27;

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

  pub fn process(&self, mapper: &mut Mapper) {
    let mut result = mapper.select_transaction(self.hash);
    if let Some(ref mut row) = result {
      let id_trunk: Option<i64> = row.take("id_trunk");
      let id_branch: Option<i64> = row.take("id_branch");
      if id_trunk.unwrap_or(0) != 0 && id_branch.unwrap_or(0) != 0 {
        return;
      }
    }
    let id_trunk = mapper.insert_or_select_transaction(self.branch_hash);
    let id_branch = mapper.insert_or_select_transaction(self.branch_hash);
    let id_address = mapper.insert_or_select_address(self.address_hash);
    let id_bundle = mapper.insert_bundle(
      self.bundle_hash,
      milliseconds_since_epoch(),
      self.last_index,
    );
    mapper.save_transaction(
      result.is_none(),
      self.hash,
      id_trunk,
      id_branch,
      id_address,
      id_bundle,
      &self.tag[..MAX_TAG_LENGTH],
      self.value,
      self.timestamp,
      self.current_index,
      self.last_index,
    );
  }
}

fn milliseconds_since_epoch() -> f64 {
  let duration = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
  duration.as_secs() as f64 * 1000.0 +
    (duration.subsec_nanos() / 1_000_000) as f64
}
