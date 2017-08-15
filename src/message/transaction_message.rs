use super::Result;
use solid::Solid;

pub const TAG_LENGTH: usize = 27;

#[derive(Debug)]
pub struct TransactionMessage {
  hash: String,
  address_hash: String,
  value: i64,
  tag: String,
  timestamp: f64,
  current_index: i32,
  last_index: i32,
  bundle_hash: String,
  trunk_hash: String,
  branch_hash: String,
  arrival: f64,
  is_milestone: bool,
  solid: Solid,
}

impl TransactionMessage {
  pub fn parse(
    source: &str,
    milestone_address: &str,
    milestone_start_index: &str,
  ) -> Result<Self> {
    let chunks: Vec<&str> = source.split(' ').collect();
    let hash = chunks[1].to_owned();
    let address_hash = chunks[2].to_owned();
    let value = chunks[3].parse()?;
    let tag = chunks[4][..TAG_LENGTH].to_owned();
    let timestamp = chunks[5].parse()?;
    let current_index = chunks[6].parse()?;
    let last_index = chunks[7].parse()?;
    let bundle_hash = chunks[8].to_owned();
    let trunk_hash = chunks[9].to_owned();
    let branch_hash = chunks[10].to_owned();
    let arrival = normalize_timestamp(chunks[11].parse()?);
    let is_milestone = address_hash == milestone_address;
    let solid = if is_milestone && tag == milestone_start_index {
      Solid::Complete
    } else {
      Solid::None
    };
    Ok(Self {
      hash,
      address_hash,
      value,
      tag,
      timestamp,
      current_index,
      last_index,
      bundle_hash,
      trunk_hash,
      branch_hash,
      arrival,
      is_milestone,
      solid,
    })
  }

  impl_getter!(hash, &str);
  impl_getter!(address_hash, &str);
  impl_getter!(value, i64);
  impl_getter!(tag, &str);
  impl_getter!(timestamp, f64);
  impl_getter!(current_index, i32);
  impl_getter!(last_index, i32);
  impl_getter!(bundle_hash, &str);
  impl_getter!(trunk_hash, &str);
  impl_getter!(branch_hash, &str);
  impl_getter!(arrival, f64);
  impl_getter!(is_milestone, bool);
  impl_getter!(solid, Solid);
}

fn normalize_timestamp(mut timestamp: f64) -> f64 {
  const THRESHOLD: f64 = 1_262_304_000_000.0; // 01.01.2010 in milliseconds
  if timestamp > THRESHOLD {
    timestamp /= 1000.0;
  }
  timestamp
}
