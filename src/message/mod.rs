mod error;

pub use self::error::{Error, Result};

pub const TAG_LENGTH: usize = 27;

#[derive(Debug)]
pub struct Message {
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
  is_milestone: bool,
  solid: u8,
}

impl Message {
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
    let is_milestone = address_hash == milestone_address;
    let solid = if is_milestone && tag == milestone_start_index {
      0b11
    } else {
      0b00
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
      is_milestone,
      solid,
    })
  }

  define_getter!(hash, &str);
  define_getter!(address_hash, &str);
  define_getter!(value, i64);
  define_getter!(tag, &str);
  define_getter!(timestamp, f64);
  define_getter!(current_index, i32);
  define_getter!(last_index, i32);
  define_getter!(bundle_hash, &str);
  define_getter!(trunk_hash, &str);
  define_getter!(branch_hash, &str);
  define_getter!(is_milestone, bool);
  define_getter!(solid, u8);
}
