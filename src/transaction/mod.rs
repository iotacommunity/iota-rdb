mod error;

pub use self::error::{Error, Result};

pub const TAG_LENGTH: usize = 27;

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
  is_milestone: bool,
  solid: u8,
}

impl<'a> Transaction<'a> {
  pub fn new(
    source: &'a str,
    milestone_address: &str,
    milestone_start_index: &str,
  ) -> Result<Self> {
    let chunks: Vec<&'a str> = source.split(' ').collect();
    let hash = chunks[1];
    let address_hash = chunks[2];
    let value = chunks[3].parse()?;
    let tag = &chunks[4][..TAG_LENGTH];
    let timestamp = chunks[5].parse()?;
    let current_index = chunks[6].parse()?;
    let last_index = chunks[7].parse()?;
    let bundle_hash = chunks[8];
    let trunk_hash = chunks[9];
    let branch_hash = chunks[10];
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

  pub fn hash(&self) -> &str {
    self.hash
  }

  pub fn address_hash(&self) -> &str {
    self.address_hash
  }

  pub fn value(&self) -> i64 {
    self.value
  }

  pub fn tag(&self) -> &str {
    self.tag
  }

  pub fn timestamp(&self) -> i64 {
    self.timestamp
  }

  pub fn current_index(&self) -> i32 {
    self.current_index
  }

  pub fn last_index(&self) -> i32 {
    self.last_index
  }

  pub fn bundle_hash(&self) -> &str {
    self.bundle_hash
  }

  pub fn trunk_hash(&self) -> &str {
    self.trunk_hash
  }

  pub fn branch_hash(&self) -> &str {
    self.branch_hash
  }

  pub fn is_milestone(&self) -> bool {
    self.is_milestone
  }

  pub fn solid(&self) -> u8 {
    self.solid
  }

  pub fn solidate(&mut self, solid: u8) -> &mut Self {
    self.solid |= solid;
    self
  }
}
