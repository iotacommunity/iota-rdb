mod error;

pub use self::error::{Error, Result};
use counters::Counters;
use mapper::{self, Mapper};
use utils;

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
  pub fn parse(source: &'a str) -> Result<Self> {
    let chunks: Vec<&'a str> = source.split(' ').collect();
    Ok(Self {
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

  pub fn approve(mapper: &mut Mapper, mut ids: Vec<u64>) -> Result<()> {
    loop {
      match ids.pop() {
        None => break,
        Some(id) => {
          let mut row = mapper.select_transaction_by_id(id)?.ok_or(
            mapper::Error::RecordNotFound,
          )?;
          let milestone_approved =
            row.take_opt("mst_a").ok_or(mapper::Error::ColumnNotFound)?;
          if !milestone_approved.unwrap_or(false) {
            let id_trunk = row
              .take_opt("id_trunk")
              .ok_or(mapper::Error::ColumnNotFound)?
              .unwrap_or(0);
            let id_branch = row
              .take_opt("id_branch")
              .ok_or(mapper::Error::ColumnNotFound)?
              .unwrap_or(0);
            let current_index = row.take_opt("current_idx").ok_or(
              mapper::Error::ColumnNotFound,
            )?;
            if id_trunk != 0 {
              ids.push(id_trunk);
            }
            if id_branch != 0 {
              ids.push(id_branch);
            }
            if let Ok(0) = current_index {
              let id_bundle = row.take_opt("id_bundle").ok_or(
                mapper::Error::ColumnNotFound,
              )?;
              if let Ok(id_bundle) = id_bundle {
                let confirmed = utils::milliseconds_since_epoch()?;
                mapper.update_bundle(id_bundle, confirmed)?;
              }
            }
            mapper.approve_transaction(id)?;
          }
        }
      }
    }
    Ok(())
  }

  pub fn process(
    &self,
    mapper: &mut Mapper,
    counters: &Counters,
    milestone_address: &str,
  ) -> Result<Option<Vec<u64>>> {
    let mut result = mapper.select_transaction_by_hash(self.hash)?;
    if let Some(ref mut row) = result {
      let id_trunk = row.take_opt("id_trunk").ok_or(
        mapper::Error::ColumnNotFound,
      )?;
      let id_branch = row.take_opt("id_branch").ok_or(
        mapper::Error::ColumnNotFound,
      )?;
      if id_trunk.unwrap_or(0) != 0 && id_branch.unwrap_or(0) != 0 {
        return Ok(None);
      }
    }
    let id_trunk = mapper.fetch_transaction(counters, self.trunk_hash)?;
    let id_branch = mapper.fetch_transaction(counters, self.branch_hash)?;
    let id_address = mapper.fetch_address(counters, self.address_hash)?;
    let id_bundle = mapper.fetch_bundle(
      counters,
      self.bundle_hash,
      utils::milliseconds_since_epoch()?,
      self.last_index,
    )?;
    let is_milestone = self.address_hash == milestone_address;
    let transaction = mapper::Transaction {
      hash: self.hash,
      id_trunk: id_trunk,
      id_branch: id_branch,
      id_address: id_address,
      id_bundle: id_bundle,
      tag: &self.tag[..MAX_TAG_LENGTH],
      value: self.value,
      timestamp: self.timestamp,
      current_idx: self.current_index,
      last_idx: self.last_index,
      is_mst: is_milestone,
      mst_a: is_milestone,
    };
    if result.is_none() {
      mapper.insert_transaction(transaction)?;
    } else {
      mapper.update_transaction(transaction)?;
    }
    if is_milestone {
      Ok(Some(vec![id_trunk, id_branch]))
    } else {
      Ok(None)
    }
  }
}
