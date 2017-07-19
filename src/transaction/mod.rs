mod error;

pub use self::error::{Error, Result};
use counters::Counters;
use mapper::{self, Mapper};
use mysql;
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
    let (timestamp, mut counter) = (utils::milliseconds_since_epoch()?, 0);
    loop {
      match ids.pop() {
        None => break,
        Some(id) => {
          let mut row = mapper.select_transaction_by_id(id)?.ok_or(
            mapper::Error::RecordNotFound,
          )?;
          let milestone_approved =
            row.take_opt("mst_a").ok_or(mapper::Error::ColumnNotFound)?;
          if milestone_approved.unwrap_or(false) {
            continue;
          }
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
              mapper.update_bundle(id_bundle, timestamp)?;
            }
          }
          mapper.approve_transaction(id)?;
          counter += 1;
        }
      }
    }
    if counter > 0 {
      mapper.subtanble_confirmation_event(timestamp, counter)?;
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
    if Self::is_duplicate(&mut result)? {
      return Ok(None);
    }
    let timestamp = utils::milliseconds_since_epoch()?;
    let id_trunk = mapper.select_transaction_id_by_hash(self.trunk_hash)?;
    let id_branch = mapper.select_transaction_id_by_hash(self.branch_hash)?;
    if id_trunk.is_none() || id_branch.is_none() {
      mapper.unsolid_transaction_event(timestamp)?;
    }
    let id_trunk = Self::approve_or_insert_placeholder(
      mapper,
      counters,
      id_trunk,
      self.trunk_hash,
    )?;
    let id_branch = Self::approve_or_insert_placeholder(
      mapper,
      counters,
      id_branch,
      self.branch_hash,
    )?;
    let id_address = mapper.fetch_address(counters, self.address_hash)?;
    let id_bundle = mapper.fetch_bundle(
      counters,
      timestamp,
      self.bundle_hash,
      self.last_index,
    )?;
    let is_milestone = self.address_hash == milestone_address;
    let record = mapper::TransactionRecord {
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
      mapper.insert_transaction(counters, record)?;
    } else {
      mapper.update_transaction(record)?;
    }
    mapper.new_transaction_received_event(timestamp)?;
    if is_milestone {
      mapper.milestone_received_event(timestamp)?;
      Ok(Some(vec![id_trunk, id_branch]))
    } else {
      Ok(None)
    }
  }

  fn is_duplicate(result: &mut Option<mysql::Row>) -> Result<bool> {
    if let Some(ref mut row) = *result {
      let id_trunk = row.take_opt("id_trunk").ok_or(
        mapper::Error::ColumnNotFound,
      )?;
      let id_branch = row.take_opt("id_branch").ok_or(
        mapper::Error::ColumnNotFound,
      )?;
      if id_trunk.unwrap_or(0) != 0 && id_branch.unwrap_or(0) != 0 {
        return Ok(true);
      }
    }
    Ok(false)
  }

  fn approve_or_insert_placeholder(
    mapper: &mut Mapper,
    counters: &Counters,
    id: Option<u64>,
    hash: &str,
  ) -> Result<u64> {
    match id {
      Some(id) => {
        mapper.direct_approve_transaction(id)?;
        Ok(id)
      }
      None => Ok(mapper.insert_transaction_placeholder(counters, hash)?),
    }
  }
}
