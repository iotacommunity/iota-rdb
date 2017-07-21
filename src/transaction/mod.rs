mod error;

pub use self::error::{Error, Result};
use counters::Counters;
use mapper::{self, Mapper};
use mysql;
use utils;

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
  is_solid: bool,
}

pub type ApproveIds = Option<Vec<u64>>;
pub type SolidHash = Option<String>;

impl<'a> Transaction<'a> {
  pub fn parse(
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
    let is_solid = is_milestone && tag == milestone_start_index;
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
      is_solid,
    })
  }

  pub fn approve(mapper: &mut Mapper, mut ids: Vec<u64>) -> Result<()> {
    let (timestamp, mut counter) = (utils::milliseconds_since_epoch()?, 0);
    while let Some(id) = ids.pop() {
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
    if counter > 0 {
      mapper.subtanble_confirmation_event(timestamp, counter)?;
    }
    Ok(())
  }

  pub fn solidate(mapper: &mut Mapper, hash: &str) -> Result<()> {
    let (timestamp, mut counter) = (utils::milliseconds_since_epoch()?, 0);
    let mut ids = Vec::new();
    ids.push(mapper
      .select_transaction_by_hash(hash)?
      .ok_or(mapper::Error::RecordNotFound)?
      .take_opt("id_tx")
      .ok_or(mapper::Error::ColumnNotFound)??);
    while let Some(parent_id) = ids.pop() {
      let mut child_ids = Vec::new();
      for row in mapper.select_child_transactions(parent_id)? {
        let id = row?.take_opt("id_tx").ok_or(mapper::Error::ColumnNotFound)??;
        child_ids.push(id);
      }
      for id in child_ids {
        mapper.solidate_transaction(id)?;
        counter += 1;
        ids.push(id);
      }
    }
    if counter > 0 {
      mapper.subtangle_solidation_event(timestamp, counter)?;
    }
    Ok(())
  }

  pub fn process(
    &mut self,
    mapper: &mut Mapper,
    counters: &Counters,
  ) -> Result<(ApproveIds, SolidHash)> {
    let mut result = mapper.select_transaction_by_hash(self.hash)?;
    if Self::is_duplicate(&mut result)? {
      return Ok((None, None));
    }
    let timestamp = utils::milliseconds_since_epoch()?;
    let (id_trunk, trunk_is_solid) =
      Self::check_node(mapper, counters, self.trunk_hash)?;
    let (id_branch, branch_is_solid) =
      Self::check_node(mapper, counters, self.branch_hash)?;
    if !self.is_solid {
      self.is_solid = trunk_is_solid && branch_is_solid;
    }
    let id_address = mapper.fetch_address(counters, self.address_hash)?;
    let id_bundle = mapper.fetch_bundle(
      counters,
      timestamp,
      self.bundle_hash,
      self.last_index,
    )?;
    let record = mapper::TransactionRecord {
      hash: self.hash,
      id_trunk: id_trunk,
      id_branch: id_branch,
      id_address: id_address,
      id_bundle: id_bundle,
      tag: self.tag,
      value: self.value,
      timestamp: self.timestamp,
      current_idx: self.current_index,
      last_idx: self.last_index,
      is_mst: self.is_milestone,
      mst_a: self.is_milestone,
      solid: self.is_solid,
    };
    if result.is_none() {
      mapper.insert_transaction(counters, record)?;
    } else {
      mapper.update_transaction(record)?;
    }
    if !self.is_solid {
      mapper.unsolid_transaction_event(timestamp)?;
    }
    mapper.new_transaction_received_event(timestamp)?;
    let approve_ids = if self.is_milestone {
      mapper.milestone_received_event(timestamp)?;
      Some(vec![id_trunk, id_branch])
    } else {
      None
    };
    let solid_hash = if self.is_solid && !result.is_none() {
      Some(self.hash.to_owned())
    } else {
      None
    };
    Ok((approve_ids, solid_hash))
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

  fn check_node(
    mapper: &mut Mapper,
    counters: &Counters,
    hash: &str,
  ) -> Result<(u64, bool)> {
    match mapper.select_transaction_by_hash(hash)? {
      Some(mut result) => {
        let id_tx = result.take_opt("id_tx").ok_or(
          mapper::Error::ColumnNotFound,
        )??;
        let is_solid = result.take_opt("solid").ok_or(
          mapper::Error::ColumnNotFound,
        )??;
        mapper.direct_approve_transaction(id_tx)?;
        Ok((id_tx, is_solid))
      }
      None => Ok((
        mapper.insert_transaction_placeholder(counters, hash)?,
        false,
      )),
    }
  }
}
