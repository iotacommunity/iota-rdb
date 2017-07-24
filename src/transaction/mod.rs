mod error;

pub use self::error::{Error, Result};
use counters::Counters;
use mapper::{self, Mapper, NewTransaction};
use utils;

pub const TAG_LENGTH: usize = 27;
const NULL_HASH: &str = "999999999999999999999999999999999999999999999999999999999999999999999999999999999";

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
  solid: bool,
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
    let solid = is_milestone && tag == milestone_start_index;
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

  pub fn approve(mapper: &mut Mapper, mut ids: Vec<u64>) -> Result<()> {
    let (timestamp, mut counter) = (utils::milliseconds_since_epoch()?, 0);
    while let Some(id) = ids.pop() {
      let record = mapper.select_transaction_by_id(id)?;
      if record.mst_a.unwrap_or(false) {
        continue;
      }
      let id_trunk = record.id_trunk.unwrap_or(0);
      let id_branch = record.id_branch.unwrap_or(0);
      if id_trunk != 0 {
        ids.push(id_trunk);
      }
      if id_branch != 0 {
        ids.push(id_branch);
      }
      if let Ok(0) = record.current_idx {
        if let Ok(id_bundle) = record.id_bundle {
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
    let id = mapper
      .select_transaction_by_hash(hash)?
      .ok_or(mapper::Error::RecordNotFound)?
      .id_tx?;
    let mut nodes = vec![(id, Some(0))];
    while let Some((parent_id, parent_height)) = nodes.pop() {
      let (mut trunk, mut branch) = (Vec::new(), Vec::new());
      for record in mapper.select_child_transactions(parent_id)? {
        if record.id_trunk? == parent_id {
          trunk.push((record.id_tx?, record.height?, record.solid?));
        } else if record.id_branch? == parent_id {
          branch.push((record.id_tx?, record.height?, record.solid?));
        }
      }
      Self::solidate_nodes(mapper, &mut nodes, &trunk, 0b10, parent_height)?;
      Self::solidate_nodes(mapper, &mut nodes, &branch, 0b01, None)?;
      counter += trunk.len() as i32;
      counter += branch.len() as i32;
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
    let result = mapper.select_transaction_by_hash(self.hash)?;
    let new_record = if let Some(record) = result {
      if record.id_trunk.unwrap_or(0) != 0 &&
        record.id_branch.unwrap_or(0) != 0
      {
        return Ok((None, None));
      }
      false
    } else {
      true
    };
    let timestamp = utils::milliseconds_since_epoch()?;
    let (id_trunk, trunk_solid) =
      Self::check_node(mapper, counters, self.trunk_hash)?;
    let (id_branch, branch_solid) =
      Self::check_node(mapper, counters, self.branch_hash)?;
    let id_address = mapper.fetch_address(counters, self.address_hash)?;
    let id_bundle = mapper
      .fetch_bundle(counters, timestamp, self.bundle_hash, self.last_index)?;
    let solid = if self.solid {
      0b11
    } else {
      (if trunk_solid == 0b11 { 0b10 } else { 0b00 }) |
        (if branch_solid == 0b11 { 0b01 } else { 0b00 })
    };
    let record = NewTransaction {
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
      solid: solid,
    };
    if new_record {
      mapper.insert_transaction(counters, record)?;
    } else {
      mapper.update_transaction(record)?;
    }
    if solid != 0b11 {
      mapper.unsolid_transaction_event(timestamp)?;
    }
    mapper.new_transaction_received_event(timestamp)?;
    let approve_ids = if self.is_milestone {
      mapper.milestone_received_event(timestamp)?;
      Some(vec![id_trunk, id_branch])
    } else {
      None
    };
    let solid_hash = if self.solid && !new_record {
      Some(self.hash.to_owned())
    } else {
      None
    };
    Ok((approve_ids, solid_hash))
  }

  fn check_node(
    mapper: &mut Mapper,
    counters: &Counters,
    hash: &str,
  ) -> Result<(u64, u8)> {
    match mapper.select_transaction_by_hash(hash)? {
      Some(record) => {
        let id_tx = record.id_tx?;
        mapper.direct_approve_transaction(id_tx)?;
        Ok((id_tx, record.solid?))
      }
      None => {
        let solid = if hash == NULL_HASH { 0b11 } else { 0b00 };
        Ok((
          mapper
            .insert_transaction_placeholder(counters, hash, solid)?,
          solid,
        ))
      }
    }
  }

  fn solidate_nodes(
    mapper: &mut Mapper,
    nodes: &mut Vec<(u64, Option<i32>)>,
    ids: &[(u64, i32, u8)],
    solid: u8,
    height: Option<i32>,
  ) -> Result<()> {
    for &(id, mut node_height, mut node_solid) in ids {
      if node_solid & solid != 0b00 {
        continue;
      }
      node_solid |= solid;
      match height {
        Some(height) => {
          node_height = height + 1;
          mapper
            .solidate_trunk_transaction(id, node_height, node_solid)?;
        }
        None => {
          mapper.solidate_branch_transaction(id, node_solid)?;
        }
      }
      if node_solid == 0b11 {
        let node_height = if node_height > 0 {
          Some(node_height)
        } else {
          None
        };
        nodes.push((id, node_height));
      }
    }
    Ok(())
  }
}
