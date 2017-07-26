use counters::Counters;
use mapper::{Mapper, NewTransaction};
use mysql;
use query::FindTransactionByHash;
use std::sync::Arc;
use transaction::Transaction;
use utils;
use worker::{ApproveVec, Result, SolidateVec};

const NULL_HASH: &str = "999999999999999999999999999999999999999999999999999999999999999999999999999999999";

pub struct Write<'a> {
  mapper: Mapper<'a>,
  counters: Arc<Counters>,
  find_transaction_by_hash_query: FindTransactionByHash<'a>,
}

impl<'a> Write<'a> {
  pub fn new(
    pool: &mysql::Pool,
    mapper: Mapper<'a>,
    counters: Arc<Counters>,
  ) -> Result<Self> {
    Ok(Self {
      find_transaction_by_hash_query: FindTransactionByHash::new(pool)?,
      mapper,
      counters,
    })
  }

  pub fn perform(
    &mut self,
    transaction: &mut Transaction,
  ) -> Result<(Option<ApproveVec>, Option<SolidateVec>)> {
    let result = self
      .find_transaction_by_hash_query
      .exec(transaction.hash())?;
    let id_tx = if let Some(record) = result {
      if record.id_trunk.unwrap_or(0) != 0 &&
        record.id_branch.unwrap_or(0) != 0
      {
        return Ok((None, None));
      }
      Some(record.id_tx?)
    } else {
      None
    };
    let timestamp = utils::milliseconds_since_epoch()?;
    let (id_trunk, trunk_height, trunk_solid) =
      self.check_node(transaction.trunk_hash())?;
    let (id_branch, _, branch_solid) =
      self.check_node(transaction.branch_hash())?;
    let id_address = self
      .mapper
      .fetch_address(&self.counters, transaction.address_hash())?;
    let id_bundle = self.mapper.fetch_bundle(
      &self.counters,
      timestamp,
      transaction.bundle_hash(),
      transaction.last_index(),
    )?;
    let height = if transaction.solid() != 0b11 && trunk_solid == 0b11 {
      trunk_height + 1
    } else {
      0
    };
    if trunk_solid == 0b11 {
      transaction.solidate(0b10);
    }
    if branch_solid == 0b11 {
      transaction.solidate(0b01);
    }
    let record = NewTransaction {
      hash: transaction.hash(),
      tag: transaction.tag(),
      value: transaction.value(),
      timestamp: transaction.timestamp(),
      current_idx: transaction.current_index(),
      last_idx: transaction.last_index(),
      is_mst: transaction.is_milestone(),
      mst_a: transaction.is_milestone(),
      solid: transaction.solid(),
      id_trunk,
      id_branch,
      id_address,
      id_bundle,
      height,
    };
    if id_tx.is_none() {
      self.mapper.insert_transaction(&self.counters, record)?;
    } else {
      self.mapper.update_transaction(record)?;
    }
    if transaction.solid() != 0b11 {
      self.mapper.unsolid_transaction_event(timestamp)?;
    }
    self.mapper.new_transaction_received_event(timestamp)?;
    let approve_data = if transaction.is_milestone() {
      self.mapper.milestone_received_event(timestamp)?;
      Some(vec![id_trunk, id_branch])
    } else {
      None
    };
    let solidate_data =
      id_tx.and_then(|id_tx| if transaction.solid() == 0b11 {
        Some(vec![(id_tx, Some(height))])
      } else {
        None
      });
    Ok((approve_data, solidate_data))
  }

  fn check_node(&mut self, hash: &str) -> Result<(u64, i32, u8)> {
    match self.find_transaction_by_hash_query.exec(hash)? {
      Some(record) => {
        let id_tx = record.id_tx?;
        self.mapper.direct_approve_transaction(id_tx)?;
        Ok((id_tx, record.height?, record.solid?))
      }
      None => {
        let (height, solid) = (0, if hash == NULL_HASH { 0b11 } else { 0b00 });
        let id_tx = self
          .mapper
          .insert_transaction_placeholder(&self.counters, hash, height, solid)?;
        Ok((id_tx, height, solid))
      }
    }
  }
}
