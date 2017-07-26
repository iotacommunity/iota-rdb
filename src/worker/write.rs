use counters::Counters;
use mysql;
use query::{DirectApproveTransaction, FetchAddress, FetchBundle,
            FindTransactionByHash, InsertEvent, InsertTransactionPlaceholder,
            UpsertTransaction, UpsertTransactionRecord};
use std::sync::Arc;
use transaction::Transaction;
use utils;
use worker::{ApproveVec, Result, SolidateVec};

const NULL_HASH: &str = "999999999999999999999999999999999999999999999999999999999999999999999999999999999";

pub struct Write<'a> {
  counters: Arc<Counters>,
  find_transaction_by_hash_query: FindTransactionByHash<'a>,
  upsert_transaction_query: UpsertTransaction<'a>,
  insert_transaction_placeholder_query: InsertTransactionPlaceholder<'a>,
  direct_approve_transaction_query: DirectApproveTransaction<'a>,
  fetch_address_query: FetchAddress<'a>,
  fetch_bundle_query: FetchBundle<'a>,
  insert_event_query: InsertEvent<'a>,
}

impl<'a> Write<'a> {
  pub fn new(pool: &mysql::Pool, counters: Arc<Counters>) -> Result<Self> {
    Ok(Self {
      find_transaction_by_hash_query: FindTransactionByHash::new(pool)?,
      upsert_transaction_query: UpsertTransaction::new(pool)?,
      insert_transaction_placeholder_query:
        InsertTransactionPlaceholder::new(pool)?,
      direct_approve_transaction_query: DirectApproveTransaction::new(pool)?,
      fetch_address_query: FetchAddress::new(pool)?,
      fetch_bundle_query: FetchBundle::new(pool)?,
      insert_event_query: InsertEvent::new(pool)?,
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
      .fetch_address_query
      .exec(&self.counters, transaction.address_hash())?;
    let id_bundle = self.fetch_bundle_query.exec(
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
    let record = UpsertTransactionRecord {
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
      self
        .upsert_transaction_query
        .insert(&self.counters, record)?;
    } else {
      self.upsert_transaction_query.update(record)?;
    }
    if transaction.solid() != 0b11 {
      self.insert_event_query.unsolid_transaction(timestamp)?;
    }
    self.insert_event_query.new_transaction_received(timestamp)?;
    let approve_data = if transaction.is_milestone() {
      self.insert_event_query.milestone_received(timestamp)?;
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
        self.direct_approve_transaction_query.exec(id_tx)?;
        Ok((id_tx, record.height?, record.solid?))
      }
      None => {
        let (height, solid) = (0, if hash == NULL_HASH { 0b11 } else { 0b00 });
        let id_tx = self
          .insert_transaction_placeholder_query
          .exec(&self.counters, hash, height, solid)?;
        Ok((id_tx, height, solid))
      }
    }
  }
}
