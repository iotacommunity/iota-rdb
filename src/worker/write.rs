use counters::Counters;
use mysql;
use query::{DirectApproveTransaction, FetchAddress, FetchBundle,
            FindTransactions, FindTransactionsResult, InsertEvent,
            InsertTransactionPlaceholder, UpsertTransaction,
            UpsertTransactionRecord};
use std::sync::Arc;
use transaction::Transaction;
use utils;
use worker::{ApproveVec, Result, SolidateVec};

const NULL_HASH: &str = "999999999999999999999999999999999999999999999999999999999999999999999999999999999";

pub struct Write<'a> {
  find_transactions_query: FindTransactions<'a>,
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
      find_transactions_query: FindTransactions::new(pool)?,
      upsert_transaction_query: UpsertTransaction::new(pool, counters.clone())?,
      insert_transaction_placeholder_query:
        InsertTransactionPlaceholder::new(pool, counters.clone())?,
      direct_approve_transaction_query: DirectApproveTransaction::new(pool)?,
      fetch_address_query: FetchAddress::new(pool, counters.clone())?,
      fetch_bundle_query: FetchBundle::new(pool, counters.clone())?,
      insert_event_query: InsertEvent::new(pool)?,
    })
  }

  pub fn perform(
    &mut self,
    transaction: &mut Transaction,
  ) -> Result<(Option<ApproveVec>, Option<SolidateVec>)> {
    let (current_tx, trunk_tx, branch_tx) = self.find_transactions_query.exec(
      transaction.hash(),
      transaction.trunk_hash(),
      transaction.branch_hash(),
    )?;
    if let Some(ref record) = current_tx {
      if record.id_trunk != 0 && record.id_branch != 0 {
        return Ok((None, None));
      }
    }
    let timestamp = utils::milliseconds_since_epoch()?;
    let trunk_tx = self.check_parent(trunk_tx, transaction.trunk_hash())?;
    let branch_tx = if transaction.branch_hash() != transaction.trunk_hash() {
      self.check_parent(branch_tx, transaction.branch_hash())?
    } else {
      trunk_tx.clone()
    };
    let id_address = self.fetch_address_query.exec(transaction.address_hash())?;
    let id_bundle = self.fetch_bundle_query.exec(
      timestamp,
      transaction.bundle_hash(),
      transaction.last_index(),
    )?;
    let height = if transaction.solid() != 0b11 && trunk_tx.solid == 0b11 {
      trunk_tx.height + 1
    } else {
      0
    };
    if trunk_tx.solid == 0b11 {
      transaction.solidate(0b10);
    }
    if branch_tx.solid == 0b11 {
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
      id_trunk: trunk_tx.id_tx,
      id_branch: branch_tx.id_tx,
      id_address,
      id_bundle,
      height,
    };
    if current_tx.is_none() {
      self.upsert_transaction_query.insert(record)?;
    } else {
      self.upsert_transaction_query.update(record)?;
    }
    if transaction.solid() != 0b11 {
      self.insert_event_query.unsolid_transaction(timestamp)?;
    }
    self.insert_event_query.new_transaction_received(timestamp)?;
    let approve_data = if transaction.is_milestone() {
      self.insert_event_query.milestone_received(timestamp)?;
      Some(vec![trunk_tx.id_tx, branch_tx.id_tx])
    } else {
      None
    };
    let solidate_data =
      current_tx.and_then(|current_tx| if transaction.solid() == 0b11 {
        Some(vec![(current_tx.id_tx, Some(height))])
      } else {
        None
      });
    Ok((approve_data, solidate_data))
  }

  fn check_parent(
    &mut self,
    transaction: Option<FindTransactionsResult>,
    hash: &str,
  ) -> Result<FindTransactionsResult> {
    match transaction {
      Some(record) => {
        let id_tx = record.id_tx;
        self.direct_approve_transaction_query.exec(id_tx)?;
        Ok(record)
      }
      None => {
        let (height, solid) = (0, if hash == NULL_HASH { 0b11 } else { 0b00 });
        let id_tx = self
          .insert_transaction_placeholder_query
          .exec(hash, height, solid)?;
        Ok(FindTransactionsResult {
          id_tx,
          height,
          solid,
          id_trunk: 0,
          id_branch: 0,
        })
      }
    }
  }
}
