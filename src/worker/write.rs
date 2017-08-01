use counters::Counters;
use mysql;
use query::{self, event};
use transaction::Transaction;
use utils;
use worker::{ApproveVec, Error, Result, SolidateVec};

const HASH_SIZE: usize = 81;

pub struct Write {
  conn: mysql::Conn,
  counters: Counters,
  null_hash: String,
}

impl Write {
  pub fn new(mysql_uri: &str, counters: Counters) -> Result<Self> {
    let conn = mysql::Conn::new(mysql_uri)?;
    let null_hash = utils::trits_string(0, HASH_SIZE)
      .ok_or(Error::NullHashToTrits)?;
    Ok(Self {
      conn,
      counters,
      null_hash,
    })
  }

  pub fn perform(
    &mut self,
    transaction: &Transaction,
  ) -> Result<(Option<ApproveVec>, Option<SolidateVec>)> {
    if transaction.hash() == self.null_hash {
      return Ok((None, None));
    }
    let (current_tx, trunk_tx, branch_tx) = query::find_transactions(
      &mut self.conn,
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
    let id_address = query::fetch_address(
      &mut self.conn,
      &self.counters,
      transaction.address_hash(),
    )?;
    let id_bundle = query::fetch_bundle(
      &mut self.conn,
      &self.counters,
      timestamp,
      transaction.bundle_hash(),
      transaction.last_index(),
    )?;
    let mut solid = transaction.solid();
    let height = if solid != 0b11 && trunk_tx.solid == 0b11 {
      trunk_tx.height + 1
    } else {
      0
    };
    if trunk_tx.solid == 0b11 {
      solid |= 0b10;
    }
    if branch_tx.solid == 0b11 {
      solid |= 0b01;
    }
    let record = query::UpsertTransactionRecord {
      hash: transaction.hash(),
      tag: transaction.tag(),
      value: transaction.value(),
      timestamp: transaction.timestamp(),
      current_idx: transaction.current_index(),
      last_idx: transaction.last_index(),
      is_mst: transaction.is_milestone(),
      mst_a: transaction.is_milestone(),
      id_trunk: trunk_tx.id_tx,
      id_branch: branch_tx.id_tx,
      id_address,
      id_bundle,
      height,
      solid,
    };
    if current_tx.is_none() {
      query::insert_transaction(&mut self.conn, &self.counters, &record)?;
    } else {
      query::update_transaction(&mut self.conn, &record)?;
    }
    if solid != 0b11 {
      event::unsolid_transaction(&mut self.conn, timestamp)?;
    }
    event::new_transaction_received(&mut self.conn, timestamp)?;
    let approve_data = if transaction.is_milestone() {
      event::milestone_received(&mut self.conn, timestamp)?;
      Some(vec![trunk_tx.id_tx, branch_tx.id_tx])
    } else {
      None
    };
    let solidate_data = current_tx.and_then(|current_tx| if solid == 0b11 {
      Some(vec![(current_tx.id_tx, Some(height))])
    } else {
      None
    });
    Ok((approve_data, solidate_data))
  }

  fn check_parent(
    &mut self,
    transaction: Option<query::FindTransactionsResult>,
    hash: &str,
  ) -> Result<query::FindTransactionsResult> {
    match transaction {
      Some(record) => {
        let id_tx = record.id_tx;
        query::direct_approve_transaction(&mut self.conn, id_tx)?;
        Ok(record)
      }
      None => {
        let height = 0;
        let solid = if hash == self.null_hash { 0b11 } else { 0b00 };
        let id_tx = query::insert_transaction_placeholder(
          &mut self.conn,
          &self.counters,
          hash,
          height,
          solid,
        )?;
        Ok(query::FindTransactionsResult {
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
