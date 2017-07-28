use counters::Counters;
use mysql;
use query::{self, event};
use std::sync::Arc;
use transaction::Transaction;
use utils;
use worker::{ApproveVec, Error, Result, SolidateVec};

const NULL_HASH: &str = "999999999999999999999999999999999999999999999999999999999999999999999999999999999";

pub struct Write {
  conn: mysql::Conn,
  counters: Arc<Counters>,
}

impl Write {
  pub fn new(mysql_uri: &str, counters: Arc<Counters>) -> Result<Self> {
    let conn = mysql::Conn::new(mysql_uri)?;
    Ok(Self { conn, counters })
  }

  pub fn perform(
    &mut self,
    transaction: &Transaction,
    verbose: bool,
    thread_number: usize,
  ) -> Result<(Option<ApproveVec>, Option<SolidateVec>)> {
    loop {
      let mut conn = self.conn.start_transaction(
        true,
        Some(mysql::IsolationLevel::RepeatableRead),
        Some(false),
      )?;
      match transaction_block(&mut conn, &self.counters, transaction) {
        Ok(data) => {
          conn.commit()?;
          return Ok(data);
        }
        Err(
          Error::Query(query::Error::Mysql(mysql::Error::MySqlError(ref err))),
        ) if err.code == 1213 => {
          if verbose {
            println!("[w#{}] Found a conflict; retrying", thread_number);
          }
          continue;
        }
        Err(err) => return Err(err),
      }
    }
  }
}

fn transaction_block(
  conn: &mut mysql::Transaction,
  counters: &Counters,
  transaction: &Transaction,
) -> Result<(Option<ApproveVec>, Option<SolidateVec>)> {
  let (current_tx, trunk_tx, branch_tx) = query::find_transactions(
    conn,
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
  let trunk_tx =
    check_parent(conn, counters, trunk_tx, transaction.trunk_hash())?;
  let branch_tx = if transaction.branch_hash() != transaction.trunk_hash() {
    check_parent(conn, counters, branch_tx, transaction.branch_hash())?
  } else {
    trunk_tx.clone()
  };
  let id_address =
    query::fetch_address(conn, counters, transaction.address_hash())?;
  let id_bundle = query::fetch_bundle(
    conn,
    counters,
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
    query::insert_transaction(conn, counters, &record)?;
  } else {
    query::update_transaction(conn, &record)?;
  }
  if solid != 0b11 {
    event::unsolid_transaction(conn, timestamp)?;
  }
  event::new_transaction_received(conn, timestamp)?;
  let approve_data = if transaction.is_milestone() {
    event::milestone_received(conn, timestamp)?;
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
  conn: &mut mysql::Transaction,
  counters: &Counters,
  transaction: Option<query::FindTransactionsResult>,
  hash: &str,
) -> Result<query::FindTransactionsResult> {
  match transaction {
    Some(record) => {
      let id_tx = record.id_tx;
      query::direct_approve_transaction(conn, id_tx)?;
      Ok(record)
    }
    None => {
      let (height, solid) = (0, if hash == NULL_HASH { 0b11 } else { 0b00 });
      let id_tx = query::insert_transaction_placeholder(
        conn,
        counters,
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
