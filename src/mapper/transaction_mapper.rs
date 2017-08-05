use super::{Error, Result};
use counter::Counter;
use mapper::Transaction;
use mysql;
use query;
use std::collections::HashMap;
use std::sync::Arc;
use utils;

const HASH_SIZE: usize = 81;

pub struct TransactionMapper {
  counter: Arc<Counter>,
  records: HashMap<String, Transaction>,
  null_hash: String,
}

impl TransactionMapper {
  pub fn new(counter: Arc<Counter>) -> Result<Self> {
    let records = HashMap::new();
    let null_hash = utils::trits_string(0, HASH_SIZE)
      .ok_or(Error::NullHashToTrits)?;
    Ok(Self {
      counter,
      records,
      null_hash,
    })
  }

  pub fn fetch(
    &mut self,
    conn: &mut mysql::Conn,
    current_hash: &str,
    trunk_hash: &str,
    branch_hash: &str,
  ) -> Result<Option<(Option<Transaction>, Transaction, Transaction)>> {
    if current_hash == self.null_hash {
      return Ok(None);
    }
    // TODO check records first
    let result =
      query::find_transactions(conn, &[current_hash, trunk_hash, branch_hash])?;
    let mut iter = result.iter();
    let current_tx =
      iter.next().and_then(|x| x.as_ref().map(Transaction::from));
    let trunk_tx = iter.next().and_then(|x| x.as_ref().map(Transaction::from));
    let branch_tx = iter.next().and_then(|x| x.as_ref().map(Transaction::from));
    if let Some(ref record) = current_tx {
      if record.id_trunk() != 0 && record.id_branch() != 0 {
        return Ok(None);
      }
    }
    let trunk_tx = self.check_parent(conn, trunk_tx, trunk_hash)?;
    let branch_tx = if branch_hash != trunk_hash {
      self.check_parent(conn, branch_tx, branch_hash)?
    } else {
      trunk_tx.clone()
    };
    Ok(Some((current_tx, trunk_tx, branch_tx)))
  }

  pub fn upsert(
    &self,
    conn: &mut mysql::Conn,
    current_tx: &Option<Transaction>,
    record: query::UpsertTransactionRecord,
  ) -> Result<()> {
    if current_tx.is_none() {
      query::insert_transaction(conn, &self.counter, &record)?;
      Ok(())
    } else {
      query::update_transaction(conn, &record)?;
      Ok(())
    }
  }

  fn check_parent(
    &mut self,
    conn: &mut mysql::Conn,
    transaction: Option<Transaction>,
    hash: &str,
  ) -> Result<Transaction> {
    match transaction {
      Some(record) => {
        query::direct_approve_transaction(conn, record.id_tx())?;
        Ok(record)
      }
      None => {
        let id_tx = self.counter.next_transaction();
        let solid = if hash == self.null_hash { 0b11 } else { 0b00 };
        let record = Transaction::placeholder(id_tx, 0, solid);
        self
          .records
          .entry(hash.to_owned())
          .or_insert_with(|| record.clone());
        Ok(record)
      }
    }
  }
}
