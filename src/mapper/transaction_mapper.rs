use super::{Error, Result};
use counter::Counter;
use mapper::Transaction;
use mysql;
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
  ) -> Result<Option<(Transaction, Transaction, Transaction)>> {
    if current_hash == self.null_hash &&
      (current_hash == trunk_hash || current_hash == branch_hash)
    {
      return Ok(None);
    }
    let mut hashes = vec![current_hash, trunk_hash, branch_hash];
    let mut results = Vec::new();
    hashes.sort();
    hashes.dedup();
    hashes.retain(|hash| if let Some(record) = self.records.get(*hash) {
      results.push((*hash, record.clone()));
      false
    } else {
      true
    });
    for (result, hash) in
      Transaction::find(conn, &hashes)?.into_iter().zip(hashes)
    {
      if let Some(result) = result {
        results.push((hash, result));
      }
    }
    let current_tx = self.fetch_result(&results, current_hash);
    if current_tx.is_persistent() {
      return Ok(None);
    }
    let mut trunk_tx = self.fetch_result(&results, trunk_hash);
    trunk_tx.direct_approve();
    self.store(trunk_hash, trunk_tx.clone());
    let mut branch_tx;
    if branch_hash != trunk_hash {
      branch_tx = self.fetch_result(&results, branch_hash);
      branch_tx.direct_approve();
      self.store(branch_hash, branch_tx.clone());
    } else {
      branch_tx = trunk_tx.clone();
    }
    Ok(Some((current_tx, trunk_tx, branch_tx)))
  }

  pub fn update(
    &mut self,
    conn: &mut mysql::Conn,
    hash: &str,
    mut transaction: Transaction,
  ) -> Result<()> {
    if transaction.is_modified() {
      if !transaction.is_persistent() {
        transaction.insert(conn, hash)?;
      }
      self.store(hash, transaction);
    }
    Ok(())
  }

  fn store(&mut self, hash: &str, transaction: Transaction) {
    self.records.insert(hash.to_owned(), transaction);
  }

  fn fetch_result(
    &mut self,
    results: &[(&str, Transaction)],
    hash: &str,
  ) -> Transaction {
    results
      .iter()
      .find(|&&(current_hash, _)| current_hash == hash)
      .map(|&(_, ref result)| result.clone())
      .unwrap_or_else(|| {
        let id_tx = self.counter.next_transaction();
        let solid = if hash == self.null_hash { 0b11 } else { 0b00 };
        let record = Transaction::placeholder(id_tx, solid);
        self.store(hash, record.clone());
        record
      })
  }
}
