use super::{Error, Result};
use counter::Counter;
use mapper::Transaction;
use mysql;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use utils;

const HASH_SIZE: usize = 81;

pub struct TransactionMapper {
  counter: Arc<Counter>,
  records: Mutex<HashMap<String, Transaction>>,
  null_hash: String,
}

impl TransactionMapper {
  pub fn new(counter: Arc<Counter>) -> Result<Self> {
    let records = Mutex::new(HashMap::new());
    let null_hash = utils::trits_string(0, HASH_SIZE)
      .ok_or(Error::NullHashToTrits)?;
    Ok(Self {
      counter,
      records,
      null_hash,
    })
  }

  pub fn fetch(
    &self,
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
    let mut results = Vec::new();
    let hashes = self.absent_hashes(current_hash, trunk_hash, branch_hash);
    for (result, hash) in
      Transaction::find(conn, &hashes)?.into_iter().zip(hashes)
    {
      if let Some(result) = result {
        results.push((hash, result));
      }
    }
    let current_tx = self.fetch_current(&results, current_hash);
    if current_tx.is_persistent() {
      return Ok(None);
    }
    let trunk_tx = self.fetch_parent(&results, trunk_hash);
    let branch_tx = if branch_hash != trunk_hash {
      self.fetch_parent(&results, branch_hash)
    } else {
      trunk_tx.clone()
    };
    Ok(Some((current_tx, trunk_tx, branch_tx)))
  }

  pub fn update(
    &self,
    conn: &mut mysql::Conn,
    hash: &str,
    mut record: Transaction,
  ) -> Result<()> {
    if record.is_modified() {
      if !record.is_persistent() {
        record.insert(conn, hash)?;
      }
      let mut records = self.records.lock().expect("Mutex is poisoned");
      records.insert(hash.to_owned(), record);
    }
    Ok(())
  }

  pub fn flush(&self, conn: &mut mysql::Conn) -> Result<()> {
    let mut records = self.records.lock().expect("Mutex is poisoned");
    for record in records.values_mut() {
      if record.is_modified() {
        record.update(conn)?;
      }
    }
    Ok(())
  }

  fn absent_hashes<'a>(
    &self,
    current_hash: &'a str,
    trunk_hash: &'a str,
    branch_hash: &'a str,
  ) -> Vec<&'a str> {
    let records = self.records.lock().expect("Mutex is poisoned");
    let mut hashes = vec![current_hash, trunk_hash, branch_hash];
    hashes.sort();
    hashes.dedup();
    hashes.retain(|hash| records.contains_key(*hash));
    hashes
  }

  fn fetch_current(
    &self,
    results: &[(&str, Transaction)],
    hash: &str,
  ) -> Transaction {
    let records = self.records.lock().expect("Mutex is poisoned");
    self.fetch_result(&records, results, hash)
  }

  fn fetch_parent(
    &self,
    results: &[(&str, Transaction)],
    hash: &str,
  ) -> Transaction {
    let mut records = self.records.lock().expect("Mutex is poisoned");
    let mut record = self.fetch_result(&records, results, hash);
    record.direct_approve();
    records.insert(hash.to_owned(), record.clone());
    record
  }

  fn fetch_result(
    &self,
    records: &HashMap<String, Transaction>,
    results: &[(&str, Transaction)],
    hash: &str,
  ) -> Transaction {
    results
      .iter()
      .find(|&&(current_hash, _)| current_hash == hash)
      .map(|&(_, ref result)| result.clone())
      .unwrap_or_else(|| {
        records
          .get(hash)
          .cloned()
          .unwrap_or_else(|| self.create_placeholder(hash))
      })
  }

  fn create_placeholder(&self, hash: &str) -> Transaction {
    let id_tx = self.counter.next_transaction();
    let solid = if hash == self.null_hash { 0b11 } else { 0b00 };
    Transaction::placeholder(id_tx, solid)
  }
}
