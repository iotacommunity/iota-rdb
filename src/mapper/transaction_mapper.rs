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
  records: Mutex<(HashMap<u64, Transaction>, HashMap<String, u64>)>,
  null_hash: String,
}

impl TransactionMapper {
  pub fn new(counter: Arc<Counter>) -> Result<Self> {
    let records = Mutex::new((HashMap::new(), HashMap::new()));
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
    self.fetch_results(&results, current_hash, trunk_hash, branch_hash)
  }

  pub fn insert(
    &self,
    conn: &mut mysql::Conn,
    mut current_tx: Transaction,
    mut trunk_tx: Transaction,
    mut branch_tx: Transaction,
  ) -> Result<()> {
    let (ref mut records, _) = *self.records.lock().unwrap();
    current_tx.insert(conn)?;
    current_tx.unlock();
    trunk_tx.unlock();
    branch_tx.unlock();
    records.insert(current_tx.id_tx(), current_tx);
    records.insert(trunk_tx.id_tx(), trunk_tx);
    records.insert(branch_tx.id_tx(), branch_tx);
    Ok(())
  }

  pub fn update(&self, conn: &mut mysql::Conn) -> Result<()> {
    let (ref mut records, _) = *self.records.lock().unwrap();
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
    let (_, ref hashes) = *self.records.lock().unwrap();
    let mut result = vec![current_hash, trunk_hash, branch_hash];
    result.sort();
    result.dedup();
    result.retain(|hash| hashes.contains_key(*hash));
    result
  }

  fn fetch_results(
    &self,
    results: &[(&str, Transaction)],
    current_hash: &str,
    trunk_hash: &str,
    branch_hash: &str,
  ) -> Result<Option<(Transaction, Transaction, Transaction)>> {
    let (ref mut records, ref mut hashes) = *self.records.lock().unwrap();
    let mut current_tx =
      self.fetch_result(records, hashes, results, current_hash);
    if current_tx.is_persistent() {
      return Ok(None);
    }
    let mut trunk_tx = self.fetch_result(records, hashes, results, trunk_hash);
    let mut branch_tx = if branch_hash != trunk_hash {
      self.fetch_result(records, hashes, results, branch_hash)
    } else {
      trunk_tx.clone()
    };
    if current_tx.is_locked() || trunk_tx.is_locked() || branch_tx.is_locked() {
      return Err(Error::Locked);
    }
    current_tx.lock();
    current_tx.store(records, hashes);
    trunk_tx.lock();
    trunk_tx.direct_approve();
    trunk_tx.store(records, hashes);
    if branch_tx.id_tx() != trunk_tx.id_tx() {
      branch_tx.lock();
      branch_tx.direct_approve();
      branch_tx.store(records, hashes);
    }
    Ok(Some((current_tx, trunk_tx, branch_tx)))
  }

  fn fetch_result(
    &self,
    records: &HashMap<u64, Transaction>,
    hashes: &HashMap<String, u64>,
    results: &[(&str, Transaction)],
    hash: &str,
  ) -> Transaction {
    hashes
      .get(hash)
      .and_then(|id_tx| records.get(id_tx))
      .cloned()
      .unwrap_or_else(|| {
        results
          .iter()
          .find(|&&(current_hash, _)| current_hash == hash)
          .map(|&(_, ref result)| result.clone())
          .unwrap_or_else(|| self.create_placeholder(hash))
      })
  }

  fn create_placeholder(&self, hash: &str) -> Transaction {
    let id_tx = self.counter.next_transaction();
    let solid = if hash == self.null_hash { 0b11 } else { 0b00 };
    Transaction::placeholder(hash.to_owned(), id_tx, solid)
  }
}
