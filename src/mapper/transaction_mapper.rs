use super::{Error, Result};
use counter::Counter;
use mapper::{Record, RecordGuard, Transaction};
use mysql;
use std::collections::hash_map::{Entry, HashMap};
use std::sync::{Arc, Mutex, MutexGuard};
use utils;

const HASH_SIZE: usize = 81;

type Data = (HashMap<u64, Transaction>, HashMap<String, u64>);

pub struct TransactionMapper {
  counter: Arc<Counter>,
  data: Mutex<Data>,
  null_hash: String,
}

impl TransactionMapper {
  pub fn new(counter: Arc<Counter>) -> Result<Self> {
    let data = Mutex::new((HashMap::new(), HashMap::new()));
    let null_hash = utils::trits_string(0, HASH_SIZE)
      .ok_or(Error::NullHashToTrits)?;
    Ok(Self {
      counter,
      data,
      null_hash,
    })
  }

  pub fn lock(&self) -> MutexGuard<Data> {
    self.data.lock().unwrap()
  }

  pub fn fetch<'a>(
    &self,
    guard: &'a mut MutexGuard<Data>,
    conn: &mut mysql::Conn,
    id: u64,
  ) -> Result<RecordGuard<'a, Transaction>> {
    let (ref mut records, _) = **guard;
    let record = match records.entry(id) {
      Entry::Occupied(entry) => {
        let mut record = entry.into_mut();
        if record.is_locked() {
          return Err(Error::Locked);
        } else {
          record.lock();
          record
        }
      }
      Entry::Vacant(entry) => {
        let mut record = Transaction::find_by_id(conn, id)?;
        record.lock();
        entry.insert(record)
      }
    };
    Ok(RecordGuard::new(record))
  }

  pub fn fetch_triplet(
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
    let hashes = self.absent_hashes(current_hash, trunk_hash, branch_hash);
    let results = Transaction::find_by_hashes(conn, &hashes)?;
    self.fetch_triplet_results(&results, current_hash, trunk_hash, branch_hash)
  }

  pub fn insert(
    &self,
    conn: &mut mysql::Conn,
    mut current_tx: Transaction,
    mut trunk_tx: Transaction,
    mut branch_tx: Transaction,
  ) -> Result<()> {
    let (ref mut records, _) = *self.data.lock().unwrap();
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
    let (ref mut records, _) = *self.data.lock().unwrap();
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
    let (_, ref hashes) = *self.data.lock().unwrap();
    let mut result = vec![current_hash, trunk_hash, branch_hash];
    result.sort();
    result.dedup();
    result.retain(|hash| hashes.contains_key(*hash));
    result
  }

  fn fetch_triplet_results(
    &self,
    results: &[Transaction],
    current_hash: &str,
    trunk_hash: &str,
    branch_hash: &str,
  ) -> Result<Option<(Transaction, Transaction, Transaction)>> {
    let (ref mut records, ref mut hashes) = *self.data.lock().unwrap();
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
    results: &[Transaction],
    hash: &str,
  ) -> Transaction {
    hashes
      .get(hash)
      .and_then(|id_tx| records.get(id_tx))
      .cloned()
      .unwrap_or_else(|| {
        results
          .iter()
          .find(|result| result.hash() == hash)
          .cloned()
          .unwrap_or_else(|| self.create_placeholder(hash))
      })
  }

  fn create_placeholder(&self, hash: &str) -> Transaction {
    let id_tx = self.counter.next_transaction();
    let solid = if hash == self.null_hash { 0b11 } else { 0b00 };
    Transaction::placeholder(hash.to_owned(), id_tx, solid)
  }
}
