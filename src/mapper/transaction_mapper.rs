use super::{Error, Mapper, Result};
use counter::Counter;
use mysql;
use record::{Record, TransactionRecord};
use std::collections::hash_map::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};
use utils;

const HASH_SIZE: usize = 81;

type TransactionData = (HashMap<u64, TransactionRecord>, HashMap<String, u64>);

pub struct TransactionMapper {
  counter: Arc<Counter>,
  data: Mutex<TransactionData>,
  null_hash: String,
}

impl Mapper for TransactionMapper {
  type Data = TransactionData;
  type Record = TransactionRecord;

  fn new(counter: Arc<Counter>) -> Result<Self> {
    let data = Mutex::new((HashMap::new(), HashMap::new()));
    let null_hash = utils::trits_string(0, HASH_SIZE)
      .ok_or(Error::NullHashToTrits)?;
    Ok(Self {
      counter,
      data,
      null_hash,
    })
  }

  fn lock(&self) -> MutexGuard<TransactionData> {
    self.data.lock().unwrap()
  }

  fn records<'a>(
    guard: &'a mut MutexGuard<TransactionData>,
  ) -> &'a mut HashMap<u64, TransactionRecord> {
    let (ref mut records, _) = **guard;
    records
  }
}

impl TransactionMapper {
  // TODO return record guards
  pub fn fetch_triplet(
    &self,
    conn: &mut mysql::Conn,
    current_hash: &str,
    trunk_hash: &str,
    branch_hash: &str,
  ) -> Result<Option<(TransactionRecord, TransactionRecord, TransactionRecord)>> {
    if current_hash == self.null_hash &&
      (current_hash == trunk_hash || current_hash == branch_hash)
    {
      return Ok(None);
    }
    let hashes = self.absent_hashes(current_hash, trunk_hash, branch_hash);
    let results = TransactionRecord::find_by_hashes(conn, &hashes)?;
    self.fetch_triplet_results(&results, current_hash, trunk_hash, branch_hash)
  }

  pub fn insert(
    &self,
    conn: &mut mysql::Conn,
    mut current_tx: TransactionRecord,
    mut trunk_tx: TransactionRecord,
    mut branch_tx: TransactionRecord,
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
    results: &[TransactionRecord],
    current_hash: &str,
    trunk_hash: &str,
    branch_hash: &str,
  ) -> Result<Option<(TransactionRecord, TransactionRecord, TransactionRecord)>> {
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
    records: &HashMap<u64, TransactionRecord>,
    hashes: &HashMap<String, u64>,
    results: &[TransactionRecord],
    hash: &str,
  ) -> TransactionRecord {
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

  fn create_placeholder(&self, hash: &str) -> TransactionRecord {
    let id_tx = self.counter.next_transaction();
    let solid = if hash == self.null_hash { 0b11 } else { 0b00 };
    TransactionRecord::placeholder(hash.to_owned(), id_tx, solid)
  }
}
