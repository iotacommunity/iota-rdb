use super::{Mapper, Result};
use counter::Counter;
use mysql;
use record::{Record, TransactionRecord};
use std::collections::hash_map::HashMap;
use std::sync::{Arc, Mutex, RwLock};

pub struct TransactionMapper {
  counter: Arc<Counter>,
  records: RwLock<HashMap<u64, Arc<Mutex<TransactionRecord>>>>,
  hashes: RwLock<HashMap<String, u64>>,
}

impl Mapper for TransactionMapper {
  type Record = TransactionRecord;

  fn new(counter: Arc<Counter>) -> Result<Self> {
    let records = RwLock::new(HashMap::new());
    let hashes = RwLock::new(HashMap::new());
    Ok(Self {
      counter,
      records,
      hashes,
    })
  }

  fn records(&self) -> &RwLock<HashMap<u64, Arc<Mutex<TransactionRecord>>>> {
    &self.records
  }

  fn hashes(&self) -> &RwLock<HashMap<String, u64>> {
    &self.hashes
  }

  fn next_counter(&self) -> u64 {
    self.counter.next_transaction()
  }
}

impl TransactionMapper {
  pub fn fetch_many(
    &self,
    conn: &mut mysql::Conn,
    mut input: Vec<&str>,
  ) -> Result<Vec<Arc<Mutex<TransactionRecord>>>> {
    input.sort_unstable();
    input.dedup();
    let mut missing = input.clone();
    {
      let hashes = self.hashes.read().unwrap();
      missing.retain(|hash| hashes.contains_key(*hash));
    }
    let found = TransactionRecord::find_by_hashes(conn, &missing)?;
    let mut records = self.records.write().unwrap();
    let mut hashes = self.hashes.write().unwrap();
    Ok(
      input
        .iter()
        .map(|&hash| {
          hashes
            .get(hash)
            .and_then(|id_tx| records.get(id_tx))
            .cloned()
            .unwrap_or_else(|| {
              let record = found
                .iter()
                .find(|record| record.hash() == hash)
                .cloned()
                .unwrap_or_else(|| {
                  TransactionRecord::placeholder(
                    hash.to_owned(),
                    self.next_counter(),
                  )
                });
              Self::store_and_clone(&mut records, &mut hashes, record)
            })
        })
        .collect(),
    )
  }
}
