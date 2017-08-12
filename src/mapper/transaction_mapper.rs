use super::{Mapper, Record, Result, TransactionRecord};
use counter::Counter;
use mysql;
use std::collections::HashMap;
use std::hash::Hash;
use std::sync::{Arc, Mutex, RwLock, RwLockWriteGuard};

type Records = RwLock<HashMap<u64, Arc<Mutex<TransactionRecord>>>>;
type Hashes = RwLock<HashMap<String, u64>>;
type Index<K, V> = HashMap<K, Arc<Mutex<Vec<V>>>>;
type IndexGuard<'a, K, V> = RwLockWriteGuard<'a, Index<K, V>>;

pub struct TransactionMapper {
  counter: Arc<Counter>,
  records: Records,
  hashes: Hashes,
  trunks: RwLock<Index<u64, u64>>,
  branches: RwLock<Index<u64, u64>>,
}

impl<'a> Mapper<'a> for TransactionMapper {
  type Record = TransactionRecord;
  type Indices = (IndexGuard<'a, u64, u64>, IndexGuard<'a, u64, u64>);

  fn new(counter: Arc<Counter>) -> Result<Self> {
    let records = RwLock::new(HashMap::new());
    let hashes = RwLock::new(HashMap::new());
    let trunks = RwLock::new(HashMap::new());
    let branches = RwLock::new(HashMap::new());
    Ok(Self {
      counter,
      records,
      hashes,
      trunks,
      branches,
    })
  }

  fn records(&self) -> &Records {
    &self.records
  }

  fn hashes(&self) -> &Hashes {
    &self.hashes
  }

  fn indices(&'a self) -> Self::Indices {
    (self.trunks.write().unwrap(), self.branches.write().unwrap())
  }

  fn store_indices(
    &mut (ref mut trunks, ref mut branches): &mut Self::Indices,
    record: &TransactionRecord,
  ) {
    if let Some(id_trunk) = record.id_trunk() {
      store_index(trunks, id_trunk, record.id());
    }
    if let Some(id_branch) = record.id_branch() {
      store_index(branches, id_branch, record.id());
    }
  }

  fn next_counter(&self) -> u64 {
    self.counter.next_transaction()
  }
}

impl TransactionMapper {
  pub fn fetch_many(
    &self,
    conn: &mut mysql::Conn,
    input: Vec<&str>,
  ) -> Result<Vec<Arc<Mutex<TransactionRecord>>>> {
    let mut missing = input.clone();
    {
      let hashes = self.hashes.read().unwrap();
      missing.retain(|hash| hashes.contains_key(*hash));
    }
    let found = TransactionRecord::find_by_hashes(conn, &missing)?;
    let mut records = self.records.write().unwrap();
    let mut hashes = self.hashes.write().unwrap();
    let mut indices = self.indices();
    let mut output = input
      .into_iter()
      .map(|hash| {
        hashes
          .get(hash)
          .and_then(|id_tx| {
            records.get(id_tx).cloned().map(|record| (*id_tx, record))
          })
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
            let (id_tx, record) =
              Self::store(&mut records, &mut hashes, &mut indices, record);
            (id_tx, record.clone())
          })
      })
      .collect::<Vec<_>>();
    output.sort_unstable_by_key(|&(id_tx, _)| id_tx);
    Ok(output.into_iter().map(|(_, record)| record).collect())
  }

  pub fn trunk_references(&self, id: u64) -> Option<Arc<Mutex<Vec<u64>>>> {
    let trunks = self.trunks.read().unwrap();
    trunks.get(&id).cloned()
  }

  pub fn branch_references(&self, id: u64) -> Option<Arc<Mutex<Vec<u64>>>> {
    let branches = self.branches.read().unwrap();
    branches.get(&id).cloned()
  }

  pub fn set_trunk(&self, record: &mut TransactionRecord, id_trunk: u64) {
    match record.id_trunk() {
      Some(_) => panic!("`id_trunk` is immutable"),
      None => {
        let mut trunks = self.trunks.write().unwrap();
        store_index(&mut trunks, id_trunk, record.id());
        record.set_id_trunk(Some(id_trunk));
      }
    }
  }

  pub fn set_branch(&self, record: &mut TransactionRecord, id_branch: u64) {
    match record.id_branch() {
      Some(_) => panic!("`id_branch` is immutable"),
      None => {
        let mut branches = self.branches.write().unwrap();
        store_index(&mut branches, id_branch, record.id());
        record.set_id_branch(Some(id_branch));
      }
    }
  }
}

fn store_index<K, V>(index: &mut IndexGuard<K, V>, key: K, value: V)
where
  K: Eq + Hash,
  V: Ord,
{
  let vec = index
    .entry(key)
    .or_insert_with(|| Arc::new(Mutex::new(Vec::new())))
    .clone();
  let mut vec = vec.lock().unwrap();
  if let Err(i) = vec.binary_search(&value) {
    vec.insert(i, value)
  }
}
