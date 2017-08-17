use super::{Mapper, Record, Result, TransactionRecord};
use mysql;
use std::collections::{BTreeMap, HashMap};
use std::hash::Hash;
use std::sync::{Arc, Mutex, RwLock, RwLockWriteGuard};

type Records = RwLock<BTreeMap<u64, Arc<Mutex<TransactionRecord>>>>;
type Hashes = RwLock<HashMap<String, u64>>;
type Index<K, V> = HashMap<K, Arc<Mutex<Vec<V>>>>;
type IndexGuard<'a, K, V> = RwLockWriteGuard<'a, Index<K, V>>;

pub struct TransactionMapper {
  counter: Mutex<u64>,
  records: Records,
  hashes: Hashes,
  trunks: RwLock<Index<u64, u64>>,
  branches: RwLock<Index<u64, u64>>,
}

impl<'a> Mapper<'a> for TransactionMapper {
  type Record = TransactionRecord;
  type Indices = (IndexGuard<'a, u64, u64>, IndexGuard<'a, u64, u64>);

  fn new(conn: &mut mysql::Conn) -> Result<Self> {
    let counter = Self::init_counter(
      conn,
      r"SELECT id_tx FROM tx ORDER BY id_tx DESC LIMIT 1",
    )?;
    let records = RwLock::new(BTreeMap::new());
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

  fn counter(&self) -> &Mutex<u64> {
    &self.counter
  }

  fn records(&self) -> &Records {
    &self.records
  }

  fn hashes(&self) -> &Hashes {
    &self.hashes
  }

  fn indices(&'a self) -> Self::Indices {
    let trunks = self.trunks.write().unwrap();
    let branches = self.branches.write().unwrap();
    (trunks, branches)
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

  fn remove_indices(
    &mut (ref mut trunks, ref mut branches): &mut Self::Indices,
    record: &TransactionRecord,
  ) {
    if let Some(id_trunk) = record.id_trunk() {
      remove_index(trunks, &id_trunk, &record.id());
    }
    if let Some(id_branch) = record.id_branch() {
      remove_index(branches, &id_branch, &record.id());
    }
  }
}

impl TransactionMapper {
  pub fn fetch_many(
    &self,
    conn: &mut mysql::Conn,
    input: Vec<&str>,
  ) -> Result<Vec<Arc<Mutex<TransactionRecord>>>> {
    let cached = {
      let records = self.records.read().unwrap();
      let hashes = self.hashes.read().unwrap();
      input
        .iter()
        .filter_map(|&hash| {
          hashes
            .get(hash)
            .and_then(|&id| records.get(&id).map(|x| (hash, (id, x.clone()))))
        })
        .collect::<HashMap<_, _>>()
    };
    let missing = input
      .iter()
      .filter(|&hash| !cached.contains_key(hash))
      .cloned()
      .collect::<Vec<_>>();
    let found = TransactionRecord::find_by_hashes(conn, &missing)?;
    let mut records = self.records.write().unwrap();
    let mut hashes = self.hashes.write().unwrap();
    let mut indices = self.indices();
    let mut output = input
      .into_iter()
      .map(|hash| {
        cached.get(hash).cloned().unwrap_or_else(|| {
          hashes
            .get(hash)
            .and_then(|&id_tx| {
              records.get(&id_tx).map(|record| (id_tx, record.clone()))
            })
            .unwrap_or_else(|| {
              let record = found
                .iter()
                .find(|record| record.hash() == hash)
                .cloned()
                .unwrap_or_else(|| {
                  TransactionRecord::placeholder(
                    hash.to_owned(),
                    self.next_id(),
                  )
                });
              Self::store(&mut records, &mut hashes, &mut indices, record)
            })
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
    .or_insert_with(|| Arc::new(Mutex::new(Vec::new())));
  let mut vec = vec.lock().unwrap();
  if let Err(i) = vec.binary_search(&value) {
    vec.insert(i, value);
  }
}

fn remove_index<K, V>(index: &mut IndexGuard<K, V>, key: &K, value: &V)
where
  K: Eq + Hash,
  V: Ord,
{
  let mut remove = false;
  if let Some(vec) = index.get(key) {
    let mut vec = vec.lock().unwrap();
    if let Ok(i) = vec.binary_search(value) {
      vec.remove(i);
      if vec.is_empty() {
        remove = true;
      }
    }
  }
  if remove {
    index.remove(key);
  }
}
