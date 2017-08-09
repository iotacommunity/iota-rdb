use super::Mapper;
use counter::Counter;
use mysql;
use record::{Record, Result, TransactionRecord};
use std::collections::HashMap;
use std::hash::Hash;
use std::sync::{Arc, Mutex, RwLock};

type Records = RwLock<HashMap<u64, Arc<Mutex<TransactionRecord>>>>;
type Hashes = RwLock<HashMap<String, u64>>;
type Trunks = RwLock<HashMap<u64, Arc<Mutex<Vec<u64>>>>>;
type Branches = RwLock<HashMap<u64, Arc<Mutex<Vec<u64>>>>>;

pub struct TransactionMapper {
  counter: Arc<Counter>,
  records: Records,
  hashes: Hashes,
  trunks: Trunks,
  branches: Branches,
}

impl<'a> Mapper<'a> for TransactionMapper {
  type Record = TransactionRecord;
  type Indices = (&'a Trunks, &'a Branches);

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
    (&self.trunks, &self.branches)
  }

  fn store_indices(
    (trunks, branches): Self::Indices,
    record: &TransactionRecord,
  ) {
    let trunks = {
      let mut trunks = trunks.write().unwrap();
      store_index(&mut trunks, record.id_trunk())
    };
    store_ordered(&trunks, record.id());
    let branches = {
      let mut branches = branches.write().unwrap();
      store_index(&mut branches, record.id_branch())
    };
    store_ordered(&branches, record.id());
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
              Self::store(&mut records, &mut hashes, self.indices(), record);
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
}

fn store_index<K, V>(
  index: &mut HashMap<K, Arc<Mutex<Vec<V>>>>,
  key: K,
) -> Arc<Mutex<Vec<V>>>
where
  K: Eq + Hash,
{
  index
    .entry(key)
    .or_insert_with(|| Arc::new(Mutex::new(Vec::new())))
    .clone()
}

fn store_ordered<V>(vec: &Arc<Mutex<Vec<V>>>, value: V)
where
  V: Ord,
{
  let mut vec = vec.lock().unwrap();
  if let Err(i) = vec.binary_search(&value) {
    vec.insert(i, value)
  }
}
