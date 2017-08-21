use super::{Garbage, Hashes, Index, Mapper, Record, Records, Result,
            TransactionRecord};
use mysql;
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex, MutexGuard, RwLock, RwLockWriteGuard};

pub struct TransactionMapper {
  counter: Mutex<u64>,
  records: RwLock<Records<TransactionRecord>>,
  hashes: RwLock<Hashes>,
  indices: [RwLock<Records<Index>>; 2],
}

impl Mapper for TransactionMapper {
  type Record = TransactionRecord;

  fn new(conn: &mut mysql::Conn) -> Result<Self> {
    let counter = Self::init_counter(
      conn,
      r"SELECT id_tx FROM tx ORDER BY id_tx DESC LIMIT 1",
    )?;
    let records = RwLock::new(BTreeMap::new());
    let hashes = RwLock::new(HashMap::new());
    let indices = [RwLock::new(BTreeMap::new()), RwLock::new(BTreeMap::new())];
    Ok(Self {
      counter,
      records,
      hashes,
      indices,
    })
  }

  fn counter(&self) -> &Mutex<u64> {
    &self.counter
  }

  fn records(&self) -> &RwLock<Records<TransactionRecord>> {
    &self.records
  }

  fn hashes(&self) -> &RwLock<Hashes> {
    &self.hashes
  }

  fn indices(&self) -> &[RwLock<Records<Index>>] {
    &self.indices
  }

  fn fill_indices(
    indices: &mut [RwLockWriteGuard<Records<Index>>],
    record: &TransactionRecord,
  ) {
    let inner = if record.is_persisted() {
      None
    } else {
      Some(Vec::new())
    };
    indices[0].insert(record.id(), Arc::new(Mutex::new(inner.clone())));
    indices[1].insert(record.id(), Arc::new(Mutex::new(inner)));
    if let Some(id_trunk) = record.id_trunk() {
      if let Some(index) = indices[0].get(&id_trunk) {
        insert_ref(index, record.id());
      }
    }
    if let Some(id_branch) = record.id_branch() {
      if let Some(index) = indices[1].get(&id_branch) {
        insert_ref(index, record.id());
      }
    }
  }

  fn mark_garbage(garbage: &Garbage<TransactionRecord>) {
    for id in garbage.keys().cloned().collect::<Vec<_>>() {
      if let Some(&Some((_, _, ref mark))) = garbage.get(&id) {
        if mark.get().is_none() {
          can_prune(garbage, id);
        }
      }
    }
  }
}

impl TransactionMapper {
  pub fn fetch_many(
    &self,
    conn: &mut mysql::Conn,
    mut input: Vec<&str>,
  ) -> Result<Vec<Arc<Mutex<TransactionRecord>>>> {
    input.dedup();
    let cached = {
      debug!("Mutex check at line {}", line!());
      let records = self.records.read().unwrap();
      debug!("Mutex check at line {}", line!());
      let hashes = self.hashes.read().unwrap();
      debug!("Mutex check at line {}", line!());
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
    let found = TransactionRecord::find_by_hashes(conn, missing)?;
    debug!("Mutex check at line {}", line!());
    let mut records = self.records.write().unwrap();
    debug!("Mutex check at line {}", line!());
    let mut hashes = self.hashes.write().unwrap();
    debug!("Mutex check at line {}", line!());
    let mut indices = self.lock_indices();
    debug!("Mutex check at line {}", line!());
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
              let id_tx = record.id();
              hashes.insert(hash.to_owned(), id_tx);
              Self::fill_indices(&mut indices, &record);
              let wrapper = Arc::new(Mutex::new(record));
              records.insert(id_tx, wrapper.clone());
              (id_tx, wrapper)
            })
        })
      })
      .collect::<Vec<_>>();
    output.sort_unstable_by_key(|&(id_tx, _)| id_tx);
    Ok(output.into_iter().map(|(_, record)| record).collect())
  }

  pub fn set_trunk(&self, record: &mut TransactionRecord, id_trunk: u64) {
    match record.id_trunk() {
      Some(_) => panic!("`id_trunk` is immutable"),
      None => {
        debug!("Mutex check at line {}", line!());
        let trunks = self.indices[0].read().unwrap();
        debug!("Mutex check at line {}", line!());
        if let Some(index) = trunks.get(&id_trunk) {
          insert_ref(index, record.id());
        }
        record.set_id_trunk(Some(id_trunk));
      }
    }
  }

  pub fn set_branch(&self, record: &mut TransactionRecord, id_branch: u64) {
    match record.id_branch() {
      Some(_) => panic!("`id_branch` is immutable"),
      None => {
        debug!("Mutex check at line {}", line!());
        let branches = self.indices[1].read().unwrap();
        debug!("Mutex check at line {}", line!());
        if let Some(index) = branches.get(&id_branch) {
          insert_ref(index, record.id());
        }
        record.set_id_branch(Some(id_branch));
      }
    }
  }

  pub fn trunk_index(&self, id: u64) -> Option<Arc<Mutex<Option<Vec<u64>>>>> {
    debug!("Mutex check at line {}", line!());
    let trunks = self.indices[0].read().unwrap();
    debug!("Mutex check at line {}", line!());
    trunks.get(&id).cloned()
  }

  pub fn branch_index(&self, id: u64) -> Option<Arc<Mutex<Option<Vec<u64>>>>> {
    debug!("Mutex check at line {}", line!());
    let branches = self.indices[1].read().unwrap();
    debug!("Mutex check at line {}", line!());
    branches.get(&id).cloned()
  }

  pub fn fetch_trunk<'a>(
    &self,
    conn: &mut mysql::Conn,
    id: u64,
    index: &'a Mutex<Option<Vec<u64>>>,
  ) -> Result<MutexGuard<'a, Option<Vec<u64>>>> {
    self.fetch_children(conn, id, index, TransactionRecord::find_trunk)
  }

  pub fn fetch_branch<'a>(
    &self,
    conn: &mut mysql::Conn,
    id: u64,
    index: &'a Mutex<Option<Vec<u64>>>,
  ) -> Result<MutexGuard<'a, Option<Vec<u64>>>> {
    self.fetch_children(conn, id, index, TransactionRecord::find_branch)
  }

  fn fetch_children<'a, F>(
    &self,
    conn: &mut mysql::Conn,
    id: u64,
    index: &'a Mutex<Option<Vec<u64>>>,
    f: F,
  ) -> Result<MutexGuard<'a, Option<Vec<u64>>>>
  where
    F: FnOnce(&mut mysql::Conn, u64)
      -> Result<Vec<TransactionRecord>>,
  {
    {
      debug!("Mutex check at line {}", line!());
      let guard = index.lock().unwrap();
      debug!("Mutex check at line {}", line!());
      if guard.is_some() {
        return Ok(guard);
      }
    }
    f(conn, id).map(|found| {
      debug!("Mutex check at line {}", line!());
      let mut records = self.records.write().unwrap();
      debug!("Mutex check at line {}", line!());
      let mut hashes = self.hashes.write().unwrap();
      debug!("Mutex check at line {}", line!());
      let mut index = index.lock().unwrap();
      debug!("Mutex check at line {}", line!());
      match *index {
        Some(_) => index,
        None => {
          let mut ids = found
            .into_iter()
            .map(|record| {
              let id_tx = record.id();
              records.entry(id_tx).or_insert_with(|| {
                hashes.insert(record.hash().to_owned(), id_tx);
                Arc::new(Mutex::new(record))
              });
              id_tx
            })
            .collect::<Vec<_>>();
          ids.sort_unstable();
          ids.dedup();
          *index = Some(ids);
          index
        }
      }
    })
  }
}

fn insert_ref(index: &Mutex<Option<Vec<u64>>>, id: u64) {
  debug!("Mutex check at line {}", line!());
  let mut index = index.lock().unwrap();
  debug!("Mutex check at line {}", line!());
  if let Some(ref mut vec) = *index {
    if let Err(i) = vec.binary_search(&id) {
      vec.insert(i, id);
    }
  }
}

fn can_prune(garbage: &Garbage<TransactionRecord>, id: u64) -> bool {
  garbage
    .get(&id)
    .map(|data| {
      data
        .as_ref()
        .map(|&(ref record, _, ref mark)| {
          mark.get().unwrap_or_else(|| {
            let prune = record
              .id_trunk()
              .map(|id_trunk| can_prune(garbage, id_trunk))
              .unwrap_or(true) &&
              record
                .id_branch()
                .map(|id_branch| can_prune(garbage, id_branch))
                .unwrap_or(true);
            mark.set(Some(prune));
            prune
          })
        })
        .unwrap_or(false)
    })
    .unwrap_or(true)
}
