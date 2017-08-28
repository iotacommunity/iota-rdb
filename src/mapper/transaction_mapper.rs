use super::{Hashes, Index, Mapper, Record, Records, Result, TransactionRecord};
use mysql;
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex, MutexGuard, RwLock, RwLockWriteGuard};

pub struct TransactionMapper {
  counter: Mutex<u64>,
  records: RwLock<Records<TransactionRecord>>,
  hashes: RwLock<Hashes>,
  indices: [RwLock<Records<Index>>; 2],
}

type FetchManyResult = Result<
  Vec<(u64, String, Arc<Mutex<TransactionRecord>>)>,
>;

type FetchIndexResult<'a> = Result<
  (MutexGuard<'a, Index>, Option<(usize, u64)>),
>;

impl Mapper for TransactionMapper {
  type Record = TransactionRecord;

  fn new(conn: &mut mysql::Conn, retry_interval: u64) -> Result<Self> {
    let counter = Self::init_counter(
      conn,
      retry_interval,
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
    skip_index: Option<(usize, u64)>,
  ) {
    let inner = if record.is_persisted() {
      None
    } else {
      Some(Vec::new())
    };
    indices[0].insert(record.id(), Arc::new(Mutex::new(inner.clone())));
    indices[1].insert(record.id(), Arc::new(Mutex::new(inner)));
    if let Some(id_trunk) = record.id_trunk() {
      match skip_index {
        Some((0, id)) if id == id_trunk => {}
        _ => if let Some(index) = indices[0].get(&id_trunk) {
          debug!("Mutex lock");
          let mut index = index.lock().unwrap();
          debug!("Mutex acquire");
          record.fill_index(&mut index);
        },
      }
    }
    if let Some(id_branch) = record.id_branch() {
      match skip_index {
        Some((1, id)) if id == id_branch => {}
        _ => if let Some(index) = indices[1].get(&id_branch) {
          debug!("Mutex lock");
          let mut index = index.lock().unwrap();
          debug!("Mutex acquire");
          record.fill_index(&mut index);
        },
      }
    }
  }
}

impl TransactionMapper {
  pub fn fetch_many(
    &self,
    conn: &mut mysql::Conn,
    mut input: Vec<&str>,
  ) -> FetchManyResult {
    input.dedup();
    let cached = {
      debug!("Mutex lock");
      let records = self.records.read().unwrap();
      debug!("Mutex lock/acquire");
      let hashes = self.hashes.read().unwrap();
      debug!("Mutex acquire");
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
    debug!("Mutex lock");
    let mut records = self.records.write().unwrap();
    debug!("Mutex lock/acquire");
    let mut hashes = self.hashes.write().unwrap();
    debug!("Mutex lock/acquire");
    let mut indices = self.lock_indices();
    debug!("Mutex acquire");
    let mut output = input
      .into_iter()
      .map(|hash| {
        cached
          .get(hash)
          .map(|&(id_tx, ref record)| {
            (id_tx, hash.to_owned(), record.clone())
          })
          .unwrap_or_else(|| {
            hashes
              .get(hash)
              .and_then(|&id_tx| {
                records
                  .get(&id_tx)
                  .map(|record| (id_tx, hash.to_owned(), record.clone()))
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
                Self::fill_indices(&mut indices, &record, None);
                let wrapper = Arc::new(Mutex::new(record));
                records.insert(id_tx, wrapper.clone());
                (id_tx, hash.to_owned(), wrapper)
              })
          })
      })
      .collect::<Vec<_>>();
    output.sort_unstable_by_key(|&(id_tx, _, _)| id_tx);
    Ok(output)
  }

  pub fn trunk_index(&self, id: u64) -> Option<Arc<Mutex<Index>>> {
    debug!("Mutex lock");
    let trunks = self.indices[0].read().unwrap();
    debug!("Mutex acquire");
    trunks.get(&id).cloned()
  }

  pub fn branch_index(&self, id: u64) -> Option<Arc<Mutex<Index>>> {
    debug!("Mutex lock");
    let branches = self.indices[1].read().unwrap();
    debug!("Mutex acquire");
    branches.get(&id).cloned()
  }

  pub fn fetch_trunk<'a>(
    &self,
    conn: &mut mysql::Conn,
    id: u64,
    index: &'a Mutex<Index>,
  ) -> FetchIndexResult<'a> {
    self.fetch_index(
      conn,
      id,
      index,
      Some((0, id)),
      TransactionRecord::find_trunk,
    )
  }

  pub fn fetch_branch<'a>(
    &self,
    conn: &mut mysql::Conn,
    id: u64,
    index: &'a Mutex<Index>,
  ) -> FetchIndexResult<'a> {
    self.fetch_index(
      conn,
      id,
      index,
      Some((1, id)),
      TransactionRecord::find_branch,
    )
  }

  pub fn fetch_bundle<'a>(
    &self,
    conn: &mut mysql::Conn,
    id: u64,
    index: &'a Mutex<Index>,
  ) -> FetchIndexResult<'a> {
    self.fetch_index(conn, id, index, None, TransactionRecord::find_bundle)
  }

  fn fetch_index<'a, F>(
    &self,
    conn: &mut mysql::Conn,
    id: u64,
    index: &'a Mutex<Index>,
    skip_index: Option<(usize, u64)>,
    f: F,
  ) -> FetchIndexResult<'a>
  where
    F: FnOnce(&mut mysql::Conn, u64)
      -> Result<Vec<TransactionRecord>>,
  {
    {
      debug!("Mutex lock");
      let guard = index.lock().unwrap();
      debug!("Mutex acquire");
      if guard.is_some() {
        return Ok((guard, skip_index));
      }
    }
    f(conn, id).map(|found| {
      debug!("Mutex lock");
      let mut index = index.lock().unwrap();
      debug!("Mutex acquire");
      match *index {
        Some(_) => (index, skip_index),
        None => {
          debug!("Mutex lock");
          let mut records = self.records.write().unwrap();
          debug!("Mutex lock/acquire");
          let mut hashes = self.hashes.write().unwrap();
          debug!("Mutex lock/acquire");
          let mut indices = self.lock_indices();
          debug!("Mutex acquire");
          let mut ids = found
            .into_iter()
            .map(|record| {
              let id_tx = record.id();
              records.entry(id_tx).or_insert_with(|| {
                hashes.insert(record.hash().to_owned(), id_tx);
                Self::fill_indices(&mut indices, &record, skip_index);
                Arc::new(Mutex::new(record))
              });
              id_tx
            })
            .collect::<Vec<_>>();
          ids.sort_unstable();
          ids.dedup();
          *index = Some(ids);
          (index, skip_index)
        }
      }
    })
  }
}
