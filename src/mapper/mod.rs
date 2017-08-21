mod transaction_mapper;
mod address_mapper;
mod bundle_mapper;
mod record;
mod error;

pub use self::address_mapper::AddressMapper;
pub use self::bundle_mapper::BundleMapper;
pub use self::error::{Error, Result};
pub use self::record::{AddressRecord, BundleRecord, Record, TransactionRecord};
pub use self::transaction_mapper::TransactionMapper;

use mysql;
use std::cell::Cell;
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex, MutexGuard, RwLock, RwLockWriteGuard};

pub type Records<T> = BTreeMap<u64, Arc<Mutex<T>>>;
pub type Hashes = HashMap<String, u64>;
pub type Index = Option<Vec<u64>>;
pub type Garbage<'a, T> = HashMap<
  u64,
  Option<
    (
      MutexGuard<'a, T>,
      Vec<MutexGuard<'a, Index>>,
      Cell<Option<bool>>,
    ),
  >,
>;

pub trait Mapper: Sized {
  type Record: Record;

  fn new(conn: &mut mysql::Conn) -> Result<Self>;

  fn counter(&self) -> &Mutex<u64>;

  fn records(&self) -> &RwLock<Records<Self::Record>>;

  fn hashes(&self) -> &RwLock<Hashes>;

  fn indices(&self) -> &[RwLock<Records<Index>>];

  fn fill_indices(
    indices: &mut [RwLockWriteGuard<Records<Index>>],
    record: &Self::Record,
  );

  fn mark_garbage(garbage: &Garbage<Self::Record>);

  fn lock_indices(&self) -> Vec<RwLockWriteGuard<Records<Index>>> {
    self
      .indices()
      .iter()
      .map(|index| index.write().unwrap())
      .collect()
  }

  fn init_counter(
    conn: &mut mysql::Conn,
    query: &str,
  ) -> mysql::Result<Mutex<u64>> {
    conn
      .first(query)
      .and_then(|row| row.map_or_else(|| Ok(0), mysql::from_row_opt))
      .map(Mutex::new)
  }

  fn next_id(&self) -> u64 {
    debug!("Mutex check at line {}", line!());
    let mut counter = self.counter().lock().unwrap();
    debug!("Mutex check at line {}", line!());
    *counter += 1;
    *counter
  }

  fn current_id(&self) -> u64 {
    debug!("Mutex check at line {}", line!());
    let counter = *self.counter().lock().unwrap();
    debug!("Mutex check at line {}", line!());
    counter
  }

  fn fetch(
    &self,
    conn: &mut mysql::Conn,
    id: u64,
  ) -> Result<Arc<Mutex<Self::Record>>> {
    let cached = {
      debug!("Mutex check at line {}", line!());
      let records = self.records().read().unwrap();
      debug!("Mutex check at line {}", line!());
      records.get(&id).cloned()
    };
    cached.map(Ok).unwrap_or_else(|| {
      Self::Record::find_by_id(conn, id).map(|record| {
        debug!("Mutex check at line {}", line!());
        let mut records = self.records().write().unwrap();
        debug!("Mutex check at line {}", line!());
        records
          .entry(record.id())
          .or_insert_with(|| {
            debug!("Mutex check at line {}", line!());
            let mut hashes = self.hashes().write().unwrap();
            debug!("Mutex check at line {}", line!());
            let mut indices = self.lock_indices();
            debug!("Mutex check at line {}", line!());
            hashes.insert(record.hash().to_owned(), record.id());
            Self::fill_indices(&mut indices, &record);
            Arc::new(Mutex::new(record))
          })
          .clone()
      })
    })
  }

  fn fetch_by_hash<T>(
    &self,
    conn: &mut mysql::Conn,
    hash: &str,
    f: T,
  ) -> Result<Arc<Mutex<Self::Record>>>
  where
    T: FnOnce(u64) -> Result<Self::Record>,
  {
    let cached = {
      debug!("Mutex check at line {}", line!());
      let records = self.records().read().unwrap();
      debug!("Mutex check at line {}", line!());
      let hashes = self.hashes().read().unwrap();
      debug!("Mutex check at line {}", line!());
      hashes.get(hash).and_then(|id| records.get(id)).cloned()
    };
    cached.map(Ok).unwrap_or_else(|| {
      Self::Record::find_by_hash(conn, hash).and_then(|record| {
        debug!("Mutex check at line {}", line!());
        let mut records = self.records().write().unwrap();
        debug!("Mutex check at line {}", line!());
        let mut hashes = self.hashes().write().unwrap();
        debug!("Mutex check at line {}", line!());
        let mut indices = self.lock_indices();
        debug!("Mutex check at line {}", line!());
        record.map_or_else(|| f(self.next_id()), Ok).map(|record| {
          records
            .entry(record.id())
            .or_insert_with(|| {
              hashes.insert(record.hash().to_owned(), record.id());
              Self::fill_indices(&mut indices, &record);
              Arc::new(Mutex::new(record))
            })
            .clone()
        })
      })
    })
  }

  fn update(&self, conn: &mut mysql::Conn) -> Result<usize> {
    let mut counter = 0;
    let records = {
      debug!("Mutex check at line {}", line!());
      let records = self.records().read().unwrap();
      debug!("Mutex check at line {}", line!());
      records.values().cloned().collect::<Vec<_>>()
    };
    for record in records {
      debug!("Mutex check at line {}", line!());
      let mut record = record.lock().unwrap();
      debug!("Mutex check at line {}", line!());
      if !record.is_persisted() {
        continue;
      }
      if record.is_modified() {
        record.update(conn)?;
        counter += 1;
      }
      record.advance_generation();
    }
    Ok(counter)
  }

  fn prune(&self, generation_limit: usize) -> usize {
    debug!("Mutex check at line {}", line!());
    let mut records = self.records().write().unwrap();
    debug!("Mutex check at line {}", line!());
    let mut hashes = self.hashes().write().unwrap();
    debug!("Mutex check at line {}", line!());
    let mut indices = self.lock_indices();
    debug!("Mutex check at line {}", line!());
    let record_refs = records.values().cloned().collect::<Vec<_>>();
    let index_refs = indices
      .iter()
      .map(|i| i.iter().map(|(&k, v)| (k, v.clone())).collect())
      .collect::<Vec<BTreeMap<_, _>>>();
    let garbage: Garbage<Self::Record> = record_refs
      .iter()
      .map(|reference| {
        debug!("Mutex check at line {}", line!());
        let record = reference.lock().unwrap();
        debug!("Mutex check at line {}", line!());
        let id = record.id();
        let index_refs = index_refs
          .iter()
          .filter_map(|index| index.get(&id))
          .collect::<Vec<_>>();
        debug!("Mutex check at line {}", line!());
        let indices = index_refs
          .iter()
          .map(|index| index.lock().unwrap())
          .collect::<Vec<_>>();
        debug!("Mutex check at line {}", line!());
        if Arc::strong_count(reference) == 2 &&
          index_refs.iter().all(|index| Arc::strong_count(index) == 2) &&
          record.is_persisted() && !record.is_modified() &&
          record.generation() > generation_limit
        {
          (id, Some((record, indices, Cell::new(None))))
        } else {
          (id, None)
        }
      })
      .collect();
    Self::mark_garbage(&garbage);
    for value in garbage.values() {
      if let Some((ref record, _, ref mark)) = *value {
        if let Some(true) = mark.get() {
          records.remove(&record.id());
          hashes.remove(record.hash());
          for index in &mut indices {
            index.remove(&record.id());
          }
        }
      }
    }
    record_refs.len() - records.len()
  }
}
