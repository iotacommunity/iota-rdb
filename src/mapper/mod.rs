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
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex, RwLock, RwLockWriteGuard};
use std::thread;
use std::time::Duration;

pub type Records<T> = BTreeMap<u64, Arc<Mutex<T>>>;
pub type Hashes = HashMap<String, u64>;
pub type Index = Option<Vec<u64>>;

pub trait Mapper: Sized {
  type Record: Record;

  fn new(conn: &mut mysql::Conn, retry_interval: u64) -> Result<Self>;

  fn counter(&self) -> &Mutex<u64>;

  fn records(&self) -> &RwLock<Records<Self::Record>>;

  fn hashes(&self) -> &RwLock<Hashes>;

  fn indices(&self) -> &[RwLock<Records<Index>>];

  fn fill_indices(
    indices: &mut [RwLockWriteGuard<Records<Index>>],
    record: &Self::Record,
    skip_index: Option<(usize, u64)>,
  );

  fn lock_indices(&self) -> Vec<RwLockWriteGuard<Records<Index>>> {
    self
      .indices()
      .iter()
      .map(|index| index.write().unwrap())
      .collect()
  }

  fn init_counter(
    conn: &mut mysql::Conn,
    retry_interval: u64,
    query: &str,
  ) -> mysql::Result<Mutex<u64>> {
    let retry_interval = Duration::from_millis(retry_interval);
    let row;
    loop {
      match conn.first(query) {
        Ok(result) => {
          row = result;
          break;
        }
        Err(mysql::Error::MySqlError(ref err)) if err.code == 1146 => {
          warn!("Counter initialization failure: {}. Retrying...", err);
          thread::sleep(retry_interval);
        }
        Err(err) => {
          return Err(err);
        }
      }
    }
    row
      .map_or_else(|| Ok(0), mysql::from_row_opt)
      .map(Mutex::new)
  }

  fn next_id(&self) -> u64 {
    debug!("Mutex lock");
    let mut counter = self.counter().lock().unwrap();
    debug!("Mutex acquire");
    *counter += 1;
    *counter
  }

  fn current_id(&self) -> u64 {
    debug!("Mutex lock");
    let counter = *self.counter().lock().unwrap();
    debug!("Mutex acquire");
    counter
  }

  fn fetch(
    &self,
    conn: &mut mysql::Conn,
    id: u64,
    skip_index: Option<(usize, u64)>,
  ) -> Result<Arc<Mutex<Self::Record>>> {
    let cached = {
      debug!("Mutex lock");
      let records = self.records().read().unwrap();
      debug!("Mutex acquire");
      records.get(&id).cloned()
    };
    cached.map(Ok).unwrap_or_else(|| {
      Self::Record::find_by_id(conn, id).map(|record| {
        debug!("Mutex lock");
        let mut records = self.records().write().unwrap();
        debug!("Mutex acquire");
        records
          .entry(record.id())
          .or_insert_with(|| {
            debug!("Mutex lock");
            let mut hashes = self.hashes().write().unwrap();
            debug!("Mutex lock/acquire");
            let mut indices = self.lock_indices();
            debug!("Mutex acquire");
            hashes.insert(record.hash().to_owned(), record.id());
            Self::fill_indices(&mut indices, &record, skip_index);
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
  ) -> Result<(u64, Arc<Mutex<Self::Record>>)>
  where
    T: FnOnce(u64) -> Result<Self::Record>,
  {
    let cached = {
      debug!("Mutex lock");
      let records = self.records().read().unwrap();
      debug!("Mutex lock/acquire");
      let hashes = self.hashes().read().unwrap();
      debug!("Mutex acquire");
      hashes
        .get(hash)
        .and_then(|&id| records.get(&id).map(|record| (id, record.clone())))
    };
    cached.map(Ok).unwrap_or_else(|| {
      Self::Record::find_by_hash(conn, hash).and_then(|record| {
        debug!("Mutex lock");
        let mut records = self.records().write().unwrap();
        debug!("Mutex lock/acquire");
        let mut hashes = self.hashes().write().unwrap();
        debug!("Mutex lock/acquire");
        let mut indices = self.lock_indices();
        debug!("Mutex acquire");
        record.map_or_else(|| f(self.next_id()), Ok).map(|record| {
          let id = record.id();
          let record = records.entry(id).or_insert_with(|| {
            hashes.insert(record.hash().to_owned(), id);
            Self::fill_indices(&mut indices, &record, None);
            Arc::new(Mutex::new(record))
          });
          (id, record.clone())
        })
      })
    })
  }

  fn update(&self, conn: &mut mysql::Conn) -> Result<usize> {
    let mut counter = 0;
    let records = {
      debug!("Mutex lock");
      let records = self.records().read().unwrap();
      debug!("Mutex acquire");
      records.values().cloned().collect::<Vec<_>>()
    };
    for record in records {
      debug!("Mutex lock");
      let mut record = record.lock().unwrap();
      debug!("Mutex acquire");
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
    debug!("Mutex lock");
    let mut records = self.records().write().unwrap();
    debug!("Mutex lock/acquire");
    let mut hashes = self.hashes().write().unwrap();
    debug!("Mutex lock/acquire");
    let mut indices = self.lock_indices();
    debug!("Mutex acquire");
    let record_refs = records.values().cloned().collect::<Vec<_>>();
    let index_refs = indices
      .iter()
      .map(|i| i.iter().map(|(&k, v)| (k, v.clone())).collect())
      .collect::<Vec<HashMap<_, _>>>();
    record_refs
      .iter()
      .filter_map(|reference| {
        reference.try_lock().ok().map(|record| (record, reference))
      })
      .filter_map(|(record, reference)| {
        let index_refs = index_refs
          .iter()
          .filter_map(|index| index.get(&record.id()))
          .collect::<Vec<_>>();
        let indices = index_refs
          .iter()
          .filter_map(|index| index.try_lock().ok())
          .collect::<Vec<_>>();
        if indices.len() == index_refs.len() &&
          Arc::strong_count(reference) == 2 &&
          index_refs.iter().all(|index| Arc::strong_count(index) == 2) &&
          record.is_persisted() && !record.is_modified() &&
          record.generation() > generation_limit
        {
          Some((record, indices))
        } else {
          None
        }
      })
      .for_each(|(record, _)| {
        records.remove(&record.id());
        hashes.remove(record.hash());
        for index in &mut indices {
          index.remove(&record.id());
        }
      });
    record_refs.len() - records.len()
  }
}
