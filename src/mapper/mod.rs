mod transaction_mapper;
mod address_mapper;
mod bundle_mapper;
mod error;

pub use self::address_mapper::AddressMapper;
pub use self::bundle_mapper::BundleMapper;
pub use self::error::{Error, Result};
pub use self::transaction_mapper::TransactionMapper;

use counter::Counter;
use mysql;
use record::{self, Record};
use std::collections::hash_map::HashMap;
use std::sync::{Arc, Mutex, RwLock};

pub trait Mapper: Sized {
  type Record: Record;

  fn new(counter: Arc<Counter>) -> Result<Self>;

  fn records(&self) -> &RwLock<HashMap<u64, Arc<Mutex<Self::Record>>>>;

  fn hashes(&self) -> &RwLock<HashMap<String, u64>>;

  fn next_counter(&self) -> u64;

  fn fetch(
    &self,
    conn: &mut mysql::Conn,
    id: u64,
  ) -> Result<Arc<Mutex<Self::Record>>> {
    let cached = {
      let records = self.records().read().unwrap();
      records.get(&id).cloned()
    };
    match cached {
      Some(record) => Ok(record),
      None => {
        let record = Self::Record::find_by_id(conn, id)?;
        let mut records = self.records().write().unwrap();
        let mut hashes = self.hashes().write().unwrap();
        Ok(Self::store_and_clone(&mut records, &mut hashes, record))
      }
    }
  }

  fn fetch_or_insert<T>(
    &self,
    conn: &mut mysql::Conn,
    hash: &str,
    f: T,
  ) -> Result<u64>
  where
    T: FnOnce(u64) -> record::Result<Self::Record>,
  {
    let cached = {
      let hashes = self.hashes().read().unwrap();
      hashes.get(hash).cloned()
    };
    match cached {
      Some(id) => Ok(id),
      None => {
        let record = match Self::Record::find_by_hash(conn, hash)? {
          Some(record) => record,
          None => f(self.next_counter())?,
        };
        let mut records = self.records().write().unwrap();
        let mut hashes = self.hashes().write().unwrap();
        Ok(Self::store(&mut records, &mut hashes, record))
      }
    }
  }

  fn store_and_clone(
    records: &mut HashMap<u64, Arc<Mutex<Self::Record>>>,
    hashes: &mut HashMap<String, u64>,
    record: Self::Record,
  ) -> Arc<Mutex<Self::Record>> {
    records
      .entry(record.id())
      .or_insert_with(|| {
        hashes.insert(record.hash().to_owned(), record.id());
        Arc::new(Mutex::new(record))
      })
      .clone()
  }

  fn store(
    records: &mut HashMap<u64, Arc<Mutex<Self::Record>>>,
    hashes: &mut HashMap<String, u64>,
    record: Self::Record,
  ) -> u64 {
    let id = record.id();
    records.entry(id).or_insert_with(|| {
      hashes.insert(record.hash().to_owned(), id);
      Arc::new(Mutex::new(record))
    });
    id
  }

  fn update(&self, conn: &mut mysql::Conn) -> Result<()> {
    let records = {
      let records = self.records().read().unwrap();
      records.values().cloned().collect::<Vec<_>>()
    };
    for record in records {
      let mut record = record.lock().unwrap();
      if record.is_modified() {
        record.update(conn)?;
      }
    }
    Ok(())
  }
}
