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
use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};

pub trait Mapper<'a>: Sized {
  type Record: Record;
  type Indices;

  fn new(conn: &mut mysql::Conn) -> Result<Self>;

  fn counter(&self) -> &Mutex<u64>;

  fn records(&self) -> &RwLock<HashMap<u64, Arc<Mutex<Self::Record>>>>;

  fn hashes(&self) -> &RwLock<HashMap<String, u64>>;

  fn indices(&'a self) -> Self::Indices;

  fn store_indices(indices: &mut Self::Indices, record: &Self::Record);

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
    let mut counter = self.counter().lock().unwrap();
    *counter += 1;
    *counter
  }

  fn current_id(&self) -> u64 {
    *self.counter().lock().unwrap()
  }

  fn fetch(
    &'a self,
    conn: &mut mysql::Conn,
    id: u64,
  ) -> Result<Arc<Mutex<Self::Record>>> {
    let cached = {
      let records = self.records().read().unwrap();
      records.get(&id).cloned()
    };
    cached.map(Ok).unwrap_or_else(|| {
      Self::Record::find_by_id(conn, id).map(|record| {
        let mut records = self.records().write().unwrap();
        let mut hashes = self.hashes().write().unwrap();
        let mut indices = self.indices();
        Self::store(&mut records, &mut hashes, &mut indices, record)
          .1
          .clone()
      })
    })
  }

  fn fetch_or_insert<T>(
    &'a self,
    conn: &mut mysql::Conn,
    hash: &str,
    f: T,
  ) -> Result<u64>
  where
    T: FnOnce(u64) -> Result<Self::Record>,
  {
    let cached = {
      let hashes = self.hashes().read().unwrap();
      hashes.get(hash).cloned()
    };
    cached.map(Ok).unwrap_or_else(|| {
      Self::Record::find_by_hash(conn, hash).and_then(|record| {
        let mut records = self.records().write().unwrap();
        let mut hashes = self.hashes().write().unwrap();
        let mut indices = self.indices();
        record
          .map(Ok)
          .unwrap_or_else(|| {
            f(self.next_id())
              .and_then(|mut record| record.insert(conn).map(|_| record))
          })
          .map(|record| {
            Self::store(&mut records, &mut hashes, &mut indices, record).0
          })
      })
    })
  }

  fn update(&self, conn: &mut mysql::Conn) -> Result<()> {
    let records = {
      let records = self.records().read().unwrap();
      records.values().cloned().collect::<Vec<_>>()
    };
    for record in records {
      let mut record = record.lock().unwrap();
      if record.is_persisted() && record.is_modified() {
        record.update(conn)?;
      }
    }
    Ok(())
  }

  fn store<'b>(
    records: &'b mut HashMap<u64, Arc<Mutex<Self::Record>>>,
    hashes: &mut HashMap<String, u64>,
    indices: &mut Self::Indices,
    record: Self::Record,
  ) -> (u64, &'b Arc<Mutex<Self::Record>>) {
    let id = record.id();
    let record = records.entry(id).or_insert_with(|| {
      hashes.insert(record.hash().to_owned(), id);
      Self::store_indices(indices, &record);
      Arc::new(Mutex::new(record))
    });
    (id, record)
  }
}
