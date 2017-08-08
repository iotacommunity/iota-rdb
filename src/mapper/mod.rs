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
use record::{Record, RecordGuard};
use std::collections::hash_map::{Entry, HashMap};
use std::sync::{Arc, MutexGuard};

pub trait Mapper: Sized {
  type Data;
  type Record: Record;

  fn new(counter: Arc<Counter>) -> Result<Self>;

  fn lock(&self) -> MutexGuard<Self::Data>;

  fn records<'a>(
    guard: &'a mut MutexGuard<Self::Data>,
  ) -> &'a mut HashMap<u64, Self::Record>;

  fn fetch<'a>(
    &self,
    guard: &'a mut MutexGuard<Self::Data>,
    conn: &mut mysql::Conn,
    id: u64,
  ) -> Result<RecordGuard<'a, Self::Record>> {
    let record = match Self::records(guard).entry(id) {
      Entry::Occupied(entry) => {
        let mut record = entry.into_mut();
        if record.is_locked() {
          return Err(Error::Locked);
        } else {
          record.lock();
          record
        }
      }
      Entry::Vacant(entry) => {
        let mut record = Self::Record::find(conn, id)?;
        record.lock();
        entry.insert(record)
      }
    };
    Ok(RecordGuard::new(record))
  }

  fn update(
    &self,
    guard: &mut MutexGuard<Self::Data>,
    conn: &mut mysql::Conn,
  ) -> Result<()> {
    // TODO check locked
    for record in Self::records(guard).values_mut() {
      if record.is_modified() {
        record.update(conn)?;
      }
    }
    Ok(())
  }
}
