use super::{AddressRecord, Hashes, Index, Mapper, Records, Result};
use mysql;
use std::collections::{BTreeMap, HashMap};
use std::sync::{Mutex, RwLock, RwLockWriteGuard};

pub struct AddressMapper {
  counter: Mutex<u64>,
  records: RwLock<Records<AddressRecord>>,
  hashes: RwLock<Hashes>,
  indices: [RwLock<Records<Index>>; 0],
}

impl Mapper for AddressMapper {
  type Record = AddressRecord;

  fn new(conn: &mut mysql::Conn, retry_interval: u64) -> Result<Self> {
    let counter = Self::init_counter(
      conn,
      retry_interval,
      r"SELECT id_address FROM address ORDER BY id_address DESC LIMIT 1",
    )?;
    let records = RwLock::new(BTreeMap::new());
    let hashes = RwLock::new(HashMap::new());
    let indices = [];
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

  fn records(&self) -> &RwLock<Records<AddressRecord>> {
    &self.records
  }

  fn hashes(&self) -> &RwLock<Hashes> {
    &self.hashes
  }

  fn indices(&self) -> &[RwLock<Records<Index>>] {
    &self.indices
  }

  fn fill_indices(
    _indices: &mut [RwLockWriteGuard<Records<Index>>],
    _record: &AddressRecord,
    _skip_index: Option<(usize, u64)>,
  ) {
  }
}
