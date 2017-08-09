use super::{Mapper, Result};
use counter::Counter;
use record::AddressRecord;
use std::collections::hash_map::HashMap;
use std::sync::{Arc, Mutex, RwLock};

pub struct AddressMapper {
  counter: Arc<Counter>,
  records: RwLock<HashMap<u64, Arc<Mutex<AddressRecord>>>>,
  hashes: RwLock<HashMap<String, u64>>,
}

impl Mapper for AddressMapper {
  type Record = AddressRecord;

  fn new(counter: Arc<Counter>) -> Result<Self> {
    let records = RwLock::new(HashMap::new());
    let hashes = RwLock::new(HashMap::new());
    Ok(Self {
      counter,
      records,
      hashes,
    })
  }

  fn records(&self) -> &RwLock<HashMap<u64, Arc<Mutex<AddressRecord>>>> {
    &self.records
  }

  fn hashes(&self) -> &RwLock<HashMap<String, u64>> {
    &self.hashes
  }

  fn next_counter(&self) -> u64 {
    self.counter.next_address()
  }
}
