use super::{Mapper, Result};
use counter::Counter;
use record::BundleRecord;
use std::collections::hash_map::HashMap;
use std::sync::{Arc, Mutex, RwLock};

pub struct BundleMapper {
  counter: Arc<Counter>,
  records: RwLock<HashMap<u64, Arc<Mutex<BundleRecord>>>>,
  hashes: RwLock<HashMap<String, u64>>,
}

impl Mapper for BundleMapper {
  type Record = BundleRecord;

  fn new(counter: Arc<Counter>) -> Result<Self> {
    let records = RwLock::new(HashMap::new());
    let hashes = RwLock::new(HashMap::new());
    Ok(Self {
      counter,
      records,
      hashes,
    })
  }

  fn records(&self) -> &RwLock<HashMap<u64, Arc<Mutex<BundleRecord>>>> {
    &self.records
  }

  fn hashes(&self) -> &RwLock<HashMap<String, u64>> {
    &self.hashes
  }

  fn next_counter(&self) -> u64 {
    self.counter.next_bundle()
  }
}
