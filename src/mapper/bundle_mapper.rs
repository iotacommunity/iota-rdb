use super::Mapper;
use counter::Counter;
use record::{BundleRecord, Result};
use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};

type Records = RwLock<HashMap<u64, Arc<Mutex<BundleRecord>>>>;
type Hashes = RwLock<HashMap<String, u64>>;

pub struct BundleMapper {
  counter: Arc<Counter>,
  records: Records,
  hashes: Hashes,
}

impl<'a> Mapper<'a> for BundleMapper {
  type Record = BundleRecord;
  type Indices = ();

  fn new(counter: Arc<Counter>) -> Result<Self> {
    let records = RwLock::new(HashMap::new());
    let hashes = RwLock::new(HashMap::new());
    Ok(Self {
      counter,
      records,
      hashes,
    })
  }

  fn records(&self) -> &Records {
    &self.records
  }

  fn hashes(&self) -> &Hashes {
    &self.hashes
  }

  fn indices(&self) {}

  fn store_indices(_indices: (), _record: &BundleRecord) {}

  fn next_counter(&self) -> u64 {
    self.counter.next_bundle()
  }
}
