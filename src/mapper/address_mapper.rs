use super::{AddressRecord, Mapper, Result};
use counter::Counter;
use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};

type Records = RwLock<HashMap<u64, Arc<Mutex<AddressRecord>>>>;
type Hashes = RwLock<HashMap<String, u64>>;

pub struct AddressMapper {
  counter: Arc<Counter>,
  records: Records,
  hashes: Hashes,
}

impl<'a> Mapper<'a> for AddressMapper {
  type Record = AddressRecord;
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

  fn store_indices(_indices: &mut (), _record: &AddressRecord) {}

  fn next_counter(&self) -> u64 {
    self.counter.next_address()
  }
}
