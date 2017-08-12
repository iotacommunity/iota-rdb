use super::{AddressRecord, Mapper, Result};
use mysql;
use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};

type Records = RwLock<HashMap<u64, Arc<Mutex<AddressRecord>>>>;
type Hashes = RwLock<HashMap<String, u64>>;

pub struct AddressMapper {
  counter: Mutex<u64>,
  records: Records,
  hashes: Hashes,
}

impl<'a> Mapper<'a> for AddressMapper {
  type Record = AddressRecord;
  type Indices = ();

  fn new(conn: &mut mysql::Conn) -> Result<Self> {
    let counter = Self::init_counter(
      conn,
      r"SELECT id_address FROM address ORDER BY id_address DESC LIMIT 1",
    )?;
    let records = RwLock::new(HashMap::new());
    let hashes = RwLock::new(HashMap::new());
    Ok(Self {
      counter,
      records,
      hashes,
    })
  }

  fn counter(&self) -> &Mutex<u64> {
    &self.counter
  }

  fn records(&self) -> &Records {
    &self.records
  }

  fn hashes(&self) -> &Hashes {
    &self.hashes
  }

  fn indices(&self) {}

  fn store_indices(_indices: &mut (), _record: &AddressRecord) {}
}
