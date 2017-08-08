use super::{Mapper, Result};
use counter::Counter;
use mysql;
use record::{AddressRecord, Record};
use std::collections::hash_map::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};

type AddressData = (HashMap<u64, AddressRecord>, HashMap<String, u64>);

pub struct AddressMapper {
  counter: Arc<Counter>,
  data: Mutex<AddressData>,
}

impl Mapper for AddressMapper {
  type Data = AddressData;
  type Record = AddressRecord;

  fn new(counter: Arc<Counter>) -> Result<Self> {
    let data = Mutex::new((HashMap::new(), HashMap::new()));
    Ok(Self { counter, data })
  }

  fn lock(&self) -> MutexGuard<AddressData> {
    self.data.lock().unwrap()
  }

  fn records<'a>(
    guard: &'a mut MutexGuard<AddressData>,
  ) -> &'a mut HashMap<u64, AddressRecord> {
    let (ref mut records, _) = **guard;
    records
  }
}

impl AddressMapper {
  pub fn fetch_or_insert(
    &self,
    conn: &mut mysql::Conn,
    hash: &str,
  ) -> Result<u64> {
    let (ref mut records, ref mut hashes) = *self.data.lock().unwrap();
    match hashes.get(hash) {
      Some(&id_address) => Ok(id_address),
      None => {
        let record = match AddressRecord::find_by_address(conn, hash)? {
          Some(record) => record,
          None => {
            let id_address = self.counter.next_address();
            let mut record = AddressRecord::new(id_address, hash.to_owned())?;
            record.insert(conn)?;
            record
          }
        };
        let id_address = record.id_address();
        record.store(records, hashes);
        Ok(id_address)
      }
    }
  }
}
