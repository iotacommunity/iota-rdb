use super::{Mapper, Result};
use counter::Counter;
use mysql;
use record::{BundleRecord, Record};
use std::collections::hash_map::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};

type BundleData = (HashMap<u64, BundleRecord>, HashMap<String, u64>);

pub struct BundleMapper {
  counter: Arc<Counter>,
  data: Mutex<BundleData>,
}

impl Mapper for BundleMapper {
  type Data = BundleData;
  type Record = BundleRecord;

  fn new(counter: Arc<Counter>) -> Result<Self> {
    let data = Mutex::new((HashMap::new(), HashMap::new()));
    Ok(Self { counter, data })
  }

  fn lock(&self) -> MutexGuard<BundleData> {
    self.data.lock().unwrap()
  }

  fn records<'a>(
    guard: &'a mut MutexGuard<BundleData>,
  ) -> &'a mut HashMap<u64, BundleRecord> {
    let (ref mut records, _) = **guard;
    records
  }
}

impl BundleMapper {
  pub fn fetch_or_insert(
    &self,
    conn: &mut mysql::Conn,
    hash: &str,
    size: i32,
    created: f64,
  ) -> Result<u64> {
    let (ref mut records, ref mut hashes) = *self.data.lock().unwrap();
    match hashes.get(hash) {
      Some(&id_bundle) => Ok(id_bundle),
      None => {
        let record = match BundleRecord::find_by_bundle(conn, hash)? {
          Some(record) => record,
          None => {
            let id_bundle = self.counter.next_bundle();
            let mut record =
              BundleRecord::new(id_bundle, hash.to_owned(), size, created);
            record.insert(conn)?;
            record
          }
        };
        let id_bundle = record.id_bundle();
        record.store(records, hashes);
        Ok(id_bundle)
      }
    }
  }
}
