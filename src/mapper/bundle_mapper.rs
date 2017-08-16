use super::{BundleRecord, Mapper, Result};
use mysql;
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex, RwLock};

type Records = RwLock<BTreeMap<u64, Arc<Mutex<BundleRecord>>>>;
type Hashes = RwLock<HashMap<String, u64>>;

pub struct BundleMapper {
  counter: Mutex<u64>,
  records: Records,
  hashes: Hashes,
}

impl<'a> Mapper<'a> for BundleMapper {
  type Record = BundleRecord;
  type Indices = ();

  fn new(conn: &mut mysql::Conn) -> Result<Self> {
    let counter = Self::init_counter(
      conn,
      r"SELECT id_bundle FROM bundle ORDER BY id_bundle DESC LIMIT 1",
    )?;
    let records = RwLock::new(BTreeMap::new());
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

  fn store_indices(_indices: &mut (), _record: &BundleRecord) {}

  fn remove_indices(_indices: &mut (), _record: &BundleRecord) {}
}
