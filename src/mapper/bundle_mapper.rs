use super::{BundleRecord, Garbage, Hashes, Index, Mapper, Record, Records,
            Result};
use mysql;
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex, RwLock, RwLockWriteGuard};

pub struct BundleMapper {
  counter: Mutex<u64>,
  records: RwLock<Records<BundleRecord>>,
  hashes: RwLock<Hashes>,
  indices: [RwLock<Records<Index>>; 1],
}

impl Mapper for BundleMapper {
  type Record = BundleRecord;

  fn new(conn: &mut mysql::Conn) -> Result<Self> {
    let counter = Self::init_counter(
      conn,
      r"SELECT id_bundle FROM bundle ORDER BY id_bundle DESC LIMIT 1",
    )?;
    let records = RwLock::new(BTreeMap::new());
    let hashes = RwLock::new(HashMap::new());
    let indices = [RwLock::new(BTreeMap::new())];
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

  fn records(&self) -> &RwLock<Records<BundleRecord>> {
    &self.records
  }

  fn hashes(&self) -> &RwLock<Hashes> {
    &self.hashes
  }

  fn indices(&self) -> &[RwLock<Records<Index>>] {
    &self.indices
  }

  fn fill_indices(
    indices: &mut [RwLockWriteGuard<Records<Index>>],
    record: &BundleRecord,
  ) {
    let inner = if record.is_persisted() {
      None
    } else {
      Some(Vec::new())
    };
    indices[0].insert(record.id(), Arc::new(Mutex::new(inner)));
  }

  fn mark_garbage(_garbage: &Garbage<BundleRecord>) {}
}

impl BundleMapper {
  pub fn transaction_index(
    &self,
    id: u64,
  ) -> Option<Arc<Mutex<Option<Vec<u64>>>>> {
    debug!("Mutex check at line {}", line!());
    let transactions = self.indices[0].read().unwrap();
    debug!("Mutex check at line {}", line!());
    transactions.get(&id).cloned()
  }
}
