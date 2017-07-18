mod error;

pub use self::error::{Error, Result};
use mysql;
use std::sync::Mutex;
use std::sync::PoisonError;

pub struct Counters {
  transaction: Mutex<u64>,
  address: Mutex<u64>,
  bundle: Mutex<u64>,
}

impl Counters {
  pub fn new(pool: &mysql::Pool) -> Result<Self> {
    Ok(Self {
      transaction: Mutex::new(0),
      address: Mutex::new(0),
      bundle: Mutex::new(0),
    })
  }

  pub fn next_transaction(&self) -> u64 {
    let mut counter = self.transaction.lock().expect("Mutex is poisoned");
    *counter += 1;
    *counter
  }

  pub fn next_address(&self) -> u64 {
    let mut counter = self.address.lock().expect("Mutex is poisoned");
    *counter += 1;
    *counter
  }

  pub fn next_bundle(&self) -> u64 {
    let mut counter = self.bundle.lock().expect("Mutex is poisoned");
    *counter += 1;
    *counter
  }
}
