mod error;

pub use self::error::{Error, Result};
use mysql;
use std::sync::Mutex;

#[derive(Debug)]
pub struct Counters {
  transaction: Mutex<u64>,
  address: Mutex<u64>,
  bundle: Mutex<u64>,
}

impl Counters {
  pub fn new(pool: &mysql::Pool) -> Result<Self> {
    let transaction = Self::fetch_counter(
      pool,
      r"SELECT id_tx FROM tx ORDER BY id_tx DESC LIMIT 1",
      "id_tx",
    )?;
    let address = Self::fetch_counter(
      pool,
      r"SELECT id_address FROM address ORDER BY id_address DESC LIMIT 1",
      "id_address",
    )?;
    let bundle = Self::fetch_counter(
      pool,
      r"SELECT id_bundle FROM bundle ORDER BY id_bundle DESC LIMIT 1",
      "id_bundle",
    )?;
    Ok(Self {
      transaction: Mutex::new(transaction),
      address: Mutex::new(address),
      bundle: Mutex::new(bundle),
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

  fn fetch_counter(
    pool: &mysql::Pool,
    query: &str,
    column: &str,
  ) -> Result<u64> {
    match pool.get_conn()?.first(query)? {
      Some(mut result) => {
        Ok(result.take_opt(column).ok_or(Error::IdColumnNotFound)??)
      }
      None => Ok(0),
    }
  }
}
