use mysql;
use std::fmt;
use std::sync::Mutex;

pub struct Counter {
  transaction: Mutex<u64>,
  address: Mutex<u64>,
  bundle: Mutex<u64>,
}

impl Counter {
  pub fn new(mysql_uri: &str) -> mysql::Result<Self> {
    let mut conn = mysql::Conn::new(mysql_uri)?;
    let transaction = Self::fetch_counter(
      &mut conn,
      r"SELECT id_tx FROM tx ORDER BY id_tx DESC LIMIT 1",
    )?;
    let address = Self::fetch_counter(
      &mut conn,
      r"SELECT id_address FROM address ORDER BY id_address DESC LIMIT 1",
    )?;
    let bundle = Self::fetch_counter(
      &mut conn,
      r"SELECT id_bundle FROM bundle ORDER BY id_bundle DESC LIMIT 1",
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

  fn fetch_counter(conn: &mut mysql::Conn, query: &str) -> mysql::Result<u64> {
    match conn.first(query)? {
      Some(row) => {
        let (id,) = mysql::from_row_opt(row)?;
        Ok(id)
      }
      None => Ok(0),
    }
  }
}

impl fmt::Display for Counter {
  fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
    write!(
      f,
      "tx:{}, address:{}, bundle:{}",
      *self.transaction.lock().expect("Mutex is poisoned"),
      *self.address.lock().expect("Mutex is poisoned"),
      *self.bundle.lock().expect("Mutex is poisoned"),
    )
  }
}
