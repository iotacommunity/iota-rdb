use super::Result;
use counter::Counter;
use mysql;
use query;
use std::sync::Arc;

pub struct AddressMapper {
  counter: Arc<Counter>,
}

impl AddressMapper {
  pub fn new(counter: Arc<Counter>) -> Result<Self> {
    Ok(Self { counter })
  }

  pub fn fetch(&self, conn: &mut mysql::Conn, address: &str) -> Result<u64> {
    Ok(query::fetch_address(conn, &self.counter, address)?)
  }

  pub fn flush(&self, _conn: &mut mysql::Conn) -> Result<()> {
    Ok(())
  }
}
