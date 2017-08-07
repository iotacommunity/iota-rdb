use super::Result;
use counter::Counter;
use mysql;
use query;
use std::sync::Arc;

pub struct BundleMapper {
  counter: Arc<Counter>,
}

impl BundleMapper {
  pub fn new(counter: Arc<Counter>) -> Result<Self> {
    Ok(Self { counter })
  }

  pub fn fetch(
    &self,
    conn: &mut mysql::Conn,
    created: f64,
    bundle: &str,
    size: i32,
  ) -> Result<u64> {
    Ok(query::fetch_bundle(
      conn,
      &self.counter,
      created,
      bundle,
      size,
    )?)
  }

  pub fn flush(&self, _conn: &mut mysql::Conn) -> Result<()> {
    Ok(())
  }
}
