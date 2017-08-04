use super::Result;
use counter::Counter;
use mysql;
use query;
use std::sync::Arc;

pub struct Bundle {
  counter: Arc<Counter>,
}

impl Bundle {
  pub fn new(counter: Arc<Counter>) -> Self {
    Self { counter }
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
}
