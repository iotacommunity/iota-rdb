use counters::Counters;
use mysql;
use query::Result;
use std::sync::Arc;

pub struct InsertTransactionPlaceholder<'a> {
  counters: Arc<Counters>,
  stmt: mysql::Stmt<'a>,
}

impl<'a> InsertTransactionPlaceholder<'a> {
  pub fn new(pool: &mysql::Pool, counters: Arc<Counters>) -> Result<Self> {
    Ok(Self {
      counters,
      stmt: pool.prepare(
        r#"
          INSERT INTO tx (
            id_tx, hash, da, height, solid
          ) VALUES (
            :id_tx, :hash, 1, :height, :solid
          )
        "#,
      )?,
    })
  }

  pub fn exec(&mut self, hash: &str, height: i32, solid: u8) -> Result<u64> {
    let id_tx = self.counters.next_transaction();
    self.stmt.execute(params!{
      "id_tx" => id_tx,
      "hash" => hash,
      "height" => height,
      "solid" => solid,
    })?;
    Ok(id_tx)
  }
}
