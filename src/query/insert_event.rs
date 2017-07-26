use mysql;
use query::Result;

pub struct InsertEvent<'a> {
  stmt: mysql::Stmt<'a>,
}

impl<'a> InsertEvent<'a> {
  pub fn new(pool: &mysql::Pool) -> Result<Self> {
    Ok(Self {
      stmt: pool.prepare(
        r#"
          INSERT INTO txload (
            event, count, timestamp
          ) VALUES (
            :event, :count, :timestamp
          )
        "#,
      )?,
    })
  }

  pub fn new_transaction_received(&mut self, timestamp: f64) -> Result<()> {
    self.stmt.execute(params!{
      "event" => "NTX",
      "count" => 1,
      "timestamp" => timestamp,
    })?;
    Ok(())
  }

  pub fn milestone_received(&mut self, timestamp: f64) -> Result<()> {
    self.stmt.execute(params!{
      "event" => "MST",
      "count" => 1,
      "timestamp" => timestamp,
    })?;
    Ok(())
  }

  pub fn subtanble_confirmation(
    &mut self,
    timestamp: f64,
    count: i32,
  ) -> Result<()> {
    self.stmt.execute(params!{
      "event" => "CNF",
      "count" => count,
      "timestamp" => timestamp,
    })?;
    Ok(())
  }

  pub fn unsolid_transaction(&mut self, timestamp: f64) -> Result<()> {
    self.stmt.execute(params!{
      "event" => "UNS",
      "count" => 1,
      "timestamp" => timestamp,
    })?;
    Ok(())
  }

  pub fn subtangle_solidation(
    &mut self,
    timestamp: f64,
    count: i32,
  ) -> Result<()> {
    self.stmt.execute(params!{
      "event" => "SOL",
      "count" => count,
      "timestamp" => timestamp,
    })?;
    Ok(())
  }
}
