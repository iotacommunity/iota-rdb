use counters::Counters;
use mysql;
use query::{Error, Result};

pub struct FetchAddress<'a> {
  select_stmt: mysql::Stmt<'a>,
  insert_stmt: mysql::Stmt<'a>,
}

impl<'a> FetchAddress<'a> {
  pub fn new(pool: &mysql::Pool) -> Result<Self> {
    Ok(Self {
      select_stmt: pool.prepare(
        r#"
          SELECT id_address FROM address WHERE address = :address
        "#,
      )?,
      insert_stmt: pool.prepare(
        r#"
          INSERT INTO address (
            id_address, address
          ) VALUES (
            :id_address, :address
          )
        "#,
      )?,
    })
  }

  pub fn exec(&mut self, counters: &Counters, address: &str) -> Result<u64> {
    match self.select_stmt.first_exec(params!{"address" => address})? {
      Some(mut result) => Ok(
        result.take_opt("id_address").ok_or(Error::ColumnNotFound)??,
      ),
      None => {
        let id_address = counters.next_address();
        self.insert_stmt.execute(params!{
          "id_address" => id_address,
          "address" => address,
        })?;
        Ok(id_address)
      }
    }
  }
}
