use counters::Counters;
use mysql;
use query::{Error, Result};

pub struct FetchBundle<'a> {
  select_stmt: mysql::Stmt<'a>,
  insert_stmt: mysql::Stmt<'a>,
}

impl<'a> FetchBundle<'a> {
  pub fn new(pool: &mysql::Pool) -> Result<Self> {
    Ok(Self {
      select_stmt: pool.prepare(
        r#"
          SELECT id_bundle FROM bundle WHERE bundle = :bundle
        "#,
      )?,
      insert_stmt: pool.prepare(
        r#"
          INSERT INTO bundle (
            id_bundle, bundle, created, size
          ) VALUES (
            :id_bundle, :bundle, :created, :size
          )
        "#,
      )?,
    })
  }

  pub fn exec(
    &mut self,
    counters: &Counters,
    created: f64,
    bundle: &str,
    size: i32,
  ) -> Result<u64> {
    match self.select_stmt.first_exec(params!{"bundle" => bundle})? {
      Some(mut result) => Ok(
        result.take_opt("id_bundle").ok_or(Error::ColumnNotFound)??,
      ),
      None => {
        let id_bundle = counters.next_bundle();
        self.insert_stmt.execute(params!{
          "id_bundle" => id_bundle,
          "bundle" => bundle,
          "created" => created,
          "size" => size,
        })?;
        Ok(id_bundle)
      }
    }
  }
}
