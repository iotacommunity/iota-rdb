use mysql;
use query::Result;

pub struct UpdateBundle<'a> {
  stmt: mysql::Stmt<'a>,
}

impl<'a> UpdateBundle<'a> {
  pub fn new(pool: &mysql::Pool) -> Result<Self> {
    Ok(Self {
      stmt: pool.prepare(
        r#"
          UPDATE bundle SET
            confirmed = :confirmed
          WHERE id_bundle = :id_bundle
        "#,
      )?,
    })
  }

  pub fn exec(
    &mut self,
    id: u64,
    confirmed: f64,
  ) -> Result<mysql::QueryResult> {
    Ok(self.stmt.execute(params!{
      "id_bundle" => id,
      "confirmed" => confirmed,
    })?)
  }
}
