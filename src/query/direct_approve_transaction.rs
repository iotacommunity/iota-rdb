use mysql;
use query::Result;

pub struct DirectApproveTransaction<'a> {
  stmt: mysql::Stmt<'a>,
}

impl<'a> DirectApproveTransaction<'a> {
  pub fn new(pool: &mysql::Pool) -> Result<Self> {
    Ok(Self {
      stmt: pool.prepare(
        r#"
          UPDATE tx SET da = da + 1 WHERE id_tx = :id_tx
        "#,
      )?,
    })
  }

  pub fn exec(&mut self, id: u64) -> Result<mysql::QueryResult> {
    Ok(self.stmt.execute(params!{"id_tx" => id})?)
  }
}
