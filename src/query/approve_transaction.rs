use mysql;
use query::Result;

pub struct ApproveTransaction<'a> {
  stmt: mysql::Stmt<'a>,
}

impl<'a> ApproveTransaction<'a> {
  pub fn new(pool: &mysql::Pool) -> Result<Self> {
    Ok(Self {
      stmt: pool.prepare(
        r#"
          UPDATE tx SET mst_a = :mst_a WHERE id_tx = :id_tx
        "#,
      )?,
    })
  }

  pub fn exec(&mut self, id: u64) -> Result<mysql::QueryResult> {
    Ok(self.stmt.execute(params!{
      "id_tx" => id,
      "mst_a" => true,
    })?)
  }
}
