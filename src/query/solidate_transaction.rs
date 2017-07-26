use mysql;
use query::Result;

pub struct SolidateTransaction<'a> {
  trunk_stmt: mysql::Stmt<'a>,
  branch_stmt: mysql::Stmt<'a>,
}

impl<'a> SolidateTransaction<'a> {
  pub fn new(pool: &mysql::Pool) -> Result<Self> {
    Ok(Self {
      trunk_stmt: pool.prepare(
        r#"
          UPDATE tx SET height = :height, solid = :solid WHERE id_tx = :id_tx
        "#,
      )?,
      branch_stmt: pool.prepare(
        r#"
          UPDATE tx SET solid = :solid WHERE id_tx = :id_tx
        "#,
      )?,
    })
  }

  pub fn trunk(
    &mut self,
    id: u64,
    height: i32,
    solid: u8,
  ) -> Result<mysql::QueryResult> {
    Ok(self.trunk_stmt.execute(params!{
      "id_tx" => id,
      "height" => height,
      "solid" => solid,
    })?)
  }

  pub fn branch(&mut self, id: u64, solid: u8) -> Result<mysql::QueryResult> {
    Ok(self.branch_stmt.execute(params!{
      "id_tx" => id,
      "solid" => solid,
    })?)
  }
}
