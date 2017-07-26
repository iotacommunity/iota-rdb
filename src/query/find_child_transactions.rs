use mysql;
use query::{Error, Result};

pub struct FindChildTransactions<'a> {
  stmt: mysql::Stmt<'a>,
}

pub struct FindChildTransactionsResult {
  pub id_tx: mysql::Result<u64>,
  pub id_trunk: mysql::Result<u64>,
  pub id_branch: mysql::Result<u64>,
  pub height: mysql::Result<i32>,
  pub solid: mysql::Result<u8>,
}

impl<'a> FindChildTransactions<'a> {
  pub fn new(pool: &mysql::Pool) -> Result<Self> {
    Ok(Self {
      stmt: pool.prepare(
        r#"
          SELECT
            id_tx, id_trunk, id_branch, height, solid
          FROM tx
          WHERE id_trunk = :id_tx OR id_branch = :id_tx
        "#,
      )?,
    })
  }

  pub fn exec(
    &mut self,
    id_tx: u64,
  ) -> Result<Vec<FindChildTransactionsResult>> {
    let mut records = Vec::new();
    let results = self.stmt.execute(params!{"id_tx" => id_tx})?;
    for row in results {
      let mut row = row?;
      records.push(FindChildTransactionsResult {
        id_tx: row.take_opt("id_tx").ok_or(Error::ColumnNotFound)?,
        id_trunk: row.take_opt("id_trunk").ok_or(Error::ColumnNotFound)?,
        id_branch: row.take_opt("id_branch").ok_or(Error::ColumnNotFound)?,
        height: row.take_opt("height").ok_or(Error::ColumnNotFound)?,
        solid: row.take_opt("solid").ok_or(Error::ColumnNotFound)?,
      });
    }
    Ok(records)
  }
}
