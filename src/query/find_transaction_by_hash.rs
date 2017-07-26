use mysql;
use query::{Error, Result};

pub struct FindTransactionByHash<'a> {
  stmt: mysql::Stmt<'a>,
}

pub struct FindTransactionByHashResult {
  pub id_tx: mysql::Result<u64>,
  pub id_trunk: mysql::Result<u64>,
  pub id_branch: mysql::Result<u64>,
  pub height: mysql::Result<i32>,
  pub solid: mysql::Result<u8>,
}

impl<'a> FindTransactionByHash<'a> {
  pub fn new(pool: &mysql::Pool) -> Result<Self> {
    Ok(Self {
      stmt: pool.prepare(
        r#"
        SELECT
          id_tx, id_trunk, id_branch, height, solid
        FROM tx
        WHERE hash = :hash
      "#,
      )?,
    })
  }

  pub fn exec(
    &mut self,
    hash: &str,
  ) -> Result<Option<FindTransactionByHashResult>> {
    match self.stmt.first_exec(params!{"hash" => hash})? {
      Some(mut row) => Ok(Some(FindTransactionByHashResult {
        id_tx: row.take_opt("id_tx").ok_or(Error::ColumnNotFound)?,
        id_trunk: row.take_opt("id_trunk").ok_or(Error::ColumnNotFound)?,
        id_branch: row.take_opt("id_branch").ok_or(Error::ColumnNotFound)?,
        height: row.take_opt("height").ok_or(Error::ColumnNotFound)?,
        solid: row.take_opt("solid").ok_or(Error::ColumnNotFound)?,
      })),
      None => Ok(None),
    }
  }
}
