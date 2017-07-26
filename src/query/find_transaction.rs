use mysql;
use query::{Error, Result};

pub struct FindTransaction<'a> {
  stmt: mysql::Stmt<'a>,
}

pub struct FindTransactionResult {
  pub id_trunk: mysql::Result<u64>,
  pub id_branch: mysql::Result<u64>,
  pub id_bundle: mysql::Result<u64>,
  pub current_idx: mysql::Result<i32>,
  pub mst_a: mysql::Result<bool>,
}

impl<'a> FindTransaction<'a> {
  pub fn new(pool: &mysql::Pool) -> Result<Self> {
    Ok(Self {
      stmt: pool.prepare(
        r#"
          SELECT
            id_trunk, id_branch, id_bundle, current_idx, mst_a
          FROM tx
          WHERE id_tx = :id_tx
        "#,
      )?,
    })
  }

  pub fn exec(&mut self, id_tx: u64) -> Result<FindTransactionResult> {
    let mut row = self
      .stmt
      .first_exec(params!{"id_tx" => id_tx})?
      .ok_or(Error::RecordNotFound)?;
    Ok(FindTransactionResult {
      id_trunk: row.take_opt("id_trunk").ok_or(Error::ColumnNotFound)?,
      id_branch: row.take_opt("id_branch").ok_or(Error::ColumnNotFound)?,
      id_bundle: row.take_opt("id_bundle").ok_or(Error::ColumnNotFound)?,
      current_idx: row.take_opt("current_idx").ok_or(Error::ColumnNotFound)?,
      mst_a: row.take_opt("mst_a").ok_or(Error::ColumnNotFound)?,
    })
  }
}
