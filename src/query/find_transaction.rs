use mysql;
use query::{Error, Result};

pub struct FindTransaction<'a> {
  stmt: mysql::Stmt<'a>,
}

pub struct FindTransactionResult {
  pub id_trunk: u64,
  pub id_branch: u64,
  pub id_bundle: Option<u64>,
  pub current_idx: Option<i32>,
  pub mst_a: bool,
}

type ResultTuple = (
  Option<u64>,
  Option<u64>,
  Option<u64>,
  Option<i32>,
  Option<bool>,
);

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
    let (id_trunk, id_branch, id_bundle, current_idx, mst_a): ResultTuple =
      mysql::from_row_opt(self
        .stmt
        .first_exec(params!{"id_tx" => id_tx})?
        .ok_or(Error::RecordNotFound)?)?;
    Ok(FindTransactionResult {
      id_trunk: id_trunk.unwrap_or(0),
      id_branch: id_branch.unwrap_or(0),
      id_bundle,
      current_idx,
      mst_a: mst_a.unwrap_or(false),
    })
  }
}
