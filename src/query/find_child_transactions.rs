use mysql;
use query::Result;

pub struct FindChildTransactions<'a> {
  stmt: mysql::Stmt<'a>,
}

pub struct FindChildTransactionsResult {
  pub id_tx: u64,
  pub id_trunk: u64,
  pub id_branch: u64,
  pub height: i32,
  pub solid: u8,
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
      let row = row?;
      let (id_tx, id_trunk, id_branch, height, solid): (
        u64,
        Option<u64>,
        Option<u64>,
        Option<i32>,
        Option<u8>,
      ) = mysql::from_row_opt(row)?;
      records.push(FindChildTransactionsResult {
        id_tx,
        id_trunk: id_trunk.unwrap_or(0),
        id_branch: id_branch.unwrap_or(0),
        height: height.unwrap_or(0),
        solid: solid.unwrap_or(0b00),
      });
    }
    Ok(records)
  }
}
