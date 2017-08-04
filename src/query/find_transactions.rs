use super::Result;
use mysql;

#[derive(Clone)]
pub struct FindTransactionsResult {
  pub id_tx: u64,
  pub id_trunk: u64,
  pub id_branch: u64,
  pub height: i32,
  pub solid: u8,
}

type ResultTuple = (
  u64,
  String,
  Option<u64>,
  Option<u64>,
  Option<i32>,
  Option<u8>,
);

const QUERY: &str = r#"
  SELECT
    id_tx, hash, id_trunk, id_branch, height, solid
  FROM tx
  WHERE hash IN (?, ?, ?)
"#;

pub fn find_transactions(
  conn: &mut mysql::Conn,
  current_hash: &str,
  trunk_hash: &str,
  branch_hash: &str,
) -> Result<
  (
    Option<FindTransactionsResult>,
    Option<FindTransactionsResult>,
    Option<FindTransactionsResult>,
  ),
> {
  let (mut current_tx, mut trunk_tx, mut branch_tx) = (None, None, None);
  let results = conn
    .prep_exec(QUERY, (current_hash, trunk_hash, branch_hash))?;
  for row in results {
    let row = row?;
    let (id_tx, hash, id_trunk, id_branch, height, solid): ResultTuple =
      mysql::from_row_opt(row)?;
    let record = FindTransactionsResult {
      id_tx,
      id_trunk: id_trunk.unwrap_or(0),
      id_branch: id_branch.unwrap_or(0),
      height: height.unwrap_or(0),
      solid: solid.unwrap_or(0b00),
    };
    if hash == current_hash {
      current_tx = Some(record);
    } else if hash == trunk_hash {
      trunk_tx = Some(record);
    } else if hash == branch_hash {
      branch_tx = Some(record);
    }
  }
  Ok((current_tx, trunk_tx, branch_tx))
}
