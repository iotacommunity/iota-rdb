use super::Result;
use mysql;

#[derive(Clone)]
pub struct FindTransactionsResult {
  pub id_tx: u64,
  pub id_trunk: u64,
  pub id_branch: u64,
  pub da: i32,
  pub height: i32,
  pub solid: u8,
}

type ResultTuple = (
  u64,
  String,
  Option<u64>,
  Option<u64>,
  Option<i32>,
  Option<i32>,
  Option<u8>,
);

const QUERY_THREE: &str = r#"
  SELECT
    id_tx, hash, id_trunk, id_branch, da, height, solid
  FROM tx
  WHERE hash IN (?, ?, ?)
"#;

const QUERY_TWO: &str = r#"
  SELECT
    id_tx, hash, id_trunk, id_branch, da, height, solid
  FROM tx
  WHERE hash IN (?, ?)
"#;

const QUERY_ONE: &str = r#"
  SELECT
    id_tx, hash, id_trunk, id_branch, da, height, solid
  FROM tx
  WHERE hash = ?
"#;

pub fn find_transactions(
  conn: &mut mysql::Conn,
  hashes: &[&str],
) -> Result<Vec<Option<FindTransactionsResult>>> {
  let mut all_results = Vec::new();
  for hashes in hashes.chunks(3) {
    let mut results = vec![None; hashes.len()];
    let rows = match hashes.len() {
      1 => conn.prep_exec(QUERY_ONE, (hashes[0],))?,
      2 => conn.prep_exec(QUERY_TWO, (hashes[0], hashes[1]))?,
      3 => conn
        .prep_exec(QUERY_THREE, (hashes[0], hashes[1], hashes[2]))?,
      _ => unreachable!(),
    };
    for row in rows {
      let row = row?;
      let (id_tx, hash, id_trunk, id_branch, da, height, solid): ResultTuple =
        mysql::from_row_opt(row)?;
      let record = FindTransactionsResult {
        id_tx,
        id_trunk: id_trunk.unwrap_or(0),
        id_branch: id_branch.unwrap_or(0),
        da: da.unwrap_or(0),
        height: height.unwrap_or(0),
        solid: solid.unwrap_or(0b00),
      };
      for (i, input_hash) in hashes.iter().enumerate() {
        if &hash == input_hash {
          results[i] = Some(record);
          break;
        }
      }
    }
    all_results.extend(results);
  }
  Ok(all_results)
}
