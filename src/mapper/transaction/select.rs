use super::Transaction;
use super::super::{Error, Result};
use mysql;

const QUERY_BASE: &str = r#"
  SELECT
    hash,
    id_tx,
    id_trunk,
    id_branch,
    id_address,
    id_bundle,
    tag,
    value,
    timestamp,
    current_idx,
    last_idx,
    da,
    height,
    is_mst,
    mst_a,
    solid
  FROM tx
"#;

const QUERY_ONE: &str = r"WHERE hash = ?";
const QUERY_TWO: &str = r"WHERE hash IN (?, ?)";
const QUERY_THREE: &str = r"WHERE hash IN (?, ?, ?)";

impl Transaction {
  pub fn find(
    conn: &mut mysql::Conn,
    hashes: &[&str],
  ) -> Result<Vec<Option<Transaction>>> {
    let mut all_results = Vec::new();
    for hashes in hashes.chunks(3) {
      let mut results = vec![None; hashes.len()];
      let rows = match hashes.len() {
        1 => conn
          .prep_exec(format!("{} {}", QUERY_BASE, QUERY_ONE), (hashes[0],))?,
        2 => conn.prep_exec(
          format!("{} {}", QUERY_BASE, QUERY_TWO),
          (hashes[0], hashes[1]),
        )?,
        3 => conn.prep_exec(
          format!("{} {}", QUERY_BASE, QUERY_THREE),
          (hashes[0], hashes[1], hashes[2]),
        )?,
        _ => unreachable!(),
      };
      for row in rows {
        let mut row = row?;
        let hash: String = row.take_opt("hash").ok_or(Error::ColumnNotFound)??;
        for (i, input_hash) in hashes.iter().enumerate() {
          if &hash == input_hash {
            results[i] = Some(Transaction::from_row(&mut row, hash)?);
            break;
          }
        }
      }
      all_results.extend(results);
    }
    Ok(all_results)
  }

  pub fn from_row(row: &mut mysql::Row, hash: String) -> Result<Self> {
    Ok(Self {
      locked: false,
      persistent: true,
      modified: false,
      hash,
      id_tx: row.take_opt("id_tx").ok_or(Error::ColumnNotFound)??,
      id_trunk: take_column(row, "id_trunk", 0)?,
      id_branch: take_column(row, "id_branch", 0)?,
      id_address: take_column(row, "id_address", 0)?,
      id_bundle: take_column(row, "id_bundle", 0)?,
      tag: row.take_opt("tag").ok_or(Error::ColumnNotFound)??,
      value: take_column(row, "value", 0)?,
      timestamp: take_column(row, "timestamp", 0)?,
      current_idx: take_column(row, "current_idx", 0)?,
      last_idx: take_column(row, "last_idx", 0)?,
      da: take_column(row, "da", 0)?,
      height: take_column(row, "height", 0)?,
      is_mst: take_column(row, "is_mst", false)?,
      mst_a: take_column(row, "mst_a", false)?,
      solid: take_column(row, "solid", 0b00)?,
    })
  }
}

fn take_column<T>(row: &mut mysql::Row, column: &str, default: T) -> Result<T>
where
  T: mysql::value::FromValue,
{
  match row.take_opt(column) {
    Some(value) => Ok(value?),
    None => Ok(default),
  }
}
