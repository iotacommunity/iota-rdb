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

const WHERE_ID: &str = r"WHERE id_tx = ?";
const WHERE_HASH_ONE: &str = r"WHERE hash = ?";
const WHERE_HASH_TWO: &str = r"WHERE hash IN (?, ?)";
const WHERE_HASH_THREE: &str = r"WHERE hash IN (?, ?, ?)";

impl Transaction {
  pub fn find_by_id(conn: &mut mysql::Conn, id: u64) -> Result<Transaction> {
    Ok(conn
      .first_exec(format!("{} {}", QUERY_BASE, WHERE_ID), (id,))?
      .ok_or(Error::RecordNotFound)
      .and_then(|mut row| Transaction::from_row(&mut row))?)
  }

  pub fn find_by_hashes(
    conn: &mut mysql::Conn,
    hashes: &[&str],
  ) -> Result<Vec<Transaction>> {
    let mut results = Vec::new();
    for hashes in hashes.chunks(3) {
      let rows =
        match hashes.len() {
          1 => conn.prep_exec(
            format!("{} {}", QUERY_BASE, WHERE_HASH_ONE),
            (hashes[0],),
          )?,
          2 => conn.prep_exec(
            format!("{} {}", QUERY_BASE, WHERE_HASH_TWO),
            (hashes[0], hashes[1]),
          )?,
          3 => conn.prep_exec(
            format!("{} {}", QUERY_BASE, WHERE_HASH_THREE),
            (hashes[0], hashes[1], hashes[2]),
          )?,
          _ => unreachable!(),
        };
      for row in rows {
        results.push(Transaction::from_row(&mut row?)?);
      }
    }
    Ok(results)
  }

  pub fn from_row(row: &mut mysql::Row) -> Result<Self> {
    Ok(Self {
      locked: false,
      persistent: true,
      modified: false,
      hash: row.take_opt("hash").ok_or(Error::ColumnNotFound)??,
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
