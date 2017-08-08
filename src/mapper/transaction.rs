use super::{Error, Result};
use mapper::Record;
use mysql;
use std::collections::HashMap;

#[derive(Clone)]
pub struct Transaction {
  locked: bool,
  persistent: bool,
  modified: bool,
  hash: String,
  id_tx: u64,
  id_trunk: u64,
  id_branch: u64,
  id_address: u64,
  id_bundle: u64,
  tag: String,
  value: i64,
  timestamp: f64,
  current_idx: i32,
  last_idx: i32,
  da: i32,
  height: i32,
  is_mst: bool,
  mst_a: bool,
  solid: u8,
}

const SELECT_QUERY: &str = r#"
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

const WHERE_HASH_ONE: &str = r"WHERE hash = ?";
const WHERE_HASH_TWO: &str = r"WHERE hash IN (?, ?)";
const WHERE_HASH_THREE: &str = r"WHERE hash IN (?, ?, ?)";

impl Record for Transaction {
  define_record!();

  const SELECT_QUERY: &'static str = SELECT_QUERY;
  const SELECT_WHERE_ID: &'static str = r"WHERE id_tx = ?";

  const INSERT_QUERY: &'static str = r#"
    INSERT INTO tx (
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
    ) VALUES (
      :hash,
      :id_tx,
      :id_trunk,
      :id_branch,
      :id_address,
      :id_bundle,
      :tag,
      :value,
      :timestamp,
      :current_idx,
      :last_idx,
      :da,
      :height,
      :is_mst,
      :mst_a,
      :solid
    )
  "#;

  const UPDATE_QUERY: &'static str = r#"
    UPDATE tx SET
      id_trunk = :id_trunk,
      id_branch = :id_branch,
      id_address = :id_address,
      id_bundle = :id_bundle,
      tag = :tag,
      value = :value,
      timestamp = :timestamp,
      current_idx = :current_idx,
      last_idx = :last_idx,
      da = :da,
      height = :height,
      is_mst = :is_mst,
      mst_a = :mst_a,
      solid = :solid
    WHERE id_tx = :id_tx
  "#;

  fn from_row(row: &mut mysql::Row) -> Result<Self> {
    Ok(Self {
      locked: false,
      persistent: true,
      modified: false,
      hash: row.take_opt("hash").ok_or(Error::ColumnNotFound)??,
      id_tx: row.take_opt("id_tx").ok_or(Error::ColumnNotFound)??,
      id_trunk: Self::take_column(row, "id_trunk", 0)?,
      id_branch: Self::take_column(row, "id_branch", 0)?,
      id_address: Self::take_column(row, "id_address", 0)?,
      id_bundle: Self::take_column(row, "id_bundle", 0)?,
      tag: row.take_opt("tag").ok_or(Error::ColumnNotFound)??, // TODO optional
      value: Self::take_column(row, "value", 0)?,
      timestamp: Self::take_column(row, "timestamp", 0.0)?,
      current_idx: Self::take_column(row, "current_idx", 0)?,
      last_idx: Self::take_column(row, "last_idx", 0)?,
      da: Self::take_column(row, "da", 0)?,
      height: Self::take_column(row, "height", 0)?,
      is_mst: Self::take_column(row, "is_mst", false)?,
      mst_a: Self::take_column(row, "mst_a", false)?,
      solid: Self::take_column(row, "solid", 0b00)?,
    })
  }

  fn to_params(&self) -> Vec<(String, mysql::Value)> {
    params!{
      "hash" => self.hash.clone(),
      "id_tx" => self.id_tx,
      "id_trunk" => self.id_trunk,
      "id_branch" => self.id_branch,
      "id_address" => self.id_address,
      "id_bundle" => self.id_bundle,
      "tag" => self.tag.clone(),
      "value" => self.value,
      "timestamp" => self.timestamp,
      "current_idx" => self.current_idx,
      "last_idx" => self.last_idx,
      "da" => self.da,
      "height" => self.height,
      "is_mst" => self.is_mst,
      "mst_a" => self.mst_a,
      "solid" => self.solid,
    }
  }
}

impl Transaction {
  define_getter!(hash, &str);
  define_getter!(id_tx, u64);
  define_accessors!(id_trunk, set_id_trunk, u64);
  define_accessors!(id_branch, set_id_branch, u64);
  define_accessors!(id_address, set_id_address, u64);
  define_accessors!(id_bundle, set_id_bundle, u64);
  define_getter!(tag, &str);
  define_setter!(tag, set_tag, String);
  define_accessors!(value, set_value, i64);
  define_accessors!(timestamp, set_timestamp, f64);
  define_accessors!(current_idx, set_current_idx, i32);
  define_accessors!(last_idx, set_last_idx, i32);
  define_accessors!(height, set_height, i32);
  define_accessors!(is_mst, set_is_mst, bool);
  define_accessors!(mst_a, set_mst_a, bool);
  define_accessors!(solid, set_solid, u8);

  pub fn placeholder(hash: String, id_tx: u64, solid: u8) -> Self {
    Self {
      locked: false,
      persistent: false,
      modified: true,
      hash,
      id_tx,
      id_trunk: 0,
      id_branch: 0,
      id_address: 0,
      id_bundle: 0,
      tag: String::from(""),
      value: 0,
      timestamp: 0.0,
      current_idx: 0,
      last_idx: 0,
      da: 0,
      height: 0,
      is_mst: false,
      mst_a: false,
      solid,
    }
  }

  pub fn find_by_hashes(
    conn: &mut mysql::Conn,
    hashes: &[&str],
  ) -> Result<Vec<Transaction>> {
    let mut results = Vec::new();
    for hashes in hashes.chunks(3) {
      let rows = match hashes.len() {
        1 => conn.prep_exec(
          format!("{} {}", SELECT_QUERY, WHERE_HASH_ONE),
          (hashes[0],),
        )?,
        2 => conn.prep_exec(
          format!("{} {}", SELECT_QUERY, WHERE_HASH_TWO),
          (hashes[0], hashes[1]),
        )?,
        3 => conn.prep_exec(
          format!("{} {}", SELECT_QUERY, WHERE_HASH_THREE),
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

  pub fn direct_approve(&mut self) {
    self.modified = true;
    self.da += 1;
  }

  pub fn approve(&mut self) {
    if !self.mst_a {
      self.modified = true;
      self.mst_a = true;
    }
  }

  pub fn store(
    &self,
    records: &mut HashMap<u64, Transaction>,
    hashes: &mut HashMap<String, u64>,
  ) {
    records.insert(self.id_tx(), self.clone());
    hashes.insert(self.hash().to_owned(), self.id_tx());
  }
}
