use super::super::{Error, Index, Record, Result};
use mysql;
use solid::Solid;

#[derive(Clone)]
pub struct TransactionRecord {
  generation: usize,
  persisted: bool,
  modified: bool,
  hash: String,
  id_tx: u64,
  id_trunk: Option<u64>,
  id_branch: Option<u64>,
  id_address: Option<u64>,
  id_bundle: Option<u64>,
  tag: String,
  value: i64,
  timestamp: f64,
  arrival: f64,
  conftime: f64,
  current_idx: i32,
  last_idx: i32,
  da: i32,
  height: i32,
  weight: f64,
  is_mst: bool,
  mst_a: bool,
  solid: Solid,
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
    arrival,
    conftime,
    current_idx,
    last_idx,
    da,
    height,
    weight,
    is_mst,
    mst_a,
    solid
  FROM tx
"#;

const WHERE_HASH_ONE: &str = r"WHERE hash = ?";
const WHERE_HASH_TWO: &str = r"WHERE hash IN (?, ?)";
const WHERE_HASH_THREE: &str = r"WHERE hash IN (?, ?, ?)";
const WHERE_ID_ONE: &str = r"WHERE id_tx = ?";
const WHERE_ID_TWO: &str = r"WHERE id_tx = (?, ?)";
const WHERE_ID_FOUR: &str = r"WHERE id_tx = (?, ?, ?, ?)";
const WHERE_ID_EIGHT: &str = r"WHERE id_tx = (?, ?, ?, ?, ?, ?, ?, ?)";
const WHERE_ID_TRUNK: &str = r"WHERE id_trunk = ?";
const WHERE_ID_BRANCH: &str = r"WHERE id_branch = ?";
const WHERE_ID_BUNDLE: &str = r"WHERE id_bundle = ?";

impl Record for TransactionRecord {
  impl_record!();

  const SELECT_QUERY: &'static str = SELECT_QUERY;
  const SELECT_WHERE_ID: &'static str = r"WHERE id_tx = ?";
  const SELECT_WHERE_HASH: &'static str = WHERE_HASH_ONE;

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
      arrival,
      conftime,
      current_idx,
      last_idx,
      da,
      height,
      weight,
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
      :arrival,
      :conftime,
      :current_idx,
      :last_idx,
      :da,
      :height,
      :weight,
      :is_mst,
      :mst_a,
      :solid
    )
  "#;

  const UPDATE_QUERY: &'static str = r#"
    UPDATE tx SET
      id_address = :id_address,
      id_bundle = :id_bundle,
      tag = :tag,
      value = :value,
      timestamp = :timestamp,
      arrival = :arrival,
      conftime = :conftime,
      current_idx = :current_idx,
      last_idx = :last_idx,
      da = :da,
      height = :height,
      weight = :weight,
      is_mst = :is_mst,
      mst_a = :mst_a,
      solid = :solid
    WHERE id_tx = :id_tx
  "#;

  fn from_row(row: &mut mysql::Row) -> Result<Self> {
    Ok(Self {
      generation: 0,
      persisted: true,
      modified: false,
      hash: row.take_opt("hash").ok_or(Error::ColumnNotFound)??,
      id_tx: row.take_opt("id_tx").ok_or(Error::ColumnNotFound)??,
      id_trunk: row.take_opt("id_trunk").unwrap_or_else(|| Ok(None))?,
      id_branch: row.take_opt("id_branch").unwrap_or_else(|| Ok(None))?,
      id_address: row.take_opt("id_address").unwrap_or_else(|| Ok(None))?,
      id_bundle: row.take_opt("id_bundle").unwrap_or_else(|| Ok(None))?,
      tag: row.take_opt("tag").unwrap_or_else(|| Ok(String::from("")))?,
      value: row.take_opt("value").unwrap_or_else(|| Ok(0))?,
      timestamp: row.take_opt("timestamp").unwrap_or_else(|| Ok(0.0))?,
      arrival: row.take_opt("arrival").unwrap_or_else(|| Ok(0.0))?,
      conftime: row.take_opt("conftime").unwrap_or_else(|| Ok(0.0))?,
      current_idx: row.take_opt("current_idx").unwrap_or_else(|| Ok(0))?,
      last_idx: row.take_opt("last_idx").unwrap_or_else(|| Ok(0))?,
      da: row.take_opt("da").unwrap_or_else(|| Ok(0))?,
      height: row.take_opt("height").unwrap_or_else(|| Ok(0))?,
      weight: row.take_opt("weight").unwrap_or_else(|| Ok(1.0))?,
      is_mst: row.take_opt("is_mst").unwrap_or_else(|| Ok(false))?,
      mst_a: row.take_opt("mst_a").unwrap_or_else(|| Ok(false))?,
      solid: Solid::from_db(row.take_opt("solid").unwrap_or_else(|| Ok(0))?),
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
      "arrival" => self.arrival,
      "conftime" => self.conftime,
      "current_idx" => self.current_idx,
      "last_idx" => self.last_idx,
      "da" => self.da,
      "height" => self.height,
      "weight" => self.weight,
      "is_mst" => self.is_mst,
      "mst_a" => self.mst_a,
      "solid" => self.solid.into_db(),
    }
  }

  fn id(&self) -> u64 {
    self.id_tx
  }

  fn hash(&self) -> &str {
    &self.hash
  }
}

impl TransactionRecord {
  impl_getter!(id_tx, u64);
  impl_getter!(id_trunk, Option<u64>);
  impl_getter!(id_branch, Option<u64>);
  impl_getter!(id_address, Option<u64>);
  impl_getter!(id_bundle, Option<u64>);
  impl_getter!(tag, &str);
  impl_setter!(tag, set_tag, String);
  impl_accessors!(value, set_value, i64);
  impl_accessors!(timestamp, set_timestamp, f64);
  impl_accessors!(arrival, set_arrival, f64);
  impl_accessors!(conftime, set_conftime, f64);
  impl_accessors!(current_idx, set_current_idx, i32);
  impl_accessors!(last_idx, set_last_idx, i32);
  impl_accessors!(da, set_da, i32);
  impl_accessors!(height, set_height, i32);
  impl_accessors!(weight, set_weight, f64);
  impl_accessors!(is_mst, set_is_mst, bool);
  impl_accessors!(mst_a, set_mst_a, bool);
  impl_accessors!(solid, set_solid, Solid);

  pub fn placeholder(hash: String, id_tx: u64) -> Self {
    Self {
      generation: 0,
      persisted: false,
      modified: true,
      hash,
      id_tx,
      id_trunk: None,
      id_branch: None,
      id_address: None,
      id_bundle: None,
      tag: String::from(""),
      value: 0,
      timestamp: 0.0,
      arrival: 0.0,
      conftime: 0.0,
      current_idx: 0,
      last_idx: 0,
      da: 0,
      height: 0,
      weight: 1.0,
      is_mst: false,
      mst_a: false,
      solid: Solid::None,
    }
  }

  pub fn find_by_hashes(
    conn: &mut mysql::Conn,
    mut hashes: Vec<&str>,
  ) -> Result<Vec<TransactionRecord>> {
    hashes.sort_unstable();
    hashes.dedup();
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
        results.push(TransactionRecord::from_row(&mut row?)?);
      }
    }
    Ok(results)
  }

  pub fn find_by_ids(
    conn: &mut mysql::Conn,
    ids: &[u64],
  ) -> Result<Vec<TransactionRecord>> {
    fn collect(
      results: &mut Vec<TransactionRecord>,
      rows: &mut mysql::QueryResult,
    ) -> Result<()> {
      for row in rows {
        results.push(TransactionRecord::from_row(&mut row?)?);
      }
      Ok(())
    }
    let mut results = Vec::new();
    for ids in ids.chunks(8) {
      match ids.len() {
        8 => {
          collect(
            &mut results,
            &mut conn.prep_exec(
              format!("{} {}", SELECT_QUERY, WHERE_ID_EIGHT),
              (
                ids[0],
                ids[1],
                ids[2],
                ids[3],
                ids[4],
                ids[5],
                ids[6],
                ids[7],
              ),
            )?,
          )?;
        }
        _ => for ids in ids.chunks(4) {
          match ids.len() {
            4 => {
              collect(
                &mut results,
                &mut conn.prep_exec(
                  format!("{} {}", SELECT_QUERY, WHERE_ID_FOUR),
                  (ids[0], ids[1], ids[2], ids[3]),
                )?,
              )?;
            }
            _ => for ids in ids.chunks(2) {
              match ids.len() {
                2 => {
                  collect(
                    &mut results,
                    &mut conn.prep_exec(
                      format!("{} {}", SELECT_QUERY, WHERE_ID_TWO),
                      (ids[0], ids[1]),
                    )?,
                  )?;
                }
                1 => {
                  collect(
                    &mut results,
                    &mut conn.prep_exec(
                      format!("{} {}", SELECT_QUERY, WHERE_ID_ONE),
                      (ids[0],),
                    )?,
                  )?;
                }
                _ => unreachable!(),
              }
            },
          }
        },
      }
    }
    Ok(results)
  }

  pub fn find_trunk(
    conn: &mut mysql::Conn,
    id: u64,
  ) -> Result<Vec<TransactionRecord>> {
    let mut results = Vec::new();
    for row in
      conn.prep_exec(format!("{} {}", SELECT_QUERY, WHERE_ID_TRUNK), (id,))?
    {
      results.push(TransactionRecord::from_row(&mut row?)?);
    }
    Ok(results)
  }

  pub fn find_branch(
    conn: &mut mysql::Conn,
    id: u64,
  ) -> Result<Vec<TransactionRecord>> {
    let mut results = Vec::new();
    for row in
      conn.prep_exec(format!("{} {}", SELECT_QUERY, WHERE_ID_BRANCH), (id,))?
    {
      results.push(TransactionRecord::from_row(&mut row?)?);
    }
    Ok(results)
  }

  pub fn find_bundle(
    conn: &mut mysql::Conn,
    id: u64,
  ) -> Result<Vec<TransactionRecord>> {
    let mut results = Vec::new();
    for row in
      conn.prep_exec(format!("{} {}", SELECT_QUERY, WHERE_ID_BUNDLE), (id,))?
    {
      results.push(TransactionRecord::from_row(&mut row?)?);
    }
    Ok(results)
  }

  pub fn mst_timestamp(&self) -> f64 {
    self.timestamp + self.conftime
  }

  pub fn direct_approve(&mut self) {
    let da = self.da;
    self.set_da(da + 1);
  }

  pub fn add_weight(&mut self, value: f64) {
    let weight = self.weight;
    self.set_weight(weight + value);
  }

  pub fn set_id_trunk(&mut self, id_trunk: u64, trunk_index: &mut Index) {
    match self.id_trunk {
      Some(_) => panic!("`id_trunk` is immutable"),
      None => {
        self.fill_index(trunk_index);
        self.set_modified();
        self.id_trunk = Some(id_trunk);
      }
    }
  }

  pub fn set_id_branch(&mut self, id_branch: u64, branch_index: &mut Index) {
    match self.id_branch {
      Some(_) => panic!("`id_branch` is immutable"),
      None => {
        self.fill_index(branch_index);
        self.set_modified();
        self.id_branch = Some(id_branch);
      }
    }
  }

  pub fn set_id_address(&mut self, id_address: u64) {
    match self.id_address {
      Some(_) => panic!("`id_address` is immutable"),
      None => {
        self.set_modified();
        self.id_address = Some(id_address);
      }
    }
  }

  pub fn set_id_bundle(&mut self, id_bundle: u64, bundle_index: &mut Index) {
    match self.id_bundle {
      Some(_) => panic!("`id_bundle` is immutable"),
      None => {
        self.fill_index(bundle_index);
        self.set_modified();
        self.id_bundle = Some(id_bundle);
      }
    }
  }
}
