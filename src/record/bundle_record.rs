use super::{Error, Record, Result};
use mysql;
use std::collections::HashMap;

#[derive(Clone)]
pub struct BundleRecord {
  locked: bool,
  persistent: bool,
  modified: bool,
  bundle: String,
  id_bundle: u64,
  created: f64,
  size: i32,
  confirmed: f64,
}

const SELECT_QUERY: &str = r#"
  SELECT
    bundle,
    id_bundle,
    created,
    size,
    confirmed
  FROM bundle
"#;

const WHERE_BUNDLE: &str = r"WHERE bundle = ?";

impl Record for BundleRecord {
  define_record!();

  const SELECT_QUERY: &'static str = SELECT_QUERY;
  const SELECT_WHERE_ID: &'static str = r"WHERE id_bundle = ?";

  const INSERT_QUERY: &'static str = r#"
    INSERT INTO bundle (
      bundle,
      id_bundle,
      created,
      size,
      confirmed
    ) VALUES (
      :bundle,
      :id_bundle,
      :created,
      :size,
      :confirmed
    )
  "#;

  const UPDATE_QUERY: &'static str = r#"
    UPDATE bundle SET
      created = :created,
      size = :size,
      confirmed = :confirmed
    WHERE id_bundle = :id_bundle
  "#;

  fn from_row(row: &mut mysql::Row) -> Result<Self> {
    Ok(Self {
      locked: false,
      persistent: true,
      modified: false,
      bundle: row.take_opt("bundle").ok_or(Error::ColumnNotFound)??,
      id_bundle: row.take_opt("id_bundle").ok_or(Error::ColumnNotFound)??,
      created: Self::take_column(row, "created", 0.0)?,
      size: Self::take_column(row, "size", 0)?,
      confirmed: Self::take_column(row, "confirmed", 0.0)?,
    })
  }

  fn to_params(&self) -> Vec<(String, mysql::Value)> {
    params!{
      "bundle" => self.bundle.clone(),
      "id_bundle" => self.id_bundle,
      "created" => self.created,
      "size" => self.size,
      "confirmed" => self.confirmed,
    }
  }
}

impl BundleRecord {
  define_getter!(bundle, &str);
  define_getter!(id_bundle, u64);
  define_accessors!(created, set_created, f64);
  define_accessors!(size, set_size, i32);
  define_accessors!(confirmed, set_confirmed, f64);

  pub fn new(id_bundle: u64, bundle: String, size: i32, created: f64) -> Self {
    Self {
      locked: false,
      persistent: false,
      modified: true,
      bundle,
      id_bundle,
      created,
      size,
      confirmed: 0.0,
    }
  }

  pub fn find_by_bundle(
    conn: &mut mysql::Conn,
    hash: &str,
  ) -> Result<Option<BundleRecord>> {
    match conn
      .first_exec(format!("{} {}", SELECT_QUERY, WHERE_BUNDLE), (hash,))?
    {
      Some(ref mut row) => Ok(Some(Self::from_row(row)?)),
      None => Ok(None),
    }
  }

  pub fn store(
    self,
    records: &mut HashMap<u64, BundleRecord>,
    hashes: &mut HashMap<String, u64>,
  ) {
    hashes.insert(self.bundle().to_owned(), self.id_bundle());
    records.insert(self.id_bundle(), self);
  }
}
