use super::super::{Error, Record, Result};
use mysql;

#[derive(Clone)]
pub struct BundleRecord {
  persisted: bool,
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

impl Record for BundleRecord {
  define_record!();

  const SELECT_QUERY: &'static str = SELECT_QUERY;
  const SELECT_WHERE_ID: &'static str = r"WHERE id_bundle = ?";
  const SELECT_WHERE_HASH: &'static str = r"WHERE bundle = ?";

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
      persisted: true,
      modified: false,
      bundle: row.take_opt("bundle").ok_or(Error::ColumnNotFound)??,
      id_bundle: row.take_opt("id_bundle").ok_or(Error::ColumnNotFound)??,
      created: row.take_opt("created").unwrap_or_else(|| Ok(0.0))?,
      size: row.take_opt("size").unwrap_or_else(|| Ok(0))?,
      confirmed: row.take_opt("confirmed").unwrap_or_else(|| Ok(0.0))?,
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

  fn id(&self) -> u64 {
    self.id_bundle
  }

  fn hash(&self) -> &str {
    &self.bundle
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
      persisted: false,
      modified: true,
      bundle,
      id_bundle,
      created,
      size,
      confirmed: 0.0,
    }
  }
}
