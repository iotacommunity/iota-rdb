use super::super::{Error, Record, Result};
use mysql;

#[derive(Clone)]
pub struct BundleRecord {
  generation: usize,
  persisted: bool,
  modified: bool,
  bundle: String,
  id_bundle: u64,
  is_mst: bool,
}

const SELECT_QUERY: &str = r#"
  SELECT
    bundle,
    id_bundle,
    is_mst
  FROM bundle
"#;

impl Record for BundleRecord {
  impl_record!();

  const SELECT_QUERY: &'static str = SELECT_QUERY;
  const SELECT_WHERE_ID: &'static str = r"WHERE id_bundle = ?";
  const SELECT_WHERE_HASH: &'static str = r"WHERE bundle = ?";

  const INSERT_QUERY: &'static str = r#"
    INSERT INTO bundle (
      bundle,
      id_bundle,
      is_mst
    ) VALUES (
      :bundle,
      :id_bundle,
      :is_mst
    )
  "#;

  const UPDATE_QUERY: &'static str = r#"
    UPDATE bundle SET
      is_mst = :is_mst
    WHERE id_bundle = :id_bundle
  "#;

  fn from_row(row: &mut mysql::Row) -> Result<Self> {
    Ok(Self {
      generation: 0,
      persisted: true,
      modified: false,
      bundle: row.take_opt("bundle").ok_or(Error::ColumnNotFound)??,
      id_bundle: row.take_opt("id_bundle").ok_or(Error::ColumnNotFound)??,
      is_mst: row.take_opt("is_mst").unwrap_or_else(|| Ok(false))?,
    })
  }

  fn to_params(&self) -> Vec<(String, mysql::Value)> {
    params!{
      "bundle" => self.bundle.clone(),
      "id_bundle" => self.id_bundle,
      "is_mst" => self.is_mst,
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
  impl_getter!(bundle, &str);
  impl_getter!(id_bundle, u64);
  impl_accessors!(is_mst, set_is_mst, bool);

  pub fn new(id_bundle: u64, bundle: String) -> Self {
    Self {
      generation: 0,
      persisted: false,
      modified: true,
      bundle,
      id_bundle,
      is_mst: false,
    }
  }
}
