use super::super::{Error, Record, Result};
use mysql;
use utils;

#[derive(Clone)]
pub struct AddressRecord {
  generation: usize,
  persisted: bool,
  modified: bool,
  address: String,
  id_address: u64,
  checksum: String,
}

const SELECT_QUERY: &str = r#"
  SELECT
    address,
    id_address,
    checksum
  FROM address
"#;

impl Record for AddressRecord {
  impl_record!();

  const SELECT_QUERY: &'static str = SELECT_QUERY;
  const SELECT_WHERE_ID: &'static str = r"WHERE id_address = ?";
  const SELECT_WHERE_HASH: &'static str = r"WHERE address = ?";

  const INSERT_QUERY: &'static str = r#"
    INSERT INTO address (
      address,
      id_address,
      checksum
    ) VALUES (
      :address,
      :id_address,
      :checksum
    )
  "#;

  const UPDATE_QUERY: &'static str = r#"
    UPDATE address SET
      checksum = :checksum
    WHERE id_address = :id_address
  "#;

  fn from_row(row: &mut mysql::Row) -> Result<Self> {
    Ok(Self {
      generation: 0,
      persisted: true,
      modified: false,
      address: row.take_opt("address").ok_or(Error::ColumnNotFound)??,
      id_address: row.take_opt("id_address").ok_or(Error::ColumnNotFound)??,
      checksum: row
        .take_opt("checksum")
        .unwrap_or_else(|| Ok(String::from("")))?,
    })
  }

  fn to_params(&self) -> Vec<(String, mysql::Value)> {
    params!{
      "address" => self.address.clone(),
      "id_address" => self.id_address,
      "checksum" => self.checksum.clone(),
    }
  }

  fn id(&self) -> u64 {
    self.id_address
  }

  fn hash(&self) -> &str {
    &self.address
  }
}

impl AddressRecord {
  impl_getter!(address, &str);
  impl_getter!(id_address, u64);
  impl_getter!(checksum, &str);
  impl_setter!(checksum, set_checksum, String);

  pub fn new(id_address: u64, address: String) -> Result<Self> {
    let checksum =
      utils::trits_checksum(&address).ok_or(Error::AddressChecksumToTrits)?;
    Ok(Self {
      generation: 0,
      persisted: false,
      modified: true,
      address,
      id_address,
      checksum,
    })
  }
}
