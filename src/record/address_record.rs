use super::{Error, Record, Result};
use iota_curl_cpu::CpuCurl;
use iota_sign::{trits_checksum, CHECKSUM_LEN};
use iota_trytes::{char_to_trits, trits_to_string};
use mysql;

#[derive(Clone)]
pub struct AddressRecord {
  locked: bool,
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
  define_record!();

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
      locked: false,
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
  define_getter!(address, &str);
  define_getter!(id_address, u64);
  define_getter!(checksum, &str);
  define_setter!(checksum, set_checksum, String);

  pub fn new(id_address: u64, address: String) -> Result<Self> {
    let checksum = calculate_checksum(&address)?;
    Ok(Self {
      locked: false,
      persisted: false,
      modified: true,
      address,
      id_address,
      checksum,
    })
  }
}

fn calculate_checksum(address: &str) -> Result<String> {
  let (mut checksum, mut curl) = ([0; CHECKSUM_LEN], CpuCurl::default());
  let trits: Vec<_> =
    address.chars().flat_map(char_to_trits).cloned().collect();
  trits_checksum(&trits, &mut checksum, &mut curl);
  Ok(trits_to_string(&checksum)
    .ok_or(Error::AddressChecksumToTrits)?)
}
