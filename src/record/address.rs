use super::{Error, Record, Result};
use iota_curl_cpu::CpuCurl;
use iota_sign::{trits_checksum, CHECKSUM_LEN};
use iota_trytes::{char_to_trits, trits_to_string};
use mysql;
use std::collections::HashMap;

#[derive(Clone)]
pub struct Address {
  locked: bool,
  persistent: bool,
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

const WHERE_ADDRESS: &str = r"WHERE address = ?";

impl Record for Address {
  define_record!();

  const SELECT_QUERY: &'static str = SELECT_QUERY;
  const SELECT_WHERE_ID: &'static str = r"WHERE id_address = ?";

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
      persistent: true,
      modified: false,
      address: row.take_opt("address").ok_or(Error::ColumnNotFound)??,
      id_address: row.take_opt("id_address").ok_or(Error::ColumnNotFound)??,
      // TODO optional
      checksum: row.take_opt("checksum").ok_or(Error::ColumnNotFound)??,
    })
  }

  fn to_params(&self) -> Vec<(String, mysql::Value)> {
    params!{
      "address" => self.address.clone(),
      "id_address" => self.id_address,
      "checksum" => self.checksum.clone(),
    }
  }
}

impl Address {
  define_getter!(address, &str);
  define_getter!(id_address, u64);
  define_getter!(checksum, &str);
  define_setter!(checksum, set_checksum, String);

  pub fn new(id_address: u64, address: String) -> Result<Self> {
    let checksum = calculate_checksum(&address)?;
    Ok(Self {
      locked: false,
      persistent: false,
      modified: true,
      address,
      id_address,
      checksum,
    })
  }

  pub fn find_by_address(
    conn: &mut mysql::Conn,
    hash: &str,
  ) -> Result<Option<Address>> {
    match conn
      .first_exec(format!("{} {}", SELECT_QUERY, WHERE_ADDRESS), (hash,))?
    {
      Some(ref mut row) => Ok(Some(Self::from_row(row)?)),
      None => Ok(None),
    }
  }

  pub fn store(
    self,
    records: &mut HashMap<u64, Address>,
    hashes: &mut HashMap<String, u64>,
  ) {
    hashes.insert(self.address().to_owned(), self.id_address());
    records.insert(self.id_address(), self);
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
