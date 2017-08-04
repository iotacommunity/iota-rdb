use super::{Error, Result};
use counter::Counter;
use iota_curl_cpu::CpuCurl;
use iota_sign::{trits_checksum, CHECKSUM_LEN};
use iota_trytes::{char_to_trits, trits_to_string};
use mysql;

const SELECT_QUERY: &str = r#"
  SELECT id_address FROM address WHERE address = :address
"#;

const INSERT_QUERY: &str = r#"
  INSERT INTO address (
    id_address, address, checksum
  ) VALUES (
    :id_address, :address, :checksum
  )
"#;

pub fn fetch_address(
  conn: &mut mysql::Conn,
  counter: &Counter,
  address: &str,
) -> Result<u64> {
  match conn
    .first_exec(SELECT_QUERY, params!{"address" => address})?
  {
    Some(row) => {
      let (id_address,) = mysql::from_row_opt(row)?;
      Ok(id_address)
    }
    None => {
      let id_address = counter.next_address();
      conn.prep_exec(
        INSERT_QUERY,
        params!{
          "id_address" => id_address,
          "address" => address,
          "checksum" => calculate_checksum(address)?,
        },
      )?;
      Ok(id_address)
    }
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
