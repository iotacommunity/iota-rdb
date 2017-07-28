use counters::Counters;
use mysql;
use query::Result;

const SELECT_QUERY: &str = r#"
  SELECT id_address FROM address WHERE address = :address FOR UPDATE
"#;

const INSERT_QUERY: &str = r#"
  INSERT INTO address (
    id_address, address
  ) VALUES (
    :id_address, :address
  )
"#;

pub fn fetch_address(
  conn: &mut mysql::Transaction,
  counters: &Counters,
  address: &str,
) -> Result<u64> {
  match conn
    .first_exec(SELECT_QUERY, params!{"address" => address})? {
    Some(row) => {
      let (id_address,) = mysql::from_row_opt(row)?;
      Ok(id_address)
    }
    None => {
      let id_address = counters.next_address();
      conn.prep_exec(
        INSERT_QUERY,
        params!{
          "id_address" => id_address,
          "address" => address,
        },
      )?;
      Ok(id_address)
    }
  }
}
