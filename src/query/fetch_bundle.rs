use counters::Counters;
use mysql;
use query::Result;

const SELECT_QUERY: &str = r#"
  SELECT id_bundle FROM bundle WHERE bundle = :bundle FOR UPDATE
"#;

const INSERT_QUERY: &str = r#"
  INSERT INTO bundle (
    id_bundle, bundle, created, size
  ) VALUES (
    :id_bundle, :bundle, :created, :size
  )
"#;

pub fn fetch_bundle(
  conn: &mut mysql::Transaction,
  counters: &Counters,
  created: f64,
  bundle: &str,
  size: i32,
) -> Result<u64> {
  match conn.first_exec(SELECT_QUERY, params!{"bundle" => bundle})? {
    Some(row) => {
      let (id_bundle,) = mysql::from_row_opt(row)?;
      Ok(id_bundle)
    }
    None => {
      let id_bundle = counters.next_bundle();
      conn.prep_exec(
        INSERT_QUERY,
        params!{
          "id_bundle" => id_bundle,
          "bundle" => bundle,
          "created" => created,
          "size" => size,
        },
      )?;
      Ok(id_bundle)
    }
  }
}
