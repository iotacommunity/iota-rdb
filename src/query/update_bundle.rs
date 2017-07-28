use mysql;
use query::Result;

const QUERY: &str = r#"
  UPDATE bundle SET
    confirmed = :confirmed
  WHERE id_bundle = :id_bundle
"#;

pub fn update_bundle(
  conn: &mut mysql::Conn,
  id: u64,
  confirmed: f64,
) -> Result<mysql::QueryResult> {
  Ok(conn.prep_exec(
    QUERY,
    params!{
      "id_bundle" => id,
      "confirmed" => confirmed,
    },
  )?)
}
