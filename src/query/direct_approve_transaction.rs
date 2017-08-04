use super::Result;
use mysql;

const QUERY: &str = r#"
  UPDATE tx SET da = da + 1 WHERE id_tx = :id_tx
"#;

pub fn direct_approve_transaction(
  conn: &mut mysql::Conn,
  id: u64,
) -> Result<mysql::QueryResult> {
  Ok(conn.prep_exec(QUERY, params!{"id_tx" => id})?)
}
