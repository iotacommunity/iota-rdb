use mysql;
use query::Result;

const QUERY: &str = r#"
  UPDATE tx SET mst_a = :mst_a WHERE id_tx = :id_tx
"#;

pub fn approve_transaction(
  conn: &mut mysql::Conn,
  id: u64,
) -> Result<mysql::QueryResult> {
  Ok(conn.prep_exec(
    QUERY,
    params!{
      "id_tx" => id,
      "mst_a" => true,
    },
  )?)
}
