use mysql;
use query::Result;

const QUERY: &str = r#"
  UPDATE tx SET da = da + 1 WHERE id_tx = :id_tx
"#;

pub fn direct_approve_transaction<'a>(
  conn: &'a mut mysql::Transaction,
  id: u64,
) -> Result<mysql::QueryResult<'a>> {
  Ok(conn.prep_exec(QUERY, params!{"id_tx" => id})?)
}
