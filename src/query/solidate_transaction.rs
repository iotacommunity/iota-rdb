use super::Result;
use mysql;

const TRUNK_QUERY: &str = r#"
  UPDATE tx SET height = :height, solid = :solid WHERE id_tx = :id_tx
"#;

const BRANCH_QUERY: &str = r#"
  UPDATE tx SET solid = :solid WHERE id_tx = :id_tx
"#;

pub fn solidate_transaction_trunk(
  conn: &mut mysql::Conn,
  id: u64,
  height: i32,
  solid: u8,
) -> Result<mysql::QueryResult> {
  Ok(conn.prep_exec(
    TRUNK_QUERY,
    params!{
      "id_tx" => id,
      "height" => height,
      "solid" => solid,
    },
  )?)
}

pub fn solidate_transaction_branch(
  conn: &mut mysql::Conn,
  id: u64,
  solid: u8,
) -> Result<mysql::QueryResult> {
  Ok(conn.prep_exec(
    BRANCH_QUERY,
    params!{
      "id_tx" => id,
      "solid" => solid,
    },
  )?)
}
