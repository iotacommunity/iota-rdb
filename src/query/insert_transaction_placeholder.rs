use super::Result;
use counter::Counter;
use mysql;

const QUERY: &str = r#"
  INSERT INTO tx (
    id_tx, hash, da, height, solid
  ) VALUES (
    :id_tx, :hash, 1, :height, :solid
  )
"#;

pub fn insert_transaction_placeholder(
  conn: &mut mysql::Conn,
  counter: &Counter,
  hash: &str,
  height: i32,
  solid: u8,
) -> Result<u64> {
  let id_tx = counter.next_transaction();
  conn.prep_exec(
    QUERY,
    params!{
      "id_tx" => id_tx,
      "hash" => hash,
      "height" => height,
      "solid" => solid,
    },
  )?;
  Ok(id_tx)
}
