use super::Transaction;
use super::super::Result;
use mysql;

const QUERY: &str = r#"
  INSERT INTO tx (
    hash,
    id_tx,
    id_trunk,
    id_branch,
    id_address,
    id_bundle,
    tag,
    value,
    timestamp,
    current_idx,
    last_idx,
    da,
    height,
    is_mst,
    mst_a,
    solid
  ) VALUES (
    :hash,
    :id_tx,
    :id_trunk,
    :id_branch,
    :id_address,
    :id_bundle,
    :tag,
    :value,
    :timestamp,
    :current_idx,
    :last_idx,
    :da,
    :height,
    :is_mst,
    :mst_a,
    :solid
  )
"#;

impl Transaction {
  pub fn insert(&mut self, conn: &mut mysql::Conn) -> Result<()> {
    let mut params = self.to_params();
    params.push(("hash".to_owned(), mysql::Value::from(self.hash())));
    conn.prep_exec(QUERY, params)?;
    self.persistent = true;
    self.modified = false;
    Ok(())
  }
}
