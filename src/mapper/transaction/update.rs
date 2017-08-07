use super::Transaction;
use super::super::Result;
use mysql;

const QUERY: &str = r#"
  UPDATE tx SET
    id_trunk = :id_trunk,
    id_branch = :id_branch,
    id_address = :id_address,
    id_bundle = :id_bundle,
    tag = :tag,
    value = :value,
    timestamp = :timestamp,
    current_idx = :current_idx,
    last_idx = :last_idx,
    da = :da,
    height = :height,
    is_mst = :is_mst,
    mst_a = :mst_a,
    solid = :solid
  WHERE id_tx = :id_tx
"#;

impl Transaction {
  pub fn update(&mut self, conn: &mut mysql::Conn) -> Result<()> {
    conn.prep_exec(QUERY, self.params())?;
    self.modified = false;
    Ok(())
  }
}
