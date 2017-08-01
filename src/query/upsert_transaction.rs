use counters::Counters;
use mysql;
use query::Result;

pub struct UpsertTransactionRecord<'a> {
  pub hash: &'a str,
  pub id_trunk: u64,
  pub id_branch: u64,
  pub id_address: u64,
  pub id_bundle: u64,
  pub tag: &'a str,
  pub value: i64,
  pub timestamp: i64,
  pub current_idx: i32,
  pub last_idx: i32,
  pub height: i32,
  pub is_mst: bool,
  pub mst_a: bool,
  pub solid: u8,
}

const INSERT_QUERY: &str = r#"
  INSERT INTO tx (
    id_tx, hash, id_trunk, id_branch, id_address, id_bundle, tag, value,
    timestamp, current_idx, last_idx, height, is_mst, mst_a, solid
  ) VALUES (
    :id_tx, :hash, :id_trunk, :id_branch, :id_address, :id_bundle, :tag,
    :value, :timestamp, :current_idx, :last_idx, :height, :is_mst,
    :mst_a, :solid
  )
"#;

const UPDATE_QUERY: &str = r#"
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
    height = :height,
    is_mst = :is_mst,
    mst_a = :mst_a,
    solid = :solid
  WHERE hash = :hash
"#;

pub fn insert_transaction<'a>(
  conn: &'a mut mysql::Conn,
  counters: &Counters,
  transaction: &UpsertTransactionRecord,
) -> Result<mysql::QueryResult<'a>> {
  let id_tx = counters.next_transaction();
  let mut params = transaction.to_params();
  params.push(("id_tx".to_owned(), mysql::Value::from(id_tx)));
  Ok(conn.prep_exec(INSERT_QUERY, params)?)
}

pub fn update_transaction<'a>(
  conn: &'a mut mysql::Conn,
  transaction: &UpsertTransactionRecord,
) -> Result<mysql::QueryResult<'a>> {
  Ok(conn.prep_exec(UPDATE_QUERY, transaction.to_params())?)
}

impl<'a> UpsertTransactionRecord<'a> {
  pub fn to_params(&self) -> Vec<(String, mysql::Value)> {
    params!{
      "hash" => self.hash,
      "id_trunk" => self.id_trunk,
      "id_branch" => self.id_branch,
      "id_address" => self.id_address,
      "id_bundle" => self.id_bundle,
      "tag" => self.tag,
      "value" => self.value,
      "timestamp" => self.timestamp,
      "current_idx" => self.current_idx,
      "last_idx" => self.last_idx,
      "height" => self.height,
      "is_mst" => self.is_mst,
      "mst_a" => self.mst_a,
      "solid" => self.solid,
    }
  }
}
