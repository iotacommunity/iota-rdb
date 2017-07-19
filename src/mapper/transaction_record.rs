use mysql;

pub struct TransactionRecord<'a> {
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
  pub is_mst: bool,
  pub mst_a: bool,
}

impl<'a> TransactionRecord<'a> {
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
      "is_mst" => self.is_mst,
      "mst_a" => self.mst_a,
    }
  }
}
