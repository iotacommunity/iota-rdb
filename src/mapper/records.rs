use mysql;

pub struct NewTransaction<'a> {
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

pub struct ReferencedTransaction {
  pub id_tx: mysql::Result<u64>,
  pub id_trunk: mysql::Result<u64>,
  pub id_branch: mysql::Result<u64>,
  pub height: mysql::Result<i32>,
  pub solid: mysql::Result<u8>,
}

pub struct TransactionById {
  pub mst_a: mysql::Result<bool>,
  pub id_trunk: mysql::Result<u64>,
  pub id_branch: mysql::Result<u64>,
  pub id_bundle: mysql::Result<u64>,
  pub current_idx: mysql::Result<i32>,
}

impl<'a> NewTransaction<'a> {
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
      "solid" => self.solid,
    }
  }
}
