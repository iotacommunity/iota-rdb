use mysql;

pub struct Mapper<'a> {
  select_transactions_by_hash: mysql::Stmt<'a>,
  select_transactions_by_id: mysql::Stmt<'a>,
  insert_transaction: mysql::Stmt<'a>,
  insert_transaction_placeholder: mysql::Stmt<'a>,
  update_transaction: mysql::Stmt<'a>,
  approve_transaction: mysql::Stmt<'a>,
  select_addresses: mysql::Stmt<'a>,
  insert_address: mysql::Stmt<'a>,
  insert_bundle: mysql::Stmt<'a>,
  update_bundle: mysql::Stmt<'a>,
}

impl<'a> Mapper<'a> {
  pub fn prepare(pool: &mysql::Pool) -> Self {
    Mapper {
      select_transactions_by_hash: pool
        .prepare(
          "SELECT id_tx, id_trunk, id_branch FROM tx WHERE hash = :hash",
        )
        .unwrap(),
      select_transactions_by_id: pool
        .prepare(
          r"
            SELECT
              id_tx, id_trunk, id_branch, id_bundle, current_idx, mst_a
            FROM tx
            WHERE id_tx = :id_tx
          ",
        )
        .unwrap(),
      insert_transaction: pool
        .prepare(
          r"
            INSERT INTO tx (
              hash, id_trunk, id_branch, id_address, id_bundle, tag, value,
              timestamp, current_idx, last_idx, is_mst, mst_a
            ) VALUES (
              :hash, :id_trunk, :id_branch, :id_address, :id_bundle, :tag, :value,
              :timestamp, :current_idx, :last_idx, :is_mst, :mst_a
            )
          ",
        )
        .unwrap(),
      insert_transaction_placeholder: pool
        .prepare("INSERT INTO tx (hash) VALUES (:hash)")
        .unwrap(),
      update_transaction: pool
        .prepare(
          r"
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
              is_mst = :is_mst,
              mst_a = :mst_a
            WHERE hash = :hash
          ",
        )
        .unwrap(),
      approve_transaction: pool
        .prepare("UPDATE tx SET mst_a = :mst_a WHERE id_tx = :id_tx")
        .unwrap(),
      select_addresses: pool
        .prepare("SELECT id_address FROM address WHERE address = :address")
        .unwrap(),
      insert_address: pool
        .prepare("INSERT INTO address (address) VALUES (:address)")
        .unwrap(),
      insert_bundle: pool
        .prepare(
          r"
            INSERT INTO bundle (
             bundle, created, size
            ) VALUES (
             :bundle, :created, :size
            )
         ",
        )
        .unwrap(),
      update_bundle: pool
        .prepare(
          r"
            UPDATE bundle SET
              confirmed = :confirmed
            WHERE id_bundle = :id_bundle
          ",
        )
        .unwrap(),
    }
  }

  pub fn select_transaction_by_hash(
    &mut self,
    hash: &str,
  ) -> Option<mysql::conn::Row> {
    self
      .select_transactions_by_hash
      .first_exec(params!{"hash" => hash})
      .unwrap()
  }

  pub fn select_transaction_by_id(
    &mut self,
    id_tx: u64,
  ) -> Option<mysql::conn::Row> {
    self
      .select_transactions_by_id
      .first_exec(params!{"id_tx" => id_tx})
      .unwrap()
  }

  pub fn save_transaction(
    &mut self,
    insert: bool,
    hash: &str,
    id_trunk: u64,
    id_branch: u64,
    id_address: u64,
    id_bundle: u64,
    tag: &str,
    value: i64,
    timestamp: i64,
    current_index: i32,
    last_index: i32,
    is_milestone: bool,
    milestone_approved: bool,
  ) {
    let statement = if insert {
      &mut self.insert_transaction
    } else {
      &mut self.update_transaction
    };
    statement
      .execute(params!{
        "hash" => hash,
        "id_trunk" => id_trunk,
        "id_branch" => id_branch,
        "id_address" => id_address,
        "id_bundle" => id_bundle,
        "tag" => tag,
        "value" => value,
        "timestamp" => timestamp,
        "current_idx" => current_index,
        "last_idx" => last_index,
        "is_mst" => is_milestone,
        "mst_a" => milestone_approved,
      })
      .unwrap();
  }

  pub fn approve_transaction(&mut self, id: u64) {
    self
      .approve_transaction
      .execute(params!{"id_tx" => id, "mst_a" => true})
      .unwrap();
  }

  pub fn insert_or_select_transaction(&mut self, hash: &str) -> u64 {
    let insert_result = self.insert_transaction_placeholder.execute(params!{
      "hash" => hash,
    });
    match insert_result {
      Ok(result) => result.last_insert_id(),
      Err(_) => {
        self
          .select_transactions_by_hash
          .first_exec(params!{"hash" => hash})
          .unwrap()
          .unwrap()
          .take_opt("id_tx")
          .unwrap()
          .unwrap()
      }
    }
  }

  pub fn insert_or_select_address(&mut self, address: &str) -> u64 {
    let insert_result = self.insert_address.execute(params!{
      "address" => address,
    });
    match insert_result {
      Ok(result) => result.last_insert_id(),
      Err(_) => {
        self
          .select_addresses
          .first_exec(params!{"address" => address})
          .unwrap()
          .unwrap()
          .take_opt("id_address")
          .unwrap()
          .unwrap()
      }
    }
  }

  pub fn insert_bundle(
    &mut self,
    bundle: &str,
    created: f64,
    size: i32,
  ) -> u64 {
    self
      .insert_bundle
      .execute(params!{
        "bundle" => bundle,
        "created" => created,
        "size" => size,
      })
      .unwrap()
      .last_insert_id()
  }

  pub fn update_bundle(&mut self, id: u64, confirmed: f64) {
    self
      .update_bundle
      .execute(params!{
        "id_bundle" => id,
        "confirmed" => confirmed,
      })
      .unwrap();
  }
}
