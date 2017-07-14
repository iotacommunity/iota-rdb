mod error;
mod transaction;

pub use self::error::{Error, Result};
pub use self::transaction::Transaction;
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
  pub fn new(pool: &mysql::Pool) -> Result<Self> {
    Ok(Mapper {
      select_transactions_by_hash: pool.prepare(
        r#"
          SELECT id_tx, id_trunk, id_branch FROM tx WHERE hash = :hash
        "#,
      )?,
      select_transactions_by_id: pool.prepare(
        r#"
          SELECT
            id_tx, id_trunk, id_branch, id_bundle, current_idx, mst_a
          FROM tx
          WHERE id_tx = :id_tx
        "#,
      )?,
      insert_transaction: pool.prepare(
        r#"
          INSERT INTO tx (
            hash, id_trunk, id_branch, id_address, id_bundle, tag, value,
            timestamp, current_idx, last_idx, is_mst, mst_a
          ) VALUES (
            :hash, :id_trunk, :id_branch, :id_address, :id_bundle, :tag, :value,
            :timestamp, :current_idx, :last_idx, :is_mst, :mst_a
          )
        "#,
      )?,
      insert_transaction_placeholder: pool.prepare(
        r#"
          INSERT INTO tx (hash) VALUES (:hash)
        "#,
      )?,
      update_transaction: pool.prepare(
        r#"
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
          "#,
      )?,
      approve_transaction: pool.prepare(
        r#"
          UPDATE tx SET mst_a = :mst_a WHERE id_tx = :id_tx
        "#,
      )?,
      select_addresses: pool.prepare(
        r#"
          SELECT id_address FROM address WHERE address = :address
        "#,
      )?,
      insert_address: pool.prepare(
        r#"
          INSERT INTO address (address) VALUES (:address)
        "#,
      )?,
      insert_bundle: pool.prepare(
        r#"
          INSERT INTO bundle (
           bundle, created, size
          ) VALUES (
           :bundle, :created, :size
          )
        "#,
      )?,
      update_bundle: pool.prepare(
        r#"
          UPDATE bundle SET
            confirmed = :confirmed
          WHERE id_bundle = :id_bundle
        "#,
      )?,
    })
  }

  pub fn select_transaction_by_hash(
    &mut self,
    hash: &str,
  ) -> Result<Option<mysql::Row>> {
    Ok(self.select_transactions_by_hash.first_exec(params!{
      "hash" => hash
    })?)
  }

  pub fn select_transaction_by_id(
    &mut self,
    id_tx: u64,
  ) -> Result<Option<mysql::Row>> {
    Ok(self.select_transactions_by_id.first_exec(params!{
      "id_tx" => id_tx
    })?)
  }

  pub fn insert_transaction(
    &mut self,
    transaction: Transaction,
  ) -> Result<mysql::QueryResult> {
    Ok(self.insert_transaction.execute(transaction.to_params())?)
  }

  pub fn update_transaction(
    &mut self,
    transaction: Transaction,
  ) -> Result<mysql::QueryResult> {
    Ok(self.update_transaction.execute(transaction.to_params())?)
  }

  pub fn approve_transaction(&mut self, id: u64) -> Result<mysql::QueryResult> {
    Ok(self.approve_transaction.execute(params!{
      "id_tx" => id, "mst_a" => true
    })?)
  }

  pub fn insert_or_select_transaction(&mut self, hash: &str) -> Result<u64> {
    let insert_result = self.insert_transaction_placeholder.execute(params!{
      "hash" => hash,
    });
    match insert_result {
      Ok(result) => Ok(result.last_insert_id()),
      Err(_) => {
        Ok(self
          .select_transactions_by_hash
          .first_exec(params!{"hash" => hash})?
          .ok_or(Error::RecordNotFound)?
          .take_opt("id_tx")
          .ok_or(Error::ColumnNotFound)??)
      }
    }
  }

  pub fn insert_or_select_address(&mut self, address: &str) -> Result<u64> {
    let insert_result = self.insert_address.execute(params!{
      "address" => address,
    });
    match insert_result {
      Ok(result) => Ok(result.last_insert_id()),
      Err(_) => {
        Ok(self
          .select_addresses
          .first_exec(params!{"address" => address})?
          .ok_or(Error::RecordNotFound)?
          .take_opt("id_address")
          .ok_or(Error::ColumnNotFound)??)
      }
    }
  }

  pub fn insert_bundle(
    &mut self,
    bundle: &str,
    created: f64,
    size: i32,
  ) -> Result<u64> {
    Ok(
      self
        .insert_bundle
        .execute(params!{
        "bundle" => bundle,
        "created" => created,
        "size" => size,
      })?
        .last_insert_id(),
    )
  }

  pub fn update_bundle(
    &mut self,
    id: u64,
    confirmed: f64,
  ) -> Result<mysql::QueryResult> {
    Ok(self.update_bundle.execute(params!{
      "id_bundle" => id,
      "confirmed" => confirmed,
    })?)
  }
}
