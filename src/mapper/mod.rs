mod error;
mod transaction;

pub use self::error::{Error, Result};
pub use self::transaction::Transaction;
use counters::Counters;
use mysql;

pub struct Mapper<'a> {
  select_transactions_by_hash: mysql::Stmt<'a>,
  select_transactions_by_id: mysql::Stmt<'a>,
  insert_transaction: mysql::Stmt<'a>,
  insert_transaction_placeholder: mysql::Stmt<'a>,
  update_transaction: mysql::Stmt<'a>,
  approve_transaction: mysql::Stmt<'a>,
  direct_approve_transaction: mysql::Stmt<'a>,
  select_addresses: mysql::Stmt<'a>,
  insert_address: mysql::Stmt<'a>,
  select_bundles: mysql::Stmt<'a>,
  insert_bundle: mysql::Stmt<'a>,
  update_bundle: mysql::Stmt<'a>,
}

impl<'a> Mapper<'a> {
  pub fn new(pool: &mysql::Pool) -> Result<Self> {
    Ok(Self {
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
            id_tx, hash, id_trunk, id_branch, id_address, id_bundle, tag, value,
            timestamp, current_idx, last_idx, is_mst, mst_a
          ) VALUES (
            :id_tx, :hash, :id_trunk, :id_branch, :id_address, :id_bundle, :tag,
            :value, :timestamp, :current_idx, :last_idx, :is_mst, :mst_a
          )
        "#,
      )?,
      insert_transaction_placeholder: pool.prepare(
        r#"
          INSERT INTO tx (id_tx, hash, da) VALUES (:id_tx, :hash, 1)
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
      direct_approve_transaction: pool.prepare(
        r#"
          UPDATE tx SET da = da + 1 WHERE id_tx = :id_tx
        "#,
      )?,
      select_addresses: pool.prepare(
        r#"
          SELECT id_address FROM address WHERE address = :address
        "#,
      )?,
      insert_address: pool.prepare(
        r#"
          INSERT INTO address (
            id_address, address
          ) VALUES (
            :id_address, :address
          )
        "#,
      )?,
      select_bundles: pool.prepare(
        r#"
          SELECT id_bundle FROM bundle WHERE bundle = :bundle
        "#,
      )?,
      insert_bundle: pool.prepare(
        r#"
          INSERT INTO bundle (
            id_bundle, bundle, created, size
          ) VALUES (
            :id_bundle, :bundle, :created, :size
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
    counters: &Counters,
    transaction: Transaction,
  ) -> Result<mysql::QueryResult> {
    let id_tx = counters.next_transaction();
    let mut params = transaction.to_params();
    params.push(("id_tx".to_owned(), mysql::Value::from(id_tx)));
    Ok(self.insert_transaction.execute(params)?)
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

  pub fn fetch_transaction(
    &mut self,
    counters: &Counters,
    hash: &str,
  ) -> Result<u64> {
    match self.select_transactions_by_hash.first_exec(params!{
      "hash" => hash,
    })? {
      Some(mut result) => {
        let id_tx = result.take_opt("id_tx").ok_or(Error::ColumnNotFound)??;
        self.direct_approve_transaction.execute(params!{
          "id_tx" => id_tx,
        })?;
        Ok(id_tx)
      }
      None => {
        let id_tx = counters.next_transaction();
        self.insert_transaction_placeholder.execute(params!{
          "id_tx" => id_tx,
          "hash" => hash,
        })?;
        Ok(id_tx)
      }
    }
  }

  pub fn fetch_address(
    &mut self,
    counters: &Counters,
    address: &str,
  ) -> Result<u64> {
    match self.select_addresses.first_exec(params!{
      "address" => address,
    })? {
      Some(mut result) => Ok(result.take_opt("id_address").ok_or(
        Error::ColumnNotFound,
      )??),
      None => {
        let id_address = counters.next_address();
        self.insert_address.execute(params!{
          "id_address" => id_address,
          "address" => address,
        })?;
        Ok(id_address)
      }
    }
  }

  pub fn fetch_bundle(
    &mut self,
    counters: &Counters,
    bundle: &str,
    created: f64,
    size: i32,
  ) -> Result<u64> {
    match self.select_bundles.first_exec(params!{
      "bundle" => bundle,
    })? {
      Some(mut result) => Ok(result.take_opt("id_bundle").ok_or(
        Error::ColumnNotFound,
      )??),
      None => {
        let id_bundle = counters.next_bundle();
        self.insert_bundle.execute(params!{
          "id_bundle" => id_bundle,
          "bundle" => bundle,
          "created" => created,
          "size" => size,
        })?;
        Ok(id_bundle)
      }
    }
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
