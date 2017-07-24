mod error;
mod records;

pub use self::error::{Error, Result};
pub use self::records::{ChildTransaction, NewTransaction, TransactionByHash,
                        TransactionById};
use counters::Counters;
use mysql;

pub struct Mapper<'a> {
  select_transactions_by_hash: mysql::Stmt<'a>,
  select_transactions_by_id: mysql::Stmt<'a>,
  select_child_transactions: mysql::Stmt<'a>,
  insert_transaction: mysql::Stmt<'a>,
  insert_transaction_placeholder: mysql::Stmt<'a>,
  update_transaction: mysql::Stmt<'a>,
  approve_transaction: mysql::Stmt<'a>,
  direct_approve_transaction: mysql::Stmt<'a>,
  solidate_branch_transaction: mysql::Stmt<'a>,
  solidate_trunk_transaction: mysql::Stmt<'a>,
  select_addresses: mysql::Stmt<'a>,
  insert_address: mysql::Stmt<'a>,
  select_bundles: mysql::Stmt<'a>,
  insert_bundle: mysql::Stmt<'a>,
  update_bundle: mysql::Stmt<'a>,
  insert_event: mysql::Stmt<'a>,
}

impl<'a> Mapper<'a> {
  pub fn new(pool: &mysql::Pool) -> Result<Self> {
    Ok(Self {
      select_transactions_by_hash: pool.prepare(
        r#"
          SELECT id_tx, id_trunk, id_branch, solid FROM tx WHERE hash = :hash
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
      select_child_transactions: pool.prepare(
        r#"
          SELECT
            id_tx, id_trunk, id_branch, height, solid
          FROM tx
          WHERE id_trunk = :id_tx OR id_branch = :id_tx
        "#,
      )?,
      insert_transaction_placeholder: pool.prepare(
        r#"
          INSERT INTO tx (id_tx, hash, da) VALUES (:id_tx, :hash, 1)
        "#,
      )?,
      insert_transaction: pool.prepare(
        r#"
          INSERT INTO tx (
            id_tx, hash, id_trunk, id_branch, id_address, id_bundle, tag, value,
            timestamp, current_idx, last_idx, is_mst, mst_a, solid
          ) VALUES (
            :id_tx, :hash, :id_trunk, :id_branch, :id_address, :id_bundle, :tag,
            :value, :timestamp, :current_idx, :last_idx, :is_mst, :mst_a, :solid
          )
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
            mst_a = :mst_a,
            solid = :solid
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
      solidate_branch_transaction: pool.prepare(
        r#"
          UPDATE tx SET solid = :solid WHERE id_tx = :id_tx
        "#,
      )?,
      solidate_trunk_transaction: pool.prepare(
        r#"
          UPDATE tx SET height = :height, solid = :solid WHERE id_tx = :id_tx
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
      insert_event: pool.prepare(
        r#"
          INSERT INTO txload (
            event, count, timestamp
          ) VALUES (
            :event, :count, :timestamp
          )
        "#,
      )?,
    })
  }

  pub fn select_transaction_by_hash(
    &mut self,
    hash: &str,
  ) -> Result<Option<TransactionByHash>> {
    match self
      .select_transactions_by_hash
      .first_exec(params!{"hash" => hash})? {
      Some(mut row) => Ok(Some(TransactionByHash {
        id_tx: row.take_opt("id_tx").ok_or(Error::ColumnNotFound)?,
        id_trunk: row.take_opt("id_trunk").ok_or(Error::ColumnNotFound)?,
        id_branch: row.take_opt("id_branch").ok_or(Error::ColumnNotFound)?,
        solid: row.take_opt("solid").ok_or(Error::ColumnNotFound)?,
      })),
      None => Ok(None),
    }
  }

  pub fn select_transaction_by_id(
    &mut self,
    id_tx: u64,
  ) -> Result<TransactionById> {
    let mut row = self
      .select_transactions_by_id
      .first_exec(params!{"id_tx" => id_tx})?
      .ok_or(Error::RecordNotFound)?;
    Ok(TransactionById {
      mst_a: row.take_opt("mst_a").ok_or(Error::ColumnNotFound)?,
      id_trunk: row.take_opt("id_trunk").ok_or(Error::ColumnNotFound)?,
      id_branch: row.take_opt("id_branch").ok_or(Error::ColumnNotFound)?,
      id_bundle: row.take_opt("id_bundle").ok_or(Error::ColumnNotFound)?,
      current_idx: row.take_opt("current_idx").ok_or(Error::ColumnNotFound)?,
    })
  }

  pub fn select_child_transactions(
    &mut self,
    id_tx: u64,
  ) -> Result<Vec<ChildTransaction>> {
    let mut records = Vec::new();
    let results = self
      .select_child_transactions
      .execute(params!{"id_tx" => id_tx})?;
    for row in results {
      let mut row = row?;
      records.push(ChildTransaction {
        id_tx: row.take_opt("id_tx").ok_or(Error::ColumnNotFound)?,
        id_trunk: row.take_opt("id_trunk").ok_or(Error::ColumnNotFound)?,
        id_branch: row.take_opt("id_branch").ok_or(Error::ColumnNotFound)?,
        height: row.take_opt("height").ok_or(Error::ColumnNotFound)?,
        solid: row.take_opt("solid").ok_or(Error::ColumnNotFound)?,
      });
    }
    Ok(records)
  }

  pub fn insert_transaction(
    &mut self,
    counters: &Counters,
    transaction: NewTransaction,
  ) -> Result<mysql::QueryResult> {
    let id_tx = counters.next_transaction();
    let mut params = transaction.to_params();
    params.push(("id_tx".to_owned(), mysql::Value::from(id_tx)));
    Ok(self.insert_transaction.execute(params)?)
  }

  pub fn update_transaction(
    &mut self,
    transaction: NewTransaction,
  ) -> Result<mysql::QueryResult> {
    Ok(self.update_transaction.execute(transaction.to_params())?)
  }

  pub fn approve_transaction(&mut self, id: u64) -> Result<mysql::QueryResult> {
    Ok(self.approve_transaction.execute(params!{
      "id_tx" => id,
      "mst_a" => true,
    })?)
  }

  pub fn direct_approve_transaction(
    &mut self,
    id: u64,
  ) -> Result<mysql::QueryResult> {
    Ok(self
      .direct_approve_transaction
      .execute(params!{"id_tx" => id})?)
  }

  pub fn solidate_branch_transaction(
    &mut self,
    id: u64,
    solid: u8,
  ) -> Result<mysql::QueryResult> {
    Ok(self.solidate_branch_transaction.execute(params!{
      "id_tx" => id,
      "solid" => solid,
    })?)
  }

  pub fn solidate_trunk_transaction(
    &mut self,
    id: u64,
    height: i32,
    solid: u8,
  ) -> Result<mysql::QueryResult> {
    Ok(self.solidate_trunk_transaction.execute(params!{
      "id_tx" => id,
      "height" => height,
      "solid" => solid,
    })?)
  }

  pub fn insert_transaction_placeholder(
    &mut self,
    counters: &Counters,
    hash: &str,
  ) -> Result<u64> {
    let id_tx = counters.next_transaction();
    self.insert_transaction_placeholder.execute(params!{
      "id_tx" => id_tx,
      "hash" => hash,
    })?;
    Ok(id_tx)
  }

  pub fn fetch_address(
    &mut self,
    counters: &Counters,
    address: &str,
  ) -> Result<u64> {
    match self
      .select_addresses
      .first_exec(params!{"address" => address})? {
      Some(mut result) => Ok(
        result.take_opt("id_address").ok_or(Error::ColumnNotFound)??,
      ),
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
    created: f64,
    bundle: &str,
    size: i32,
  ) -> Result<u64> {
    match self.select_bundles.first_exec(params!{"bundle" => bundle})? {
      Some(mut result) => Ok(
        result.take_opt("id_bundle").ok_or(Error::ColumnNotFound)??,
      ),
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

  pub fn new_transaction_received_event(
    &mut self,
    timestamp: f64,
  ) -> Result<()> {
    self.insert_event.execute(params!{
      "event" => "NTX",
      "count" => 1,
      "timestamp" => timestamp,
    })?;
    Ok(())
  }

  pub fn milestone_received_event(&mut self, timestamp: f64) -> Result<()> {
    self.insert_event.execute(params!{
      "event" => "MST",
      "count" => 1,
      "timestamp" => timestamp,
    })?;
    Ok(())
  }

  pub fn subtanble_confirmation_event(
    &mut self,
    timestamp: f64,
    count: i32,
  ) -> Result<()> {
    self.insert_event.execute(params!{
      "event" => "CNF",
      "count" => count,
      "timestamp" => timestamp,
    })?;
    Ok(())
  }

  pub fn unsolid_transaction_event(&mut self, timestamp: f64) -> Result<()> {
    self.insert_event.execute(params!{
      "event" => "UNS",
      "count" => 1,
      "timestamp" => timestamp,
    })?;
    Ok(())
  }

  pub fn subtangle_solidation_event(
    &mut self,
    timestamp: f64,
    count: i32,
  ) -> Result<()> {
    self.insert_event.execute(params!{
      "event" => "SOL",
      "count" => count,
      "timestamp" => timestamp,
    })?;
    Ok(())
  }
}
