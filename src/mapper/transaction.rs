use super::Result;
use counter::Counter;
use mysql;
use query;
use std::sync::Arc;

pub struct Transaction {
  counter: Arc<Counter>,
}

impl Transaction {
  pub fn new(counter: Arc<Counter>) -> Self {
    Self { counter }
  }

  pub fn find(
    &self,
    conn: &mut mysql::Conn,
    current_hash: &str,
    trunk_hash: &str,
    branch_hash: &str,
  ) -> Result<
    (
      Option<query::FindTransactionsResult>,
      Option<query::FindTransactionsResult>,
      Option<query::FindTransactionsResult>,
    ),
  > {
    Ok(query::find_transactions(
      conn,
      current_hash,
      trunk_hash,
      branch_hash,
    )?)
  }

  pub fn placeholder(
    &self,
    conn: &mut mysql::Conn,
    hash: &str,
    height: i32,
    solid: u8,
  ) -> Result<u64> {
    Ok(query::insert_transaction_placeholder(
      conn,
      &self.counter,
      hash,
      height,
      solid,
    )?)
  }

  pub fn upsert(
    &self,
    conn: &mut mysql::Conn,
    current_tx: &Option<query::FindTransactionsResult>,
    record: query::UpsertTransactionRecord,
  ) -> Result<()> {
    if current_tx.is_none() {
      query::insert_transaction(conn, &self.counter, &record)?;
      Ok(())
    } else {
      query::update_transaction(conn, &record)?;
      Ok(())
    }
  }

  pub fn direct_approve(&self, conn: &mut mysql::Conn, id: u64) -> Result<()> {
    query::direct_approve_transaction(conn, id)?;
    Ok(())
  }
}
