use super::Result;
use mapper::{AddressMapper, BundleMapper, TransactionMapper};
use mysql;
use query::{self, event};
use transaction::Transaction;
use utils;
use worker::{ApproveVec, SolidateVec};

pub struct Write {
  conn: mysql::Conn,
  transaction_mapper: TransactionMapper,
  address_mapper: AddressMapper,
  bundle_mapper: BundleMapper,
}

impl Write {
  pub fn new(
    mysql_uri: &str,
    transaction_mapper: TransactionMapper,
    address_mapper: AddressMapper,
    bundle_mapper: BundleMapper,
  ) -> Result<Self> {
    let conn = mysql::Conn::new(mysql_uri)?;
    Ok(Self {
      conn,
      transaction_mapper,
      address_mapper,
      bundle_mapper,
    })
  }

  pub fn perform(
    &mut self,
    transaction: &Transaction,
  ) -> Result<(Option<ApproveVec>, Option<SolidateVec>)> {
    let (mut approve_data, mut solidate_data) = (None, None);
    if let Some((current_tx, trunk_tx, branch_tx)) =
      self.transaction_mapper.fetch(
        &mut self.conn,
        transaction.hash(),
        transaction.trunk_hash(),
        transaction.branch_hash(),
      )? {
      let timestamp = utils::milliseconds_since_epoch()?;
      let id_address = self
        .address_mapper
        .fetch(&mut self.conn, transaction.address_hash())?;
      let id_bundle = self.bundle_mapper.fetch(
        &mut self.conn,
        timestamp,
        transaction.bundle_hash(),
        transaction.last_index(),
      )?;
      let mut solid = transaction.solid();
      let height = if solid != 0b11 && trunk_tx.solid() == 0b11 {
        trunk_tx.height() + 1
      } else {
        0
      };
      if trunk_tx.solid() == 0b11 {
        solid |= 0b10;
      }
      if branch_tx.solid() == 0b11 {
        solid |= 0b01;
      }
      let record = query::UpsertTransactionRecord {
        hash: transaction.hash(),
        tag: transaction.tag(),
        value: transaction.value(),
        timestamp: transaction.timestamp(),
        current_idx: transaction.current_index(),
        last_idx: transaction.last_index(),
        is_mst: transaction.is_milestone(),
        mst_a: transaction.is_milestone(),
        id_trunk: trunk_tx.id_tx(),
        id_branch: branch_tx.id_tx(),
        id_address,
        id_bundle,
        height,
        solid,
      };
      self
        .transaction_mapper
        .upsert(&mut self.conn, &current_tx, record)?;
      if solid != 0b11 {
        event::unsolid_transaction(&mut self.conn, timestamp)?;
      }
      event::new_transaction_received(&mut self.conn, timestamp)?;
      if transaction.is_milestone() {
        event::milestone_received(&mut self.conn, timestamp)?;
        approve_data = Some(vec![trunk_tx.id_tx(), branch_tx.id_tx()])
      }
      if solid == 0b11 {
        solidate_data = current_tx.map(|x| vec![(x.id_tx(), Some(height))]);
      }
    }
    Ok((approve_data, solidate_data))
  }
}
