use super::Result;
use mapper::{AddressMapper, BundleMapper, TransactionMapper};
use mysql;
use query::event;
use std::sync::Arc;
use transaction::Transaction;
use utils;
use worker::{ApproveVec, SolidateVec};

pub struct Insert {
  conn: mysql::Conn,
  transaction_mapper: Arc<TransactionMapper>,
  address_mapper: Arc<AddressMapper>,
  bundle_mapper: Arc<BundleMapper>,
}

impl Insert {
  pub fn new(
    mysql_uri: &str,
    transaction_mapper: Arc<TransactionMapper>,
    address_mapper: Arc<AddressMapper>,
    bundle_mapper: Arc<BundleMapper>,
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
    if let Some((mut current_tx, trunk_tx, branch_tx)) =
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
      current_tx.set_height(if solid != 0b11 && trunk_tx.solid() == 0b11 {
        trunk_tx.height() + 1
      } else {
        0
      });
      if trunk_tx.solid() == 0b11 {
        solid |= 0b10;
      }
      if branch_tx.solid() == 0b11 {
        solid |= 0b01;
      }
      current_tx.set_tag(transaction.tag().to_owned());
      current_tx.set_value(transaction.value());
      current_tx.set_timestamp(transaction.timestamp());
      current_tx.set_current_idx(transaction.current_index());
      current_tx.set_last_idx(transaction.last_index());
      current_tx.set_is_mst(transaction.is_milestone());
      current_tx.set_mst_a(transaction.is_milestone());
      current_tx.set_id_trunk(trunk_tx.id_tx());
      current_tx.set_id_branch(branch_tx.id_tx());
      current_tx.set_id_address(id_address);
      current_tx.set_id_bundle(id_bundle);
      current_tx.set_solid(solid);
      if current_tx.solid() != 0b11 {
        event::unsolid_transaction(&mut self.conn, timestamp)?;
      }
      event::new_transaction_received(&mut self.conn, timestamp)?;
      if transaction.is_milestone() {
        event::milestone_received(&mut self.conn, timestamp)?;
        approve_data = Some(vec![trunk_tx.id_tx(), branch_tx.id_tx()])
      }
      if current_tx.solid() == 0b11 {
        solidate_data =
          Some(vec![(current_tx.id_tx(), Some(current_tx.height()))]);
      }
      self
        .transaction_mapper
        .insert(&mut self.conn, transaction.hash(), current_tx)?;
    }
    Ok((approve_data, solidate_data))
  }
}
