use mapper::{BundleMapper, TransactionMapper};
use mysql;
use query::event;
use std::collections::VecDeque;
use std::sync::Arc;
use utils;
use worker::Result;

pub type ApproveVec = VecDeque<u64>;

pub struct Approve {
  conn: mysql::Conn,
  transaction_mapper: Arc<TransactionMapper>,
  bundle_mapper: Arc<BundleMapper>,
}

impl Approve {
  pub fn new(
    mysql_uri: &str,
    transaction_mapper: Arc<TransactionMapper>,
    bundle_mapper: Arc<BundleMapper>,
  ) -> Result<Self> {
    let conn = mysql::Conn::new(mysql_uri)?;
    Ok(Self {
      conn,
      transaction_mapper,
      bundle_mapper,
    })
  }

  pub fn perform(&mut self, mut nodes: ApproveVec) -> Result<()> {
    let (timestamp, mut counter) = (utils::milliseconds_since_epoch()?, 0);
    let &mut Self {
      ref mut conn,
      ref transaction_mapper,
      ref bundle_mapper,
    } = self;
    while let Some(id) = nodes.pop_back() {
      // TODO catch Error::Locked
      let mut guard = transaction_mapper.lock();
      let mut record = transaction_mapper.fetch(&mut guard, conn, id)?;
      if record.mst_a() || !record.is_persistent() {
        return Ok(());
      }
      if record.id_trunk() != 0 {
        nodes.push_front(record.id_trunk());
      }
      if record.id_branch() != 0 {
        nodes.push_front(record.id_branch());
      }
      bundle_mapper.modify(conn, record.id_bundle(), || {})?;
      // TODO
      // if record.current_idx() == 0 {
      //   query::update_bundle(
      //     &mut self.conn,
      //     record.id_bundle(),
      //     timestamp,
      //   )?;
      // }
      record.approve();
      counter += 1;
    }
    if counter > 0 {
      event::subtangle_confirmation(conn, timestamp, counter)?;
    }
    Ok(())
  }
}
