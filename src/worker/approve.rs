use mapper::{BundleMapper, Mapper, Record, TransactionMapper};
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
      let mut transaction = transaction_mapper.fetch(&mut guard, conn, id)?;
      if transaction.mst_a() || !transaction.is_persistent() {
        return Ok(());
      }
      if transaction.id_trunk() != 0 {
        nodes.push_front(transaction.id_trunk());
      }
      if transaction.id_branch() != 0 {
        nodes.push_front(transaction.id_branch());
      }
      if transaction.current_idx() == 0 {
        let mut guard = bundle_mapper.lock();
        let mut bundle = bundle_mapper
          .fetch(&mut guard, conn, transaction.id_bundle())?;
        bundle.set_confirmed(timestamp);
      }
      transaction.approve();
      counter += 1;
    }
    if counter > 0 {
      event::subtangle_confirmation(conn, timestamp, counter)?;
    }
    Ok(())
  }
}
