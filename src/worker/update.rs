use super::Result;
use mapper::{AddressMapper, BundleMapper, TransactionMapper};
use mysql;
use std::sync::Arc;

pub struct Update {
  conn: mysql::Conn,
  transaction_mapper: Arc<TransactionMapper>,
  address_mapper: Arc<AddressMapper>,
  bundle_mapper: Arc<BundleMapper>,
}

impl Update {
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

  pub fn perform(&mut self) -> Result<()> {
    self.transaction_mapper.update(&mut self.conn)?;
    self.address_mapper.update(&mut self.conn)?;
    self.bundle_mapper.update(&mut self.conn)?;
    Ok(())
  }
}
