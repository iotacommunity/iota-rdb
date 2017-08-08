use super::Result;
use mapper::{AddressMapper, BundleMapper, Mapper, TransactionMapper};
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
    let &mut Self {
      ref mut conn,
      ref transaction_mapper,
      ref address_mapper,
      ref bundle_mapper,
    } = self;
    update(&**transaction_mapper, conn)?;
    // TODO
    // update(&**address_mapper, conn)?;
    update(&**bundle_mapper, conn)?;
    Ok(())
  }
}

fn update<T: Mapper>(mapper: &T, conn: &mut mysql::Conn) -> Result<()> {
  let mut guard = mapper.lock();
  mapper.update(&mut guard, conn)?;
  Ok(())
}
