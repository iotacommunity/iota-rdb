use super::Result;
use mapper::{AddressMapper, BundleMapper, Mapper, TransactionMapper};
use mysql;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

const UPDATE_INTERVAL: u64 = 500;

pub struct UpdateThread<'a> {
  pub mysql_uri: &'a str,
  pub transaction_mapper: Arc<TransactionMapper>,
  pub address_mapper: Arc<AddressMapper>,
  pub bundle_mapper: Arc<BundleMapper>,
}

impl<'a> UpdateThread<'a> {
  pub fn spawn(self, verbose: bool) {
    let Self {
      mysql_uri,
      transaction_mapper,
      address_mapper,
      bundle_mapper,
    } = self;
    let mut conn =
      mysql::Conn::new(mysql_uri).expect("MySQL connection failure");
    thread::spawn(move || {
      let transaction_mapper = &*transaction_mapper;
      let address_mapper = &*address_mapper;
      let bundle_mapper = &*bundle_mapper;
      loop {
        thread::sleep(Duration::from_millis(UPDATE_INTERVAL));
        match perform(
          &mut conn,
          transaction_mapper,
          address_mapper,
          bundle_mapper,
        ) {
          Ok(()) => if verbose {
            println!("[upd]");
          },
          Err(err) => {
            eprintln!("[upd] Update error: {}", err);
          }
        }
      }
    });
  }
}

pub fn perform(
  conn: &mut mysql::Conn,
  transaction_mapper: &TransactionMapper,
  address_mapper: &AddressMapper,
  bundle_mapper: &BundleMapper,
) -> Result<()> {
  transaction_mapper.update(conn)?;
  address_mapper.update(conn)?;
  bundle_mapper.update(conn)?;
  Ok(())
}
