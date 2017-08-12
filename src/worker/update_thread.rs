use super::Result;
use mapper::{AddressMapper, BundleMapper, Mapper, TransactionMapper};
use mysql;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};
use utils::DurationUtils;

pub struct UpdateThread<'a> {
  pub mysql_uri: &'a str,
  pub update_interval: u64,
  pub transaction_mapper: Arc<TransactionMapper>,
  pub address_mapper: Arc<AddressMapper>,
  pub bundle_mapper: Arc<BundleMapper>,
}

impl<'a> UpdateThread<'a> {
  pub fn spawn(self) {
    let Self {
      mysql_uri,
      update_interval,
      transaction_mapper,
      address_mapper,
      bundle_mapper,
    } = self;
    let update_interval = Duration::from_millis(update_interval);
    let mut conn =
      mysql::Conn::new(mysql_uri).expect("MySQL connection failure");
    thread::spawn(move || {
      let transaction_mapper = &*transaction_mapper;
      let address_mapper = &*address_mapper;
      let bundle_mapper = &*bundle_mapper;
      loop {
        thread::sleep(update_interval);
        let duration = Instant::now();
        let result =
          perform(&mut conn, transaction_mapper, address_mapper, bundle_mapper);
        let duration = duration.elapsed().as_milliseconds();
        match result {
          Ok(()) => {
            info!("{:.3}ms Ok", duration);
          }
          Err(err) => {
            error!("{:.3}ms {}", duration, err);
          }
        }
      }
    });
  }
}

fn perform(
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
