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
  pub generation_limit: usize,
  pub transaction_mapper: Arc<TransactionMapper>,
  pub address_mapper: Arc<AddressMapper>,
  pub bundle_mapper: Arc<BundleMapper>,
}

impl<'a> UpdateThread<'a> {
  pub fn spawn(self) {
    let Self {
      mysql_uri,
      update_interval,
      generation_limit,
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
        let result = perform(
          &mut conn,
          transaction_mapper,
          generation_limit,
          address_mapper,
          bundle_mapper,
        );
        let duration = duration.elapsed().as_milliseconds();
        match result {
          Ok((updated, cleaned)) => {
            info!(
              "{:.3}ms updated: {}, cleaned: {}",
              duration,
              updated,
              cleaned
            );
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
  generation_limit: usize,
  address_mapper: &AddressMapper,
  bundle_mapper: &BundleMapper,
) -> Result<(usize, usize)> {
  let (mut updated, mut cleaned) = (0, 0);
  updated += transaction_mapper.update(conn)?;
  cleaned += transaction_mapper.collect_garbage(generation_limit);
  updated += address_mapper.update(conn)?;
  cleaned += address_mapper.collect_garbage(generation_limit);
  updated += bundle_mapper.update(conn)?;
  cleaned += bundle_mapper.collect_garbage(generation_limit);
  Ok((updated, cleaned))
}
