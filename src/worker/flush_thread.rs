use mapper::{AddressMapper, BundleMapper, TransactionMapper};
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use worker::Flush;

const FLUSH_INTERVAL: u64 = 100;

pub struct FlushThread<'a> {
  pub mysql_uri: &'a str,
  pub transaction_mapper: Arc<TransactionMapper>,
  pub address_mapper: Arc<AddressMapper>,
  pub bundle_mapper: Arc<BundleMapper>,
}

impl<'a> FlushThread<'a> {
  pub fn spawn(self, verbose: bool) {
    let mut worker = Flush::new(
      self.mysql_uri,
      self.transaction_mapper,
      self.address_mapper,
      self.bundle_mapper,
    ).expect("Worker initialization failure");
    thread::spawn(move || loop {
      thread::sleep(Duration::from_millis(FLUSH_INTERVAL));
      match worker.perform() {
        Ok(()) => if verbose {
          println!("[fls]");
        },
        Err(err) => {
          eprintln!("[fls] Flush error: {}", err);
        }
      }
    });
  }
}
