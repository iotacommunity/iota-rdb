use mapper::{AddressMapper, BundleMapper, TransactionMapper};
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use worker::Update;

const UPDATE_INTERVAL: u64 = 500;

pub struct UpdateThread<'a> {
  pub mysql_uri: &'a str,
  pub transaction_mapper: Arc<TransactionMapper>,
  pub address_mapper: Arc<AddressMapper>,
  pub bundle_mapper: Arc<BundleMapper>,
}

impl<'a> UpdateThread<'a> {
  pub fn spawn(self, verbose: bool) {
    let mut worker = Update::new(
      self.mysql_uri,
      self.transaction_mapper,
      self.address_mapper,
      self.bundle_mapper,
    ).expect("Worker initialization failure");
    thread::spawn(move || loop {
      thread::sleep(Duration::from_millis(UPDATE_INTERVAL));
      match worker.perform() {
        Ok(()) => if verbose {
          println!("[upd]");
        },
        Err(err) => {
          eprintln!("[upd] Update error: {}", err);
        }
      }
    });
  }
}
