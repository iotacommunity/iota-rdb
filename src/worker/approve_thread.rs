use mapper::{BundleMapper, TransactionMapper};
use std::sync::{mpsc, Arc};
use std::thread;
use worker::{Approve, ApproveVec};

pub struct ApproveThread<'a> {
  pub approve_rx: mpsc::Receiver<ApproveVec>,
  pub mysql_uri: &'a str,
  pub transaction_mapper: Arc<TransactionMapper>,
  pub bundle_mapper: Arc<BundleMapper>,
}

impl<'a> ApproveThread<'a> {
  pub fn spawn(self, verbose: bool) {
    let Self { approve_rx, .. } = self;
    let mut worker =
      Approve::new(self.mysql_uri, self.transaction_mapper, self.bundle_mapper)
        .expect("Worker initialization failure");
    thread::spawn(move || loop {
      let vec = approve_rx.recv().expect("Thread communication failure");
      match worker.perform(vec.clone()) {
        Ok(()) => {
            if verbose {
              println!("[apv] {:?}", vec);
            }
          }
        Err(err) => {
          eprintln!("[apv] Error: {}", err);
        }
      }
    });
  }
}
