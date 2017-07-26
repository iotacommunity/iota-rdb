use mysql;
use std::sync::{Arc, Mutex, mpsc};
use worker::{Approve, ApproveVec};

pub struct ApprovePool<'a> {
  pub approve_rx: mpsc::Receiver<ApproveVec>,
  pub pool: &'a mysql::Pool,
}

impl<'a> ApprovePool<'a> {
  pub fn run(self, threads_count: usize, verbose: bool) {
    let approve_rx = Arc::new(Mutex::new(self.approve_rx));
    for thread_number in 0..threads_count {
      Approve {
        approve_rx: approve_rx.clone(),
      }.spawn(self.pool, thread_number, verbose);
    }
  }
}
