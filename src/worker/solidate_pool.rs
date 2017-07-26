use mysql;
use std::sync::{Arc, Mutex, mpsc};
use worker::{Solidate, SolidateVec};

pub struct SolidatePool<'a> {
  pub solidate_rx: mpsc::Receiver<SolidateVec>,
  pub pool: &'a mysql::Pool,
}

impl<'a> SolidatePool<'a> {
  pub fn run(self, threads_count: usize, verbose: bool) {
    let solidate_rx = Arc::new(Mutex::new(self.solidate_rx));
    for thread_number in 0..threads_count {
      Solidate {
        solidate_rx: solidate_rx.clone(),
      }.spawn(self.pool, thread_number, verbose)
    }
  }
}
