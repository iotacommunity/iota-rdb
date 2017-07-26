use counters::Counters;
use mysql;
use std::sync::{Arc, Mutex, mpsc};
use worker::{ApproveVec, SolidateVec, Write};

pub struct WritePool<'a> {
  pub write_rx: mpsc::Receiver<String>,
  pub approve_tx: &'a mpsc::Sender<ApproveVec>,
  pub solidate_tx: &'a mpsc::Sender<SolidateVec>,
  pub pool: &'a mysql::Pool,
  pub counters: Arc<Counters>,
  pub milestone_address: &'a str,
  pub milestone_start_index: &'a str,
}

impl<'a> WritePool<'a> {
  pub fn run(self, threads_count: usize, verbose: bool) {
    let write_rx = Arc::new(Mutex::new(self.write_rx));
    for thread_number in 0..threads_count {
      Write {
        write_rx: write_rx.clone(),
        approve_tx: self.approve_tx.clone(),
        solidate_tx: self.solidate_tx.clone(),
        counters: self.counters.clone(),
        milestone_address: self.milestone_address.to_owned(),
        milestone_start_index: self.milestone_start_index.to_owned(),
      }.spawn(self.pool, thread_number, verbose);
    }
  }
}
