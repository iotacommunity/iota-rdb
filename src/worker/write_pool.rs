use counters::Counters;
use mapper::Mapper;
use mysql;
use std::sync::{Arc, Mutex, mpsc};
use std::thread;
use transaction::Transaction;
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
    for i in 0..threads_count {
      let write_rx = write_rx.clone();
      let approve_tx = self.approve_tx.clone();
      let solidate_tx = self.solidate_tx.clone();
      let counters = self.counters.clone();
      let milestone_address = self.milestone_address.to_owned();
      let milestone_start_index = self.milestone_start_index.to_owned();
      let mapper = Mapper::new(self.pool).expect("MySQL mapper failure");
      let mut worker = Write::new(self.pool, mapper, counters)
        .expect("Worker initialization failure");
      thread::spawn(move || loop {
        let message = write_rx
          .lock()
          .expect("Mutex is poisoned")
          .recv()
          .expect("Thread communication failure");
        match Transaction::new(
          &message,
          &milestone_address,
          &milestone_start_index,
        ) {
          Ok(mut transaction) => {
            match worker.perform(&mut transaction) {
              Ok((approve_data, solidate_data)) => {
                if verbose {
                  println!("write_thread#{} {:?}", i, transaction);
                }
                if let Some(approve_data) = approve_data {
                  approve_tx
                    .send(approve_data)
                    .expect("Thread communication failure");
                }
                if let Some(solidate_data) = solidate_data {
                  solidate_tx
                    .send(solidate_data)
                    .expect("Thread communication failure");
                }
              }
              Err(err) => {
                eprintln!("Transaction processing error: {}", err);
              }
            }
          }
          Err(err) => {
            eprintln!("Transaction parsing error: {}", err);
          }
        }
      });
    }
  }
}
