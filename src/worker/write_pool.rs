use counters::Counters;
use mapper::Mapper;
use mysql;
use std::sync::{Arc, Mutex, mpsc};
use std::thread;
use transaction::{ApproveVec, SolidateVec, Transaction};

pub struct WritePool<'a> {
  pub rx: mpsc::Receiver<String>,
  pub approve_tx: &'a mpsc::Sender<ApproveVec>,
  pub solidate_tx: &'a mpsc::Sender<SolidateVec>,
  pub pool: &'a mysql::Pool,
  pub counters: Arc<Counters>,
  pub milestone_address: &'a str,
  pub milestone_start_index: &'a str,
}

impl<'a> WritePool<'a> {
  pub fn run(self, threads_count: usize, verbose: bool) {
    let rx = Arc::new(Mutex::new(self.rx));
    for i in 0..threads_count {
      let rx = rx.clone();
      let approve_tx = self.approve_tx.clone();
      let solidate_tx = self.solidate_tx.clone();
      let counters = self.counters.clone();
      let mut mapper = Mapper::new(self.pool).expect("MySQL mapper failure");
      let milestone_address = self.milestone_address.to_owned();
      let milestone_start_index = self.milestone_start_index.to_owned();
      thread::spawn(move || loop {
        let rx = rx.lock().expect("Mutex is poisoned");
        match Transaction::parse(
          &rx.recv().expect("Thread communication failure"),
          &milestone_address,
          &milestone_start_index,
        ) {
          Ok(mut transaction) => {
            match transaction.process(&mut mapper, &counters) {
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
