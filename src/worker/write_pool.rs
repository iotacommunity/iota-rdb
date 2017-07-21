use counters::Counters;
use mapper::Mapper;
use mysql;
use std::sync::{Arc, Mutex, mpsc};
use std::thread;
use transaction::Transaction;

pub struct WritePool<'a> {
  pub rx: mpsc::Receiver<String>,
  pub approve_tx: &'a mpsc::Sender<Vec<u64>>,
  pub pool: &'a mysql::Pool,
  pub counters: Arc<Counters>,
  pub milestone_address: &'a str,
  pub milestone_start_index: &'a str,
}

impl<'a> WritePool<'a> {
  pub fn run(self, threads_count: usize, verbose: bool) {
    let rx = Arc::new(Mutex::new(self.rx));
    for i in 0..threads_count {
      let (approve_tx, rx) = (self.approve_tx.clone(), rx.clone());
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
              Ok(Some(vec)) => {
                approve_tx.send(vec).expect("Thread communication failure");
              }
              Ok(None) => {
                if verbose {
                  println!("write_thread#{} {:?}", i, transaction);
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
