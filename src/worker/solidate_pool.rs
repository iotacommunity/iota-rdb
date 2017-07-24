use mapper::Mapper;
use mysql;
use std::sync::{Arc, Mutex, mpsc};
use std::thread;
use transaction::Transaction;

pub struct SolidatePool<'a> {
  pub rx: mpsc::Receiver<(String, i32)>,
  pub pool: &'a mysql::Pool,
}

impl<'a> SolidatePool<'a> {
  pub fn run(self, threads_count: usize, verbose: bool) {
    let rx = Arc::new(Mutex::new(self.rx));
    for i in 0..threads_count {
      let rx = rx.clone();
      let mut mapper = Mapper::new(self.pool).expect("MySQL mapper failure");
      thread::spawn(move || loop {
        let rx = rx.lock().expect("Mutex is poisoned");
        let (hash, height) = rx.recv().expect("Thread communication failure");
        match Transaction::solidate(&mut mapper, &hash, height) {
          Ok(()) => {
            if verbose {
              println!("solidate_thread#{} {:?}", i, hash);
            }
          }
          Err(err) => {
            eprintln!("Transaction solidity check error: {}", err);
          }
        }
      });
    }
  }
}
