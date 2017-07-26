use mapper::Mapper;
use mysql;
use std::sync::{Arc, Mutex, mpsc};
use std::thread;
use transaction::{SolidateVec, Transaction};

pub struct SolidatePool<'a> {
  pub rx: mpsc::Receiver<SolidateVec>,
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
        let vec = rx.recv().expect("Thread communication failure");
        match Transaction::solidate(&mut mapper, vec.clone()) {
          Ok(()) => {
            if verbose {
              println!("solidate_thread#{} {:?}", i, vec);
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
