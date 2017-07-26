use mysql;
use std::sync::{Arc, Mutex, mpsc};
use std::thread;
use worker::{Solidate, SolidateVec};

pub struct SolidatePool<'a> {
  pub solidate_rx: mpsc::Receiver<SolidateVec>,
  pub pool: &'a mysql::Pool,
}

impl<'a> SolidatePool<'a> {
  pub fn run(self, threads_count: usize, verbose: bool) {
    let solidate_rx = Arc::new(Mutex::new(self.solidate_rx));
    for i in 0..threads_count {
      let solidate_rx = solidate_rx.clone();
      let mut worker =
        Solidate::new(self.pool).expect("Worker initialization failure");
      thread::spawn(move || loop {
        let vec = solidate_rx
          .lock()
          .expect("Mutex is poisoned")
          .recv()
          .expect("Thread communication failure");
        match worker.perform(vec.clone()) {
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
