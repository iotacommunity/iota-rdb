use mysql;
use std::sync::{Arc, Mutex, mpsc};
use std::thread;
use worker::{Approve, ApproveVec};

pub struct ApprovePool<'a> {
  pub approve_rx: mpsc::Receiver<ApproveVec>,
  pub pool: &'a mysql::Pool,
}

impl<'a> ApprovePool<'a> {
  pub fn run(self, threads_count: usize, verbose: bool) {
    let approve_rx = Arc::new(Mutex::new(self.approve_rx));
    for i in 0..threads_count {
      let approve_rx = approve_rx.clone();
      let mut worker =
        Approve::new(self.pool).expect("Worker initialization failure");
      thread::spawn(move || loop {
        let vec = approve_rx
          .lock()
          .expect("Mutex is poisoned")
          .recv()
          .expect("Thread communication failure");
        match worker.perform(vec.clone()) {
          Ok(()) => {
            if verbose {
              println!("approve_thread#{} {:?}", i, vec);
            }
          }
          Err(err) => {
            eprintln!("Transaction approve error: {}", err);
          }
        }
      });
    }
  }
}
