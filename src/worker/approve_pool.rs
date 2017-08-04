use std::sync::mpsc;
use std::thread;
use worker::{Approve, ApproveVec};

pub struct ApprovePool<'a> {
  pub approve_rx: mpsc::Receiver<ApproveVec>,
  pub mysql_uri: &'a str,
}

impl<'a> ApprovePool<'a> {
  pub fn run(self, verbose: bool) {
    let approve_rx = self.approve_rx;
    let mut worker =
      Approve::new(self.mysql_uri).expect("Worker initialization failure");
    thread::spawn(move || loop {
      let vec = approve_rx.recv().expect("Thread communication failure");
      match worker.perform(vec.clone()) {
        Ok(()) => {
            if verbose {
              println!("[apv] {:?}", vec);
            }
          }
        Err(err) => {
          eprintln!("[apv] Error: {}", err);
        }
      }
    });
  }
}
