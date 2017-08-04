use std::sync::mpsc;
use std::thread;
use worker::{Solidate, SolidateVec};

pub struct SolidateThread<'a> {
  pub solidate_rx: mpsc::Receiver<SolidateVec>,
  pub mysql_uri: &'a str,
}

impl<'a> SolidateThread<'a> {
  pub fn spawn(self, verbose: bool) {
    let Self { solidate_rx, .. } = self;
    let mut worker =
      Solidate::new(self.mysql_uri).expect("Worker initialization failure");
    thread::spawn(move || loop {
      let vec = solidate_rx.recv().expect("Thread communication failure");
      match worker.perform(vec.clone()) {
        Ok(()) => {
            if verbose {
              println!("[sol] {:?}", vec);
            }
          }
        Err(err) => {
          eprintln!("[sol] Error: {}", err);
        }
      }
    });
  }
}
