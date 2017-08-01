use counters::Counters;
use std::sync::mpsc;
use std::thread;
use transaction::Transaction;
use worker::{ApproveVec, SolidateVec, Write};

pub struct WritePool<'a> {
  pub write_rx: mpsc::Receiver<String>,
  pub approve_tx: mpsc::Sender<ApproveVec>,
  pub solidate_tx: mpsc::Sender<SolidateVec>,
  pub mysql_uri: &'a str,
  pub counters: Counters,
  pub milestone_address: &'a str,
  pub milestone_start_index: &'a str,
}

impl<'a> WritePool<'a> {
  pub fn run(self, verbose: bool) {
    let write_rx = self.write_rx;
    let approve_tx = self.approve_tx.clone();
    let solidate_tx = self.solidate_tx.clone();
    let milestone_address = self.milestone_address.to_owned();
    let milestone_start_index = self.milestone_start_index.to_owned();
    let mut worker = Write::new(self.mysql_uri, self.counters)
      .expect("Worker initialization failure");
    thread::spawn(move || loop {
      let message = write_rx.recv().expect("Thread communication failure");
      match Transaction::new(
        &message,
        &milestone_address,
        &milestone_start_index,
      ) {
        Ok(transaction) => {
          match worker.perform(&transaction) {
            Ok((approve_data, solidate_data)) => {
              if verbose {
                println!("[rdb] {}", transaction.hash());
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
              eprintln!("[rdb] Processing error: {}", err);
            }
          }
        }
        Err(err) => {
          eprintln!("[rdb] Parsing error: {}", err);
        }
      }
    });
  }
}
