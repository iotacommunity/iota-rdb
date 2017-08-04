use mapper;
use std::sync::mpsc;
use std::thread;
use transaction::Transaction;
use worker::{ApproveVec, SolidateVec, Write};

pub struct WriteThread<'a> {
  pub write_rx: mpsc::Receiver<String>,
  pub approve_tx: mpsc::Sender<ApproveVec>,
  pub solidate_tx: mpsc::Sender<SolidateVec>,
  pub mysql_uri: &'a str,
  pub transaction_mapper: mapper::Transaction,
  pub address_mapper: mapper::Address,
  pub bundle_mapper: mapper::Bundle,
  pub milestone_address: &'a str,
  pub milestone_start_index: String,
}

impl<'a> WriteThread<'a> {
  pub fn spawn(self, verbose: bool) {
    let Self {
      write_rx,
      approve_tx,
      solidate_tx,
      milestone_start_index,
      ..
    } = self;
    let milestone_address = self.milestone_address.to_owned();
    let mut worker = Write::new(
      self.mysql_uri,
      self.transaction_mapper,
      self.address_mapper,
      self.bundle_mapper,
    ).expect("Worker initialization failure");
    thread::spawn(move || loop {
      let message = write_rx.recv().expect("Thread communication failure");
      match Transaction::new(
        &message,
        &milestone_address,
        &milestone_start_index,
      ) {
        Ok(transaction) => match worker.perform(&transaction) {
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
        },
        Err(err) => {
          eprintln!("[rdb] Parsing error: {}", err);
        }
      }
    });
  }
}
