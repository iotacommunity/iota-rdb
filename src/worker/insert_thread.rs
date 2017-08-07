use mapper::{AddressMapper, BundleMapper, TransactionMapper};
use std::sync::{mpsc, Arc};
use std::thread;
use transaction::Transaction;
use worker::{ApproveVec, Insert, SolidateVec};

pub struct InsertThread<'a> {
  pub insert_rx: mpsc::Receiver<String>,
  pub approve_tx: mpsc::Sender<ApproveVec>,
  pub solidate_tx: mpsc::Sender<SolidateVec>,
  pub mysql_uri: &'a str,
  pub transaction_mapper: Arc<TransactionMapper>,
  pub address_mapper: Arc<AddressMapper>,
  pub bundle_mapper: Arc<BundleMapper>,
  pub milestone_address: &'a str,
  pub milestone_start_index: String,
}

impl<'a> InsertThread<'a> {
  pub fn spawn(self, verbose: bool) {
    let Self {
      insert_rx,
      approve_tx,
      solidate_tx,
      milestone_start_index,
      ..
    } = self;
    let milestone_address = self.milestone_address.to_owned();
    let mut worker = Insert::new(
      self.mysql_uri,
      self.transaction_mapper,
      self.address_mapper,
      self.bundle_mapper,
    ).expect("Worker initialization failure");
    thread::spawn(move || loop {
      let message = insert_rx.recv().expect("Thread communication failure");
      match Transaction::new(
        &message,
        &milestone_address,
        &milestone_start_index,
      ) {
        Ok(transaction) => match worker.perform(&transaction) {
          Ok((approve_data, solidate_data)) => {
            if verbose {
              println!("[ins] {}", transaction.hash());
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
            eprintln!("[ins] Processing error: {}", err);
          }
        },
        Err(err) => {
          eprintln!("[ins] Parsing error: {}", err);
        }
      }
    });
  }
}
