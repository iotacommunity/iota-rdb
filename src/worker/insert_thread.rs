use mapper::{self, AddressMapper, BundleMapper, TransactionMapper};
use message::Message;
use std::collections::VecDeque;
use std::sync::{mpsc, Arc};
use std::thread;
use std::time::Duration;
use worker::{self, ApproveVec, Insert, SolidateVec};

const LOCK_RETRY_INTERVAL: u64 = 100;

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
    let mut queue = VecDeque::new();
    thread::spawn(move || loop {
      match insert_rx.recv_timeout(Duration::from_millis(LOCK_RETRY_INTERVAL)) {
        Ok(message) => {
          let message = Message::parse(
            &message,
            &milestone_address,
            &milestone_start_index,
          );
          match message {
            Ok(message) => queue.push_back(message),
            Err(err) => {
              eprintln!("[ins] Parsing error: {}", err);
            }
          }
        }
        Err(mpsc::RecvTimeoutError::Timeout) => {}
        Err(mpsc::RecvTimeoutError::Disconnected) => {
          panic!("Thread communication failure");
        }
      }
      queue.retain(|message| match worker.perform(message) {
        Ok((approve_data, solidate_data)) => {
          if verbose {
            println!("[ins] {}", message.hash());
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
          false
        }
        Err(worker::Error::Mapper(mapper::Error::Locked)) => true,
        Err(err) => {
          eprintln!("[ins] Processing error: {}", err);
          false
        }
      });
    });
  }
}
