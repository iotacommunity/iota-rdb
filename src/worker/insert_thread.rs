use mapper::{AddressMapper, BundleMapper, TransactionMapper};
use message::TransactionMessage;
use mysql;
use std::sync::{mpsc, Arc};
use std::thread;
use std::time::Instant;
use utils::{self, DurationUtils, MysqlConnUtils};
use worker::{ApproveJob, CalculateJob, SolidateJob};

const HASH_SIZE: usize = 81;

pub struct InsertThread<'a> {
  pub insert_rx: mpsc::Receiver<String>,
  pub approve_tx: mpsc::Sender<ApproveJob>,
  pub solidate_tx: mpsc::Sender<SolidateJob>,
  pub calculate_tx: mpsc::Sender<CalculateJob>,
  pub mysql_uri: &'a str,
  pub retry_interval: u64,
  pub transaction_mapper: Arc<TransactionMapper>,
  pub address_mapper: Arc<AddressMapper>,
  pub bundle_mapper: Arc<BundleMapper>,
  pub milestone_address: &'a str,
  pub milestone_start_index: String,
}

impl<'a> InsertThread<'a> {
  pub fn spawn(self) {
    let Self {
      insert_rx,
      approve_tx,
      solidate_tx,
      calculate_tx,
      mysql_uri,
      retry_interval,
      transaction_mapper,
      address_mapper,
      bundle_mapper,
      milestone_address,
      milestone_start_index,
    } = self;
    let milestone_address = milestone_address.to_owned();
    let mut conn = mysql::Conn::new_retry(mysql_uri, retry_interval);
    let null_hash = utils::trits_string(0, HASH_SIZE)
      .expect("Can't convert null_hash to trits");
    let thread = thread::Builder::new().name("insert".into());
    let thread = thread.spawn(move || {
      let transaction_mapper = &*transaction_mapper;
      let address_mapper = &*address_mapper;
      let bundle_mapper = &*bundle_mapper;
      loop {
        let message = insert_rx.recv().expect("Thread communication failure");
        let duration = Instant::now();
        let result = TransactionMessage::parse(
          &message,
          &milestone_address,
          &milestone_start_index,
        );
        match result {
          Ok(message) => {
            let result = message.perform(
              &mut conn,
              transaction_mapper,
              address_mapper,
              bundle_mapper,
              &null_hash,
            );
            let duration = duration.elapsed().as_milliseconds();
            match result {
              Ok((approve_data, solidate_data, calculate_data)) => {
                info!("{:.3}ms {}", duration, message.hash());
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
                if let Some(calculate_data) = calculate_data {
                  calculate_tx
                    .send(calculate_data)
                    .expect("Thread communication failure");
                }
              }
              Err(err) => {
                error!("{:.3}ms Processing failure: {}", duration, err);
              }
            }
          }
          Err(err) => {
            let duration = duration.elapsed().as_milliseconds();
            error!("{:.3}ms Parsing failure: {}", duration, err);
          }
        }
      }
    });
    thread.expect("Thread spawn failure");
  }
}
