use super::Result;
use mapper::{self, AddressMapper, BundleMapper, TransactionMapper};
use message::TransactionMessage;
use mysql;
use query::event;
use std::collections::VecDeque;
use std::sync::{mpsc, Arc};
use std::thread;
use std::time::Duration;
use utils;
use worker::{self, ApproveVec, SolidateVec};

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
      mysql_uri,
      transaction_mapper,
      address_mapper,
      bundle_mapper,
      milestone_address,
      milestone_start_index,
    } = self;
    let milestone_address = milestone_address.to_owned();
    let mut conn =
      mysql::Conn::new(mysql_uri).expect("MySQL connection failure");
    thread::spawn(move || {
      let transaction_mapper = &*transaction_mapper;
      let address_mapper = &*address_mapper;
      let bundle_mapper = &*bundle_mapper;
      let mut queue = VecDeque::new();
      loop {
        match insert_rx
          .recv_timeout(Duration::from_millis(LOCK_RETRY_INTERVAL))
        {
          Ok(message) => match TransactionMessage::parse(
            &message,
            &milestone_address,
            &milestone_start_index,
          ) {
            Ok(message) => queue.push_back(message),
            Err(err) => {
              eprintln!("[ins] Parsing error: {}", err);
            }
          },
          Err(mpsc::RecvTimeoutError::Timeout) => {}
          Err(mpsc::RecvTimeoutError::Disconnected) => {
            panic!("Thread communication failure");
          }
        }
        queue.retain(|message| {
          match perform(
            &mut conn,
            transaction_mapper,
            address_mapper,
            bundle_mapper,
            message,
          ) {
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
          }
        });
      }
    });
  }
}

pub fn perform(
  conn: &mut mysql::Conn,
  transaction_mapper: &TransactionMapper,
  address_mapper: &AddressMapper,
  bundle_mapper: &BundleMapper,
  message: &TransactionMessage,
) -> Result<(Option<ApproveVec>, Option<SolidateVec>)> {
  let timestamp = utils::milliseconds_since_epoch()?;
  let (mut approve_data, mut solidate_data) = (None, None);
  let id_address = address_mapper
    .fetch_or_insert(conn, message.address_hash())?;
  let id_bundle = bundle_mapper.fetch_or_insert(
    conn,
    message.bundle_hash(),
    message.last_index(),
    timestamp,
  )?;
  let txs = transaction_mapper.fetch_triplet(
    conn,
    message.hash(),
    message.trunk_hash(),
    message.branch_hash(),
  )?;
  if let Some((mut current_tx, trunk_tx, branch_tx)) = txs {
    let mut solid = message.solid();
    current_tx.set_height(if solid != 0b11 && trunk_tx.solid() == 0b11 {
      trunk_tx.height() + 1
    } else {
      0
    });
    if trunk_tx.solid() == 0b11 {
      solid |= 0b10;
    }
    if branch_tx.solid() == 0b11 {
      solid |= 0b01;
    }
    current_tx.set_tag(message.tag().to_owned());
    current_tx.set_value(message.value());
    current_tx.set_timestamp(message.timestamp());
    current_tx.set_current_idx(message.current_index());
    current_tx.set_last_idx(message.last_index());
    current_tx.set_is_mst(message.is_milestone());
    current_tx.set_mst_a(message.is_milestone());
    current_tx.set_id_trunk(trunk_tx.id_tx());
    current_tx.set_id_branch(branch_tx.id_tx());
    current_tx.set_id_address(id_address);
    current_tx.set_id_bundle(id_bundle);
    current_tx.set_solid(solid);
    if current_tx.solid() != 0b11 {
      event::unsolid_transaction(conn, timestamp)?;
    }
    event::new_transaction_received(conn, timestamp)?;
    if message.is_milestone() {
      event::milestone_received(conn, timestamp)?;
      let mut deque = VecDeque::new();
      deque.push_front(trunk_tx.id_tx());
      if branch_tx.id_tx() != trunk_tx.id_tx() {
        deque.push_front(branch_tx.id_tx());
      }
      approve_data = Some(deque);
    }
    if current_tx.solid() == 0b11 {
      solidate_data =
        Some(vec![(current_tx.id_tx(), Some(current_tx.height()))]);
    }
    transaction_mapper
      .insert(conn, current_tx, trunk_tx, branch_tx)?;
  }
  Ok((approve_data, solidate_data))
}
