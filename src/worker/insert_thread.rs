use super::Result;
use event;
use mapper::{AddressMapper, BundleMapper, Mapper, TransactionMapper};
use message::TransactionMessage;
use mysql;
use record::{AddressRecord, BundleRecord, Record, TransactionRecord};
use std::collections::VecDeque;
use std::sync::{mpsc, Arc, MutexGuard};
use std::thread;
use utils;
use worker::{ApproveVec, SolidateVec};

const HASH_SIZE: usize = 81;

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
    let null_hash = utils::trits_string(0, HASH_SIZE)
      .expect("Can't convert null_hash to trits");
    thread::spawn(move || {
      let transaction_mapper = &*transaction_mapper;
      let address_mapper = &*address_mapper;
      let bundle_mapper = &*bundle_mapper;
      loop {
        let message = insert_rx.recv().expect("Thread communication failure");
        match TransactionMessage::parse(
          &message,
          &milestone_address,
          &milestone_start_index,
        ) {
          Ok(message) => match perform(
            &mut conn,
            transaction_mapper,
            address_mapper,
            bundle_mapper,
            &message,
            &null_hash,
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
            }
            Err(err) => {
              eprintln!("[ins] Processing error: {}", err);
            }
          },
          Err(err) => {
            eprintln!("[ins] Parsing error: {}", err);
          }
        }
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
  null_hash: &str,
) -> Result<(Option<ApproveVec>, Option<SolidateVec>)> {
  let (mut approve_data, mut solidate_data) = (None, None);
  if !is_valid(message, null_hash) {
    return Ok((approve_data, solidate_data));
  }
  let hashes =
    vec![message.hash(), message.trunk_hash(), message.branch_hash()];
  let txs = transaction_mapper.fetch_many(conn, hashes)?;
  let mut txs = txs.iter().map(|tx| tx.lock().unwrap()).collect();
  let txs = unwrap_transactions(&mut txs, message);
  if let Some((mut current_tx, mut trunk_tx, mut branch_tx)) = txs {
    let timestamp = utils::milliseconds_since_epoch()?;
    let id_address = address_mapper.fetch_or_insert(
      conn,
      message.address_hash(),
      |id_address| {
        AddressRecord::new(id_address, message.address_hash().to_owned())
      },
    )?;
    let id_bundle = bundle_mapper.fetch_or_insert(
      conn,
      message.bundle_hash(),
      |id_bundle| {
        Ok(BundleRecord::new(
          id_bundle,
          message.bundle_hash().to_owned(),
          message.last_index(),
          timestamp,
        ))
      },
    )?;
    let id_branch = map_branch(trunk_tx, &branch_tx, TransactionRecord::id_tx);
    solidate_genesis(trunk_tx, null_hash);
    trunk_tx.direct_approve();
    if let Some(ref mut branch_tx) = branch_tx {
      solidate_genesis(branch_tx, null_hash);
      branch_tx.direct_approve();
    }
    let mut solid = message.solid();
    if trunk_tx.solid() == 0b11 {
      solid |= 0b10;
    }
    if map_branch(trunk_tx, &branch_tx, TransactionRecord::solid) == 0b11 {
      solid |= 0b01;
    }
    current_tx.set_solid(solid);
    if solid != 0b11 && trunk_tx.solid() == 0b11 {
      current_tx.set_height(trunk_tx.height() + 1);
    } else {
      current_tx.set_height(0);
    }
    current_tx.set_tag(message.tag().to_owned());
    current_tx.set_value(message.value());
    current_tx.set_timestamp(message.timestamp());
    current_tx.set_current_idx(message.current_index());
    current_tx.set_last_idx(message.last_index());
    current_tx.set_is_mst(message.is_milestone());
    current_tx.set_mst_a(message.is_milestone());
    current_tx.set_id_trunk(trunk_tx.id_tx());
    current_tx.set_id_branch(id_branch);
    current_tx.set_id_address(id_address);
    current_tx.set_id_bundle(id_bundle);
    if solid != 0b11 {
      event::unsolid_transaction(conn, timestamp)?;
    }
    event::new_transaction_received(conn, timestamp)?;
    if message.is_milestone() {
      event::milestone_received(conn, timestamp)?;
      let mut deque = VecDeque::new();
      deque.push_front(trunk_tx.id_tx());
      if let Some(ref branch_tx) = branch_tx {
        deque.push_front(branch_tx.id_tx());
      }
      approve_data = Some(deque);
    }
    if solid == 0b11 {
      let vec = vec![(current_tx.id_tx(), Some(current_tx.height()))];
      solidate_data = Some(vec);
    }
    current_tx.insert(conn)?;
  }
  Ok((approve_data, solidate_data))
}

fn is_valid(message: &TransactionMessage, null_hash: &str) -> bool {
  message.hash() != null_hash ||
    message.hash() != message.trunk_hash() &&
      message.hash() != message.branch_hash()
}

fn unwrap_transactions<'a>(
  transactions: &'a mut Vec<MutexGuard<TransactionRecord>>,
  message: &TransactionMessage,
) -> Option<
  (
    &'a mut TransactionRecord,
    &'a mut TransactionRecord,
    Option<&'a mut TransactionRecord>,
  ),
> {
  let (mut current_tx, mut trunk_tx, mut branch_tx) = (None, None, None);
  for transaction in transactions {
    let transaction = &mut **transaction;
    if transaction.hash() == message.hash() {
      current_tx = Some(transaction);
    } else if transaction.hash() == message.trunk_hash() {
      trunk_tx = Some(transaction);
    } else if transaction.hash() == message.branch_hash() {
      branch_tx = Some(transaction);
    }
  }
  current_tx.and_then(|current_tx| if current_tx.is_persisted() {
    None
  } else {
    trunk_tx.map(|trunk_tx| (current_tx, trunk_tx, branch_tx))
  })
}

fn solidate_genesis(tx: &mut TransactionRecord, null_hash: &str) {
  if !tx.is_persisted() && tx.hash() == null_hash {
    tx.set_solid(0b11);
  }
}

fn map_branch<T, U>(
  trunk_tx: &mut TransactionRecord,
  branch_tx: &Option<&mut TransactionRecord>,
  f: T,
) -> U
where
  T: FnOnce(&TransactionRecord) -> U,
{
  if let Some(ref branch_tx) = *branch_tx {
    f(branch_tx)
  } else {
    f(trunk_tx)
  }
}
