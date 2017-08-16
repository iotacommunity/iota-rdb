use super::Result;
use event;
use mapper::{AddressMapper, AddressRecord, BundleMapper, BundleRecord, Mapper,
             Record, TransactionMapper, TransactionRecord};
use message::TransactionMessage;
use mysql;
use solid::{Solid, Solidate};
use std::sync::{mpsc, Arc, MutexGuard};
use std::thread;
use std::time::{Instant, SystemTime};
use utils::{self, DurationUtils, SystemTimeUtils};
use worker::{ApproveMessage, CalculateMessage, SolidateMessage};

const HASH_SIZE: usize = 81;

pub struct InsertThread<'a> {
  pub insert_rx: mpsc::Receiver<String>,
  pub approve_tx: mpsc::Sender<ApproveMessage>,
  pub solidate_tx: mpsc::Sender<SolidateMessage>,
  pub calculate_tx: mpsc::Sender<CalculateMessage>,
  pub mysql_uri: &'a str,
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
        let duration = Instant::now();
        let result = TransactionMessage::parse(
          &message,
          &milestone_address,
          &milestone_start_index,
        );
        match result {
          Ok(message) => {
            let result = perform(
              &mut conn,
              transaction_mapper,
              address_mapper,
              bundle_mapper,
              &message,
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
  }
}

fn perform(
  conn: &mut mysql::Conn,
  transaction_mapper: &TransactionMapper,
  address_mapper: &AddressMapper,
  bundle_mapper: &BundleMapper,
  message: &TransactionMessage,
  null_hash: &str,
) -> Result<
  (
    Option<ApproveMessage>,
    Option<SolidateMessage>,
    Option<CalculateMessage>,
  ),
> {
  let (mut approve_data, mut solidate_data, mut calculate_data) =
    (None, None, None);
  if !is_valid(message, null_hash) {
    return Ok((approve_data, solidate_data, calculate_data));
  }
  let mut hashes =
    vec![message.trunk_hash(), message.branch_hash(), message.hash()];
  hashes.dedup();
  let txs = transaction_mapper.fetch_many(conn, hashes)?;
  let mut txs = txs.iter().map(|tx| tx.lock().unwrap()).collect();
  let txs = unwrap_transactions(&mut txs, message);
  if let Some((mut current_tx, mut trunk_tx, mut branch_tx)) = txs {
    let timestamp = SystemTime::milliseconds_since_epoch()?;
    solidate_genesis(trunk_tx, null_hash);
    trunk_tx.direct_approve();
    transaction_mapper.set_trunk(current_tx, trunk_tx.id_tx());
    if let Some(ref mut branch_tx) = branch_tx {
      solidate_genesis(branch_tx, null_hash);
      branch_tx.direct_approve();
      transaction_mapper.set_branch(current_tx, branch_tx.id_tx());
    } else {
      transaction_mapper.set_branch(current_tx, trunk_tx.id_tx());
    }
    set_id_address(conn, address_mapper, message, current_tx)?;
    set_id_bundle(conn, bundle_mapper, message, current_tx, timestamp)?;
    set_solid(message, current_tx, trunk_tx, &branch_tx);
    set_height(message, current_tx, trunk_tx);
    current_tx.set_tag(message.tag().to_owned());
    current_tx.set_value(message.value());
    current_tx.set_timestamp(message.timestamp());
    current_tx.set_arrival(message.arrival());
    current_tx.set_current_idx(message.current_index());
    current_tx.set_last_idx(message.last_index());
    current_tx.set_is_mst(message.is_milestone());
    current_tx.set_mst_a(message.is_milestone());
    insert_events(conn, message, current_tx, timestamp)?;
    set_approve_data(&mut approve_data, current_tx);
    set_solidate_data(&mut solidate_data, current_tx);
    set_calculate_data(&mut calculate_data, current_tx);
    current_tx.insert(conn)?;
  }
  Ok((approve_data, solidate_data, calculate_data))
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
    tx.set_solid(Solid::Complete);
  }
}

fn set_id_address(
  conn: &mut mysql::Conn,
  address_mapper: &AddressMapper,
  message: &TransactionMessage,
  current_tx: &mut TransactionRecord,
) -> Result<()> {
  let address = address_mapper.fetch_by_hash(
    conn,
    message.address_hash(),
    |id_address| {
      AddressRecord::new(id_address, message.address_hash().to_owned())
    },
  )?;
  let mut address = address.lock().unwrap();
  if !address.is_persisted() {
    address.insert(conn)?;
  }
  current_tx.set_id_address(address.id_address());
  Ok(())
}

fn set_id_bundle(
  conn: &mut mysql::Conn,
  bundle_mapper: &BundleMapper,
  message: &TransactionMessage,
  current_tx: &mut TransactionRecord,
  timestamp: f64,
) -> Result<()> {
  let bundle = bundle_mapper.fetch_by_hash(
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
  let mut bundle = bundle.lock().unwrap();
  if !bundle.is_persisted() {
    bundle.insert(conn)?;
  }
  current_tx.set_id_bundle(bundle.id_bundle());
  Ok(())
}

fn set_solid(
  message: &TransactionMessage,
  current_tx: &mut TransactionRecord,
  trunk_tx: &TransactionRecord,
  branch_tx: &Option<&mut TransactionRecord>,
) {
  let mut solid = message.solid();
  let mut is_complete = trunk_tx.solid().is_complete();
  if is_complete {
    solid.solidate(Solidate::Trunk);
  }
  if let Some(ref branch_tx) = *branch_tx {
    is_complete = branch_tx.solid().is_complete();
  }
  if is_complete {
    solid.solidate(Solidate::Branch);
  }
  current_tx.set_solid(solid);
}

fn set_height(
  message: &TransactionMessage,
  current_tx: &mut TransactionRecord,
  trunk_tx: &TransactionRecord,
) {
  if !message.solid().is_complete() && trunk_tx.solid().is_complete() {
    current_tx.set_height(trunk_tx.height() + 1);
  } else {
    current_tx.set_height(0);
  }
}

fn insert_events(
  conn: &mut mysql::Conn,
  message: &TransactionMessage,
  current_tx: &mut TransactionRecord,
  timestamp: f64,
) -> Result<()> {
  event::new_transaction_received(conn, timestamp)?;
  if !current_tx.solid().is_complete() {
    event::unsolid_transaction(conn, timestamp)?;
  }
  if message.is_milestone() {
    event::milestone_received(conn, timestamp)?;
  }
  Ok(())
}

fn set_approve_data(
  approve_data: &mut Option<ApproveMessage>,
  current_tx: &TransactionRecord,
) {
  if current_tx.mst_a() {
    if let (Some(id_trunk), Some(id_branch)) =
      (current_tx.id_trunk(), current_tx.id_branch())
    {
      *approve_data = Some(ApproveMessage::Front(
        id_trunk,
        id_branch,
        current_tx.mst_timestamp(),
      ));
    }
  } else {
    *approve_data = Some(ApproveMessage::Reverse(current_tx.id_tx()));
  }
}

fn set_solidate_data(
  solidate_data: &mut Option<SolidateMessage>,
  current_tx: &TransactionRecord,
) {
  if current_tx.solid().is_complete() {
    *solidate_data = Some((current_tx.id_tx(), current_tx.height()));
  }
}

fn set_calculate_data(
  calculate_data: &mut Option<CalculateMessage>,
  current_tx: &TransactionRecord,
) {
  *calculate_data = Some(current_tx.id_tx());
}
