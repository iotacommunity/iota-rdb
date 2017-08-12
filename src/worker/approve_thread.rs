use super::Result;
use event;
use mapper::{BundleMapper, Mapper, Record, TransactionMapper,
             TransactionRecord};
use mysql;
use std::collections::{HashSet, VecDeque};
use std::sync::{mpsc, Arc, Mutex};
use std::thread;
use std::time::{Instant, SystemTime};
use utils::{DurationUtils, SystemTimeUtils};

#[derive(Debug)]
pub enum ApproveMessage {
  Front(u64, u64),
  Reverse(u64),
}

pub struct ApproveThread<'a> {
  pub approve_rx: mpsc::Receiver<ApproveMessage>,
  pub mysql_uri: &'a str,
  pub transaction_mapper: Arc<TransactionMapper>,
  pub bundle_mapper: Arc<BundleMapper>,
}

impl<'a> ApproveThread<'a> {
  pub fn spawn(self) {
    let Self {
      approve_rx,
      mysql_uri,
      transaction_mapper,
      bundle_mapper,
    } = self;
    let mut conn =
      mysql::Conn::new(mysql_uri).expect("MySQL connection failure");
    thread::spawn(move || {
      let transaction_mapper = &*transaction_mapper;
      let bundle_mapper = &*bundle_mapper;
      loop {
        let message = approve_rx.recv().expect("Thread communication failure");
        let duration = Instant::now();
        let result = match message {
          ApproveMessage::Front(id_trunk, id_branch) => perform_front(
            &mut conn,
            transaction_mapper,
            bundle_mapper,
            id_trunk,
            id_branch,
          ),
          ApproveMessage::Reverse(id) => {
            perform_reverse(&mut conn, transaction_mapper, bundle_mapper, id)
          }
        };
        let duration = duration.elapsed().as_milliseconds();
        match result {
          Ok(()) => {
            info!("{:.3}ms {:?}", duration, message);
          }
          Err(err) => {
            error!("{:.3}ms {}", duration, err);
          }
        }
      }
    });
  }
}

fn perform_front(
  conn: &mut mysql::Conn,
  transaction_mapper: &TransactionMapper,
  bundle_mapper: &BundleMapper,
  id_trunk: u64,
  id_branch: u64,
) -> Result<()> {
  let (timestamp, mut counter) = (SystemTime::milliseconds_since_epoch()?, 0);
  let (mut nodes, mut visited) = (VecDeque::new(), HashSet::new());
  nodes.push_front(id_trunk);
  if id_branch != id_trunk {
    nodes.push_front(id_branch);
  }
  while let Some(id) = nodes.pop_back() {
    if !visited.insert(id) {
      continue;
    }
    let transaction = transaction_mapper.fetch(conn, id)?;
    let mut transaction = transaction.lock().unwrap();
    if transaction.mst_a() || !transaction.is_persisted() {
      return Ok(());
    }
    if let Some(id_trunk) = transaction.id_trunk() {
      nodes.push_front(id_trunk);
    }
    if let Some(id_branch) = transaction.id_branch() {
      nodes.push_front(id_branch);
    }
    approve(conn, bundle_mapper, &mut transaction, timestamp)?;
    counter += 1;
  }
  if counter > 0 {
    event::subtangle_confirmation(conn, timestamp, counter)?;
  }
  Ok(())
}

fn perform_reverse(
  conn: &mut mysql::Conn,
  transaction_mapper: &TransactionMapper,
  bundle_mapper: &BundleMapper,
  id: u64,
) -> Result<()> {
  let (timestamp, mut is_approved) =
    (SystemTime::milliseconds_since_epoch()?, false);
  if let Some(references) = transaction_mapper.trunk_references(id) {
    is_approved = is_approved ||
      is_reference_approved(conn, transaction_mapper, &references)?
  }
  if let Some(references) = transaction_mapper.branch_references(id) {
    is_approved = is_approved ||
      is_reference_approved(conn, transaction_mapper, &references)?
  }
  if is_approved {
    let transaction = transaction_mapper.fetch(conn, id)?;
    let mut transaction = transaction.lock().unwrap();
    approve(conn, bundle_mapper, &mut transaction, timestamp)?;
  }
  Ok(())
}

fn is_reference_approved(
  conn: &mut mysql::Conn,
  transaction_mapper: &TransactionMapper,
  references: &Mutex<Vec<u64>>,
) -> Result<bool> {
  for &id in &*references.lock().unwrap() {
    let record = transaction_mapper.fetch(conn, id)?;
    let record = record.lock().unwrap();
    if record.mst_a() {
      return Ok(true);
    }
  }
  Ok(false)
}

fn approve(
  conn: &mut mysql::Conn,
  bundle_mapper: &BundleMapper,
  transaction: &mut TransactionRecord,
  timestamp: f64,
) -> Result<()> {
  if transaction.current_idx() == 0 {
    let bundle = bundle_mapper.fetch(conn, transaction.id_bundle())?;
    bundle.lock().unwrap().set_confirmed(timestamp);
  }
  transaction.set_mst_a(true);
  Ok(())
}
