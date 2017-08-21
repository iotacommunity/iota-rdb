use super::Result;
use event;
use mapper::{BundleMapper, Mapper, Record, TransactionMapper,
             TransactionRecord};
use mysql;
use std::collections::{HashSet, VecDeque};
use std::sync::{mpsc, Arc};
use std::thread;
use std::time::{Instant, SystemTime};
use utils::{DurationUtils, SystemTimeUtils};

#[derive(Debug)]
pub enum ApproveMessage {
  Reverse(u64),
  Front(u64, u64, f64),
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
          ApproveMessage::Reverse(id) => {
            perform_reverse(&mut conn, transaction_mapper, bundle_mapper, id)
          }
          ApproveMessage::Front(id_trunk, id_branch, mst_timestamp) => {
            perform_front(
              &mut conn,
              transaction_mapper,
              bundle_mapper,
              id_trunk,
              id_branch,
              mst_timestamp,
            )
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

fn perform_reverse(
  conn: &mut mysql::Conn,
  transaction_mapper: &TransactionMapper,
  bundle_mapper: &BundleMapper,
  id: u64,
) -> Result<()> {
  let mut mst_timestamp = None;
  if let Some(index) = transaction_mapper.trunk_index(id) {
    if let Some(ref index) =
      *transaction_mapper.fetch_trunk(conn, id, &index)?
    {
      mst_timestamp = approved_child(conn, transaction_mapper, index)?;
    }
  }
  if mst_timestamp.is_none() {
    if let Some(index) = transaction_mapper.branch_index(id) {
      if let Some(ref index) =
        *transaction_mapper.fetch_branch(conn, id, &index)?
      {
        mst_timestamp = approved_child(conn, transaction_mapper, index)?;
      }
    }
  }
  if let Some(mst_timestamp) = mst_timestamp {
    let (id_trunk, id_branch) = {
      let transaction = transaction_mapper.fetch(conn, id)?;
      debug!("Mutex check at line {}", line!());
      let mut transaction = transaction.lock().unwrap();
      debug!("Mutex check at line {}", line!());
      approve(
        conn,
        bundle_mapper,
        &mut transaction,
        mst_timestamp,
        SystemTime::milliseconds_since_epoch()?,
      )?;
      (transaction.id_trunk(), transaction.id_branch())
    };
    if let (Some(id_trunk), Some(id_branch)) = (id_trunk, id_branch) {
      perform_front(
        conn,
        transaction_mapper,
        bundle_mapper,
        id_trunk,
        id_branch,
        mst_timestamp,
      )?;
    }
  }
  Ok(())
}

fn perform_front(
  conn: &mut mysql::Conn,
  transaction_mapper: &TransactionMapper,
  bundle_mapper: &BundleMapper,
  id_trunk: u64,
  id_branch: u64,
  mst_timestamp: f64,
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
    debug!("Mutex check at line {}", line!());
    let mut transaction = transaction.lock().unwrap();
    debug!("Mutex check at line {}", line!());
    if transaction.mst_a() || !transaction.is_persisted() {
      continue;
    }
    if let Some(id_trunk) = transaction.id_trunk() {
      nodes.push_front(id_trunk);
    }
    if let Some(id_branch) = transaction.id_branch() {
      nodes.push_front(id_branch);
    }
    approve(
      conn,
      bundle_mapper,
      &mut transaction,
      mst_timestamp,
      timestamp,
    )?;
    counter += 1;
  }
  if counter > 0 {
    event::subtangle_confirmation(conn, timestamp, counter)?;
  }
  Ok(())
}

fn approved_child(
  conn: &mut mysql::Conn,
  transaction_mapper: &TransactionMapper,
  index: &[u64],
) -> Result<Option<f64>> {
  for &id in index {
    let record = transaction_mapper.fetch(conn, id)?;
    debug!("Mutex check at line {}", line!());
    let record = record.lock().unwrap();
    debug!("Mutex check at line {}", line!());
    if record.mst_a() {
      return Ok(Some(record.mst_timestamp()));
    }
  }
  Ok(None)
}

fn approve(
  conn: &mut mysql::Conn,
  bundle_mapper: &BundleMapper,
  transaction: &mut TransactionRecord,
  mst_timestamp: f64,
  timestamp: f64,
) -> Result<()> {
  if transaction.current_idx() == 0 {
    let bundle = bundle_mapper.fetch(conn, transaction.id_bundle())?;
    debug!("Mutex check at line {}", line!());
    bundle.lock().unwrap().set_confirmed(timestamp);
    debug!("Mutex check at line {}", line!());
  }
  let timestamp = transaction.timestamp();
  transaction.set_conftime(mst_timestamp - timestamp);
  transaction.set_mst_a(true);
  Ok(())
}
