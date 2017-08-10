use super::Result;
use event;
use mapper::{BundleMapper, Mapper, Record, TransactionMapper};
use mysql;
use std::collections::VecDeque;
use std::sync::{mpsc, Arc};
use std::thread;
use utils;

pub type ApproveMessage = (u64, Option<u64>);

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
        match perform(&mut conn, transaction_mapper, bundle_mapper, &message) {
          Ok(()) => {
            info!("{:?}", message);
          }
          Err(err) => {
            error!("{}", err);
          }
        }
      }
    });
  }
}

pub fn perform(
  conn: &mut mysql::Conn,
  transaction_mapper: &TransactionMapper,
  bundle_mapper: &BundleMapper,
  &(id_trunk, id_branch): &ApproveMessage,
) -> Result<()> {
  let (timestamp, mut counter) = (utils::milliseconds_since_epoch()?, 0);
  let mut nodes = VecDeque::new();
  nodes.push_front(id_trunk);
  if let Some(id_branch) = id_branch {
    nodes.push_front(id_branch);
  }
  while let Some(id) = nodes.pop_back() {
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
    if transaction.current_idx() == 0 {
      let bundle = bundle_mapper.fetch(conn, transaction.id_bundle())?;
      bundle.lock().unwrap().set_confirmed(timestamp);
    }
    transaction.approve();
    counter += 1;
  }
  if counter > 0 {
    event::subtangle_confirmation(conn, timestamp, counter)?;
  }
  Ok(())
}
