use super::Result;
use mapper::{BundleMapper, Mapper, TransactionMapper};
use mysql;
use query::event;
use record::Record;
use std::collections::VecDeque;
use std::sync::{mpsc, Arc};
use std::thread;
use utils;

pub type ApproveVec = VecDeque<u64>;

pub struct ApproveThread<'a> {
  pub approve_rx: mpsc::Receiver<ApproveVec>,
  pub mysql_uri: &'a str,
  pub transaction_mapper: Arc<TransactionMapper>,
  pub bundle_mapper: Arc<BundleMapper>,
}

impl<'a> ApproveThread<'a> {
  pub fn spawn(self, verbose: bool) {
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
        let vec = approve_rx.recv().expect("Thread communication failure");
        match perform(
          &mut conn,
          transaction_mapper,
          bundle_mapper,
          vec.clone(),
        ) {
          Ok(()) => if verbose {
            println!("[apv] {:?}", vec);
          },
          Err(err) => {
            eprintln!("[apv] Error: {}", err);
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
  mut nodes: ApproveVec,
) -> Result<()> {
  let (timestamp, mut counter) = (utils::milliseconds_since_epoch()?, 0);
  while let Some(id) = nodes.pop_back() {
    // TODO catch Error::Locked
    let mut guard = transaction_mapper.lock();
    let mut transaction = transaction_mapper.fetch(&mut guard, conn, id)?;
    if transaction.mst_a() || !transaction.is_persistent() {
      return Ok(());
    }
    if transaction.id_trunk() != 0 {
      nodes.push_front(transaction.id_trunk());
    }
    if transaction.id_branch() != 0 {
      nodes.push_front(transaction.id_branch());
    }
    if transaction.current_idx() == 0 {
      let mut guard = bundle_mapper.lock();
      let mut bundle = bundle_mapper
        .fetch(&mut guard, conn, transaction.id_bundle())?;
      bundle.set_confirmed(timestamp);
    }
    transaction.approve();
    counter += 1;
  }
  if counter > 0 {
    event::subtangle_confirmation(conn, timestamp, counter)?;
  }
  Ok(())
}
