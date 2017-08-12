use super::Result;
use event;
use mapper::{Mapper, TransactionMapper};
use mysql;
use solid::Solidate;
use std::collections::VecDeque;
use std::sync::{mpsc, Arc, Mutex};
use std::thread;
use std::time::{Instant, SystemTime};
use utils::{DurationUtils, SystemTimeUtils};

pub type SolidateMessage = (u64, i32);

pub struct SolidateThread<'a> {
  pub solidate_rx: mpsc::Receiver<SolidateMessage>,
  pub mysql_uri: &'a str,
  pub transaction_mapper: Arc<TransactionMapper>,
}

impl<'a> SolidateThread<'a> {
  pub fn spawn(self) {
    let Self {
      solidate_rx,
      mysql_uri,
      transaction_mapper,
    } = self;
    let mut conn =
      mysql::Conn::new(mysql_uri).expect("MySQL connection failure");
    thread::spawn(move || {
      let transaction_mapper = &*transaction_mapper;
      loop {
        let message = solidate_rx.recv().expect("Thread communication failure");
        let duration = Instant::now();
        let result = perform(&mut conn, transaction_mapper, &message);
        let duration = duration.elapsed().as_milliseconds();
        match result {
          Ok(()) => {
            info!("{}ms {:?}", duration, message);
          }
          Err(err) => {
            error!("{}ms {}", duration, err);
          }
        }
      }
    });
  }
}

pub fn perform(
  conn: &mut mysql::Conn,
  transaction_mapper: &TransactionMapper,
  &(id, height): &SolidateMessage,
) -> Result<()> {
  let (timestamp, mut counter) = (SystemTime::milliseconds_since_epoch()?, 0);
  let mut nodes = VecDeque::new();
  nodes.push_front((id, Some(height)));
  while let Some((id, height)) = nodes.pop_back() {
    counter += 1;
    if let Some(references) = transaction_mapper.trunk_references(id) {
      Solidate::Trunk
        .perform(conn, transaction_mapper, &mut nodes, references, height)?;
    }
    if let Some(references) = transaction_mapper.branch_references(id) {
      Solidate::Branch
        .perform(conn, transaction_mapper, &mut nodes, references, None)?;
    }
  }
  if counter > 1 {
    event::subtangle_solidation(conn, timestamp, counter - 1)?;
  }
  Ok(())
}

trait PerformSolidate {
  fn perform(
    self,
    conn: &mut mysql::Conn,
    transaction_mapper: &TransactionMapper,
    nodes: &mut VecDeque<(u64, Option<i32>)>,
    references: Arc<Mutex<Vec<u64>>>,
    height: Option<i32>,
  ) -> Result<()>;
}

impl PerformSolidate for Solidate {
  fn perform(
    self,
    conn: &mut mysql::Conn,
    transaction_mapper: &TransactionMapper,
    nodes: &mut VecDeque<(u64, Option<i32>)>,
    references: Arc<Mutex<Vec<u64>>>,
    height: Option<i32>,
  ) -> Result<()> {
    for &id in &*references.lock().unwrap() {
      let record = transaction_mapper.fetch(conn, id)?;
      let mut record = record.lock().unwrap();
      let mut solid = record.solid();
      if !solid.solidate(self) {
        continue;
      }
      record.set_solid(solid);
      if let Some(height) = height {
        record.set_height(height + 1);
      }
      if solid.is_complete() {
        let height = if record.height() > 0 {
          Some(record.height())
        } else {
          None
        };
        nodes.push_front((record.id_tx(), height));
      }
    }
    Ok(())
  }
}
