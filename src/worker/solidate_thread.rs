use super::Result;
use event;
use mapper::{Mapper, TransactionMapper};
use mysql;
use solid::Solidate;
use std::collections::{HashSet, VecDeque};
use std::sync::{mpsc, Arc};
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

fn perform(
  conn: &mut mysql::Conn,
  transaction_mapper: &TransactionMapper,
  &(pivot_id, height): &SolidateMessage,
) -> Result<()> {
  let (timestamp, mut counter) = (SystemTime::milliseconds_since_epoch()?, 0);
  let mut nodes = VecDeque::new();
  let mut branch_visited = HashSet::new();
  let mut trunk_visited = HashSet::new();
  nodes.push_front((pivot_id, Some(height)));
  while let Some((id, height)) = nodes.pop_back() {
    counter += 1;
    if let Some(index) = transaction_mapper.trunk_index(id) {
      if let Some(ref index) =
        *transaction_mapper.fetch_trunk(conn, id, &index)?
      {
        solidate(
          conn,
          transaction_mapper,
          &mut nodes,
          &mut trunk_visited,
          index,
          height,
          Solidate::Trunk,
        )?;
      }
    }
    if let Some(index) = transaction_mapper.branch_index(id) {
      if let Some(ref index) =
        *transaction_mapper.fetch_branch(conn, id, &index)?
      {
        solidate(
          conn,
          transaction_mapper,
          &mut nodes,
          &mut branch_visited,
          index,
          None,
          Solidate::Branch,
        )?;
      }
    }
  }
  if counter > 1 {
    event::subtangle_solidation(conn, timestamp, counter - 1)?;
  }
  Ok(())
}

fn solidate(
  conn: &mut mysql::Conn,
  transaction_mapper: &TransactionMapper,
  nodes: &mut VecDeque<(u64, Option<i32>)>,
  visited: &mut HashSet<u64>,
  index: &[u64],
  height: Option<i32>,
  solidate: Solidate,
) -> Result<()> {
  for &id in index {
    if !visited.insert(id) {
      continue;
    }
    let record = transaction_mapper.fetch(conn, id)?;
    debug!("Mutex check at line {}", line!());
    let mut record = record.lock().unwrap();
    debug!("Mutex check at line {}", line!());
    let mut solid = record.solid();
    if !solid.solidate(solidate) {
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
