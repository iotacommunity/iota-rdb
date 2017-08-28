use super::Result;
use event;
use mapper::{Index, Mapper, TransactionMapper};
use mysql;
use solid::Solidate;
use std::collections::{HashSet, VecDeque};
use std::sync::{mpsc, Arc, MutexGuard};
use std::thread;
use std::time::{Instant, SystemTime};
use utils::{DurationUtils, MysqlConnUtils, SystemTimeUtils};

#[derive(Debug)]
pub struct SolidateJob {
  pivot_id: u64,
  height: i32,
}

pub struct SolidateThread<'a> {
  pub solidate_rx: mpsc::Receiver<SolidateJob>,
  pub mysql_uri: &'a str,
  pub retry_interval: u64,
  pub transaction_mapper: Arc<TransactionMapper>,
}

struct SolidateTask<'a> {
  nodes: &'a mut VecDeque<(u64, Option<i32>)>,
  visited: &'a mut HashSet<u64>,
  index: MutexGuard<'a, Index>,
  skip_index: Option<(usize, u64)>,
}

impl<'a> SolidateThread<'a> {
  pub fn spawn(self) {
    let Self {
      solidate_rx,
      mysql_uri,
      retry_interval,
      transaction_mapper,
    } = self;
    let mut conn = mysql::Conn::new_retry(mysql_uri, retry_interval);
    let thread = thread::Builder::new().name("solidate".into());
    let thread = thread.spawn(move || {
      let transaction_mapper = &*transaction_mapper;
      loop {
        let job = solidate_rx.recv().expect("Thread communication failure");
        let duration = Instant::now();
        let result = job.perform(&mut conn, transaction_mapper);
        let duration = duration.elapsed().as_milliseconds();
        match result {
          Ok(()) => {
            info!("{:.3}ms {:?}", duration, job);
          }
          Err(err) => {
            error!("{:.3}ms {}", duration, err);
          }
        }
      }
    });
    thread.expect("Thread spawn failure");
  }
}

impl SolidateJob {
  pub fn new(pivot_id: u64, height: i32) -> Self {
    Self { pivot_id, height }
  }

  pub fn perform(
    &self,
    conn: &mut mysql::Conn,
    transaction_mapper: &TransactionMapper,
  ) -> Result<()> {
    let (timestamp, mut counter) = (SystemTime::milliseconds_since_epoch()?, 0);
    let mut nodes = VecDeque::new();
    let mut branch_visited = HashSet::new();
    let mut trunk_visited = HashSet::new();
    nodes.push_front((self.pivot_id, Some(self.height)));
    while let Some((id, height)) = nodes.pop_back() {
      counter += 1;
      if let Some(index) = transaction_mapper.trunk_index(id) {
        let (index, skip_index) =
          transaction_mapper.fetch_trunk(conn, id, &index)?;
        SolidateTask {
          nodes: &mut nodes,
          visited: &mut trunk_visited,
          index,
          skip_index,
        }.solidate(conn, transaction_mapper, height, Solidate::Trunk)?;
      }
      if let Some(index) = transaction_mapper.branch_index(id) {
        let (index, skip_index) =
          transaction_mapper.fetch_branch(conn, id, &index)?;
        SolidateTask {
          nodes: &mut nodes,
          visited: &mut branch_visited,
          index,
          skip_index,
        }.solidate(conn, transaction_mapper, None, Solidate::Branch)?;
      }
    }
    if counter > 1 {
      event::subtangle_solidation(conn, timestamp, counter - 1)?;
    }
    Ok(())
  }
}

impl<'a> SolidateTask<'a> {
  fn solidate(
    &mut self,
    conn: &mut mysql::Conn,
    transaction_mapper: &TransactionMapper,
    height: Option<i32>,
    solidate: Solidate,
  ) -> Result<()> {
    if let Some(ref index) = *self.index {
      for &id in index {
        if !self.visited.insert(id) {
          continue;
        }
        let record = transaction_mapper.fetch(conn, id, self.skip_index)?;
        debug!("Mutex lock");
        let mut record = record.lock().unwrap();
        debug!("Mutex acquire");
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
          self.nodes.push_front((record.id_tx(), height));
        }
      }
    }
    Ok(())
  }
}
