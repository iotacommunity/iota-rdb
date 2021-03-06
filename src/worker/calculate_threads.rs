use super::Result;
use mapper::{Index, Mapper, TransactionMapper};
use mysql;
use std::collections::{HashSet, VecDeque};
use std::sync::{mpsc, Arc, Mutex, MutexGuard};
use std::thread;
use std::time::Instant;
use utils::{DurationUtils, MysqlConnUtils};

#[derive(Debug)]
pub struct CalculateJob {
  pivot_id: u64,
}

pub struct CalculateThreads<'a> {
  pub calculate_rx: mpsc::Receiver<CalculateJob>,
  pub mysql_uri: &'a str,
  pub retry_interval: u64,
  pub calculation_threads: usize,
  pub calculation_limit: usize,
  pub transaction_mapper: Arc<TransactionMapper>,
}

impl<'a> CalculateThreads<'a> {
  pub fn spawn(self) {
    let Self {
      calculate_rx,
      mysql_uri,
      retry_interval,
      calculation_threads,
      calculation_limit,
      transaction_mapper,
    } = self;
    let calculate_rx = Arc::new(Mutex::new(calculate_rx));
    for i in 0..calculation_threads {
      let mut conn = mysql::Conn::new_retry(mysql_uri, retry_interval);
      let transaction_mapper = transaction_mapper.clone();
      let calculate_rx = calculate_rx.clone();
      let thread = thread::Builder::new().name(format!("calculate#{}", i));
      let thread = thread.spawn(move || {
        let transaction_mapper = &*transaction_mapper;
        let calculate_rx = &*calculate_rx;
        loop {
          let job = {
            debug!("Mutex lock");
            let rx = calculate_rx.lock().unwrap();
            debug!("Mutex acquire");
            rx.recv().expect("Thread communication failure")
          };
          let duration = Instant::now();
          let result =
            job.perform(&mut conn, transaction_mapper, calculation_limit);
          let duration = duration.elapsed().as_milliseconds();
          match result {
            Ok(()) => {
              info!("#{} {:.3}ms {:?}", i, duration, job);
            }
            Err(err) => {
              error!("#{} {:.3}ms {}", i, duration, err);
            }
          }
        }
      });
      thread.expect("Thread spawn failure");
    }
  }
}

impl CalculateJob {
  pub fn new(pivot_id: u64) -> Self {
    Self { pivot_id }
  }

  pub fn perform(
    &self,
    conn: &mut mysql::Conn,
    transaction_mapper: &TransactionMapper,
    calculation_limit: usize,
  ) -> Result<()> {
    let weight = calculate_front(conn, transaction_mapper, self.pivot_id)?;
    calculate_back(
      conn,
      transaction_mapper,
      calculation_limit,
      self.pivot_id,
      weight,
    )?;
    Ok(())
  }
}

fn calculate_front(
  conn: &mut mysql::Conn,
  transaction_mapper: &TransactionMapper,
  pivot_id: u64,
) -> Result<f64> {
  let (mut nodes, mut visited) = (VecDeque::new(), HashSet::new());
  let mut weight = 0.0;
  nodes.push_front(pivot_id);
  while let Some(id) = nodes.pop_back() {
    if let Some(index) = transaction_mapper.trunk_index(id) {
      let (index, _) = transaction_mapper.fetch_trunk(conn, id, &index)?;
      calculate_front_refs(
        &mut nodes,
        &mut visited,
        &mut weight,
        &index,
        pivot_id,
      );
    }
    if let Some(index) = transaction_mapper.branch_index(id) {
      let (index, _) = transaction_mapper.fetch_branch(conn, id, &index)?;
      calculate_front_refs(
        &mut nodes,
        &mut visited,
        &mut weight,
        &index,
        pivot_id,
      );
    }
  }
  Ok(weight)
}

fn calculate_front_refs(
  nodes: &mut VecDeque<u64>,
  visited: &mut HashSet<u64>,
  weight: &mut f64,
  index: &MutexGuard<Index>,
  pivot_id: u64,
) {
  if let Some(ref index) = **index {
    for &id in index {
      if id > pivot_id || !visited.insert(id) {
        continue;
      }
      nodes.push_front(id);
      *weight += 1.0;
    }
  }
}

fn calculate_back(
  conn: &mut mysql::Conn,
  transaction_mapper: &TransactionMapper,
  calculation_limit: usize,
  pivot_id: u64,
  mut weight: f64,
) -> Result<()> {
  let (mut nodes, mut visited) = (VecDeque::new(), HashSet::new());
  nodes.push_front(pivot_id);
  while let Some(id) = nodes.pop_back() {
    if visited.len() > calculation_limit {
      return Ok(());
    }
    if id > pivot_id || !visited.insert(id) {
      continue;
    }
    let transaction = transaction_mapper.fetch(conn, id, None)?;
    debug!("Mutex lock");
    let mut transaction = transaction.lock().unwrap();
    debug!("Mutex acquire");
    if let Some(id_trunk) = transaction.id_trunk() {
      nodes.push_front(id_trunk);
    }
    if let Some(id_branch) = transaction.id_branch() {
      nodes.push_front(id_branch);
    }
    transaction.add_weight(weight);
    if id == pivot_id {
      weight += 1.0;
    }
  }
  Ok(())
}
