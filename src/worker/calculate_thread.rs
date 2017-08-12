use super::Result;
use mapper::{Mapper, TransactionMapper};
use mysql;
use std::collections::{HashSet, VecDeque};
use std::sync::{mpsc, Arc, Mutex};
use std::thread;
use std::time::Instant;
use utils::DurationUtils;

pub type CalculateMessage = u64;

pub struct CalculateThread<'a> {
  pub calculate_rx: mpsc::Receiver<CalculateMessage>,
  pub mysql_uri: &'a str,
  pub transaction_mapper: Arc<TransactionMapper>,
}

impl<'a> CalculateThread<'a> {
  pub fn spawn(self) {
    let Self {
      calculate_rx,
      mysql_uri,
      transaction_mapper,
    } = self;
    let mut conn =
      mysql::Conn::new(mysql_uri).expect("MySQL connection failure");
    thread::spawn(move || {
      let transaction_mapper = &*transaction_mapper;
      loop {
        let message =
          calculate_rx.recv().expect("Thread communication failure");
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

fn perform(
  conn: &mut mysql::Conn,
  transaction_mapper: &TransactionMapper,
  &pivot_id: &CalculateMessage,
) -> Result<()> {
  let weight = calculate_front(transaction_mapper, pivot_id)?;
  let mut parents = VecDeque::new();
  {
    let transaction = transaction_mapper.fetch(conn, pivot_id)?;
    let mut transaction = transaction.lock().unwrap();
    if let Some(id_trunk) = transaction.id_trunk() {
      parents.push_front(id_trunk);
    }
    if let Some(id_branch) = transaction.id_branch() {
      parents.push_front(id_branch);
    }
    transaction.add_weight(weight);
  }
  calculate_back(conn, transaction_mapper, pivot_id, weight + 1.0, parents)?;
  Ok(())
}

fn calculate_front(
  transaction_mapper: &TransactionMapper,
  pivot_id: u64,
) -> Result<f64> {
  let (mut nodes, mut visited) = (VecDeque::new(), HashSet::new());
  let mut weight = 0.0;
  nodes.push_front(pivot_id);
  while let Some(id) = nodes.pop_back() {
    if let Some(references) = transaction_mapper.trunk_references(id) {
      calculate_front_references(
        &mut nodes,
        &mut visited,
        &mut weight,
        &references,
        pivot_id,
      );
    }
    if let Some(references) = transaction_mapper.branch_references(id) {
      calculate_front_references(
        &mut nodes,
        &mut visited,
        &mut weight,
        &references,
        pivot_id,
      );
    }
  }
  Ok(weight)
}

fn calculate_front_references(
  nodes: &mut VecDeque<u64>,
  visited: &mut HashSet<u64>,
  weight: &mut f64,
  references: &Mutex<Vec<u64>>,
  pivot_id: u64,
) {
  for &id in &*references.lock().unwrap() {
    if id > pivot_id || !visited.insert(id) {
      continue;
    }
    nodes.push_front(id);
    *weight += 1.0;
  }
}

fn calculate_back(
  conn: &mut mysql::Conn,
  transaction_mapper: &TransactionMapper,
  pivot_id: u64,
  weight: f64,
  mut nodes: VecDeque<u64>,
) -> Result<()> {
  let mut visited = HashSet::new();
  while let Some(id) = nodes.pop_back() {
    if id > pivot_id || !visited.insert(id) {
      continue;
    }
    let transaction = transaction_mapper.fetch(conn, id)?;
    let mut transaction = transaction.lock().unwrap();
    if let Some(id_trunk) = transaction.id_trunk() {
      nodes.push_front(id_trunk);
    }
    if let Some(id_branch) = transaction.id_branch() {
      nodes.push_front(id_branch);
    }
    transaction.add_weight(weight);
  }
  Ok(())
}
