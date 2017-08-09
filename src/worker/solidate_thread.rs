use super::Result;
use event;
use mapper::{Mapper, TransactionMapper};
use mysql;
use std::sync::{mpsc, Arc, Mutex};
use std::thread;
use utils;

pub type SolidateVec = Vec<(u64, Option<i32>)>;

pub struct SolidateThread<'a> {
  pub solidate_rx: mpsc::Receiver<SolidateVec>,
  pub mysql_uri: &'a str,
  pub transaction_mapper: Arc<TransactionMapper>,
}

impl<'a> SolidateThread<'a> {
  pub fn spawn(self, verbose: bool) {
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
        let vec = solidate_rx.recv().expect("Thread communication failure");
        match perform(&mut conn, transaction_mapper, vec.clone()) {
          Ok(()) => if verbose {
            println!("[sol] {:?}", vec);
          },
          Err(err) => {
            eprintln!("[sol] Error: {}", err);
          }
        }
      }
    });
  }
}

pub fn perform(
  conn: &mut mysql::Conn,
  transaction_mapper: &TransactionMapper,
  mut nodes: SolidateVec,
) -> Result<()> {
  let (timestamp, mut counter) = (utils::milliseconds_since_epoch()?, 0);
  while let Some((id, height)) = nodes.pop() {
    counter += 1;
    if let Some(ids) = transaction_mapper.trunk_references(id) {
      solidate_nodes(conn, transaction_mapper, ids, &mut nodes, height, 0b10)?;
    }
    if let Some(ids) = transaction_mapper.branch_references(id) {
      solidate_nodes(conn, transaction_mapper, ids, &mut nodes, None, 0b01)?;
    }
  }
  if counter > 1 {
    event::subtangle_solidation(conn, timestamp, counter - 1)?;
  }
  Ok(())
}

fn solidate_nodes(
  conn: &mut mysql::Conn,
  transaction_mapper: &TransactionMapper,
  ids: Arc<Mutex<Vec<u64>>>,
  nodes: &mut SolidateVec,
  height: Option<i32>,
  solidate: u8,
) -> Result<()> {
  for &id in &*ids.lock().unwrap() {
    let record = transaction_mapper.fetch(conn, id)?;
    let mut record = record.lock().unwrap();
    let solid = record.solid();
    if solid & solidate != 0b00 {
      continue;
    }
    record.set_solid(solid | solidate);
    if let Some(height) = height {
      record.set_height(height + 1);
    }
    if record.solid() == 0b11 {
      let height = if record.height() > 0 {
        Some(record.height())
      } else {
        None
      };
      nodes.push((record.id_tx(), height));
    }
  }
  Ok(())
}
