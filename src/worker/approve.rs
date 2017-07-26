use mapper::Mapper;
use mysql;
use std::sync::{Arc, Mutex, mpsc};
use std::thread;
use utils;
use worker::Result;

pub type ApproveVec = Vec<u64>;

pub struct Approve {
  pub approve_rx: Arc<Mutex<mpsc::Receiver<ApproveVec>>>,
}

impl Approve {
  pub fn spawn(self, pool: &mysql::Pool, thread_number: usize, verbose: bool) {
    let mut mapper = Mapper::new(pool).expect("MySQL mapper failure");
    thread::spawn(move || loop {
      self.perform(&mut mapper, thread_number, verbose);
    });
  }

  fn perform(&self, mapper: &mut Mapper, thread_number: usize, verbose: bool) {
    let vec = self
      .approve_rx
      .lock()
      .expect("Mutex is poisoned")
      .recv()
      .expect("Thread communication failure");
    match self.approve(mapper, vec.clone()) {
      Ok(()) => {
        if verbose {
          println!("approve_thread#{} {:?}", thread_number, vec);
        }
      }
      Err(err) => {
        eprintln!("Transaction approve error: {}", err);
      }
    }
  }

  fn approve(&self, mapper: &mut Mapper, mut nodes: ApproveVec) -> Result<()> {
    let (timestamp, mut counter) = (utils::milliseconds_since_epoch()?, 0);
    while let Some(id) = nodes.pop() {
      let record = mapper.select_transaction_by_id(id)?;
      if record.mst_a.unwrap_or(false) {
        continue;
      }
      let id_trunk = record.id_trunk.unwrap_or(0);
      let id_branch = record.id_branch.unwrap_or(0);
      if id_trunk != 0 {
        nodes.push(id_trunk);
      }
      if id_branch != 0 {
        nodes.push(id_branch);
      }
      if let Ok(0) = record.current_idx {
        if let Ok(id_bundle) = record.id_bundle {
          mapper.update_bundle(id_bundle, timestamp)?;
        }
      }
      mapper.approve_transaction(id)?;
      counter += 1;
    }
    if counter > 0 {
      mapper.subtanble_confirmation_event(timestamp, counter)?;
    }
    Ok(())
  }
}
