use counters::Counters;
use mapper::{Mapper, NewTransaction};
use mysql;
use std::sync::{Arc, Mutex, mpsc};
use std::thread;
use transaction::Transaction;
use utils;
use worker::{ApproveVec, Result, SolidateVec};

const NULL_HASH: &str = "999999999999999999999999999999999999999999999999999999999999999999999999999999999";

pub struct Write {
  pub write_rx: Arc<Mutex<mpsc::Receiver<String>>>,
  pub approve_tx: mpsc::Sender<ApproveVec>,
  pub solidate_tx: mpsc::Sender<SolidateVec>,
  pub counters: Arc<Counters>,
  pub milestone_address: String,
  pub milestone_start_index: String,
}

impl Write {
  pub fn spawn(self, pool: &mysql::Pool, thread_number: usize, verbose: bool) {
    let mut mapper = Mapper::new(pool).expect("MySQL mapper failure");
    thread::spawn(move || loop {
      self.perform(&mut mapper, thread_number, verbose);
    });
  }

  fn perform(&self, mapper: &mut Mapper, thread_number: usize, verbose: bool) {
    let message = self
      .write_rx
      .lock()
      .expect("Mutex is poisoned")
      .recv()
      .expect("Thread communication failure");
    match Transaction::new(
      &message,
      &self.milestone_address,
      &self.milestone_start_index,
    ) {
      Ok(mut transaction) => {
        match self.write(&mut transaction, mapper, &self.counters) {
          Ok((approve_data, solidate_data)) => {
            if verbose {
              println!("write_thread#{} {:?}", thread_number, transaction);
            }
            if let Some(approve_data) = approve_data {
              self
                .approve_tx
                .send(approve_data)
                .expect("Thread communication failure");
            }
            if let Some(solidate_data) = solidate_data {
              self
                .solidate_tx
                .send(solidate_data)
                .expect("Thread communication failure");
            }
          }
          Err(err) => {
            eprintln!("Transaction processing error: {}", err);
          }
        }
      }
      Err(err) => {
        eprintln!("Transaction parsing error: {}", err);
      }
    }
  }

  fn write(
    &self,
    transaction: &mut Transaction,
    mapper: &mut Mapper,
    counters: &Counters,
  ) -> Result<(Option<ApproveVec>, Option<SolidateVec>)> {
    let result = mapper.select_transaction_by_hash(transaction.hash())?;
    let id_tx = if let Some(record) = result {
      if record.id_trunk.unwrap_or(0) != 0 &&
        record.id_branch.unwrap_or(0) != 0
      {
        return Ok((None, None));
      }
      Some(record.id_tx?)
    } else {
      None
    };
    let timestamp = utils::milliseconds_since_epoch()?;
    let (id_trunk, trunk_height, trunk_solid) =
      self.check_node(mapper, counters, transaction.trunk_hash())?;
    let (id_branch, _, branch_solid) = self
      .check_node(mapper, counters, transaction.branch_hash())?;
    let id_address =
      mapper.fetch_address(counters, transaction.address_hash())?;
    let id_bundle = mapper.fetch_bundle(
      counters,
      timestamp,
      transaction.bundle_hash(),
      transaction.last_index(),
    )?;
    let height = if transaction.solid() != 0b11 && trunk_solid == 0b11 {
      trunk_height + 1
    } else {
      0
    };
    if trunk_solid == 0b11 {
      transaction.solidate(0b10);
    }
    if branch_solid == 0b11 {
      transaction.solidate(0b01);
    }
    let record = NewTransaction {
      hash: transaction.hash(),
      tag: transaction.tag(),
      value: transaction.value(),
      timestamp: transaction.timestamp(),
      current_idx: transaction.current_index(),
      last_idx: transaction.last_index(),
      is_mst: transaction.is_milestone(),
      mst_a: transaction.is_milestone(),
      solid: transaction.solid(),
      id_trunk,
      id_branch,
      id_address,
      id_bundle,
      height,
    };
    if id_tx.is_none() {
      mapper.insert_transaction(counters, record)?;
    } else {
      mapper.update_transaction(record)?;
    }
    if transaction.solid() != 0b11 {
      mapper.unsolid_transaction_event(timestamp)?;
    }
    mapper.new_transaction_received_event(timestamp)?;
    let approve_data = if transaction.is_milestone() {
      mapper.milestone_received_event(timestamp)?;
      Some(vec![id_trunk, id_branch])
    } else {
      None
    };
    let solidate_data =
      id_tx.and_then(|id_tx| if transaction.solid() == 0b11 {
        Some(vec![(id_tx, Some(height))])
      } else {
        None
      });
    Ok((approve_data, solidate_data))
  }

  fn check_node(
    &self,
    mapper: &mut Mapper,
    counters: &Counters,
    hash: &str,
  ) -> Result<(u64, i32, u8)> {
    match mapper.select_transaction_by_hash(hash)? {
      Some(record) => {
        let id_tx = record.id_tx?;
        mapper.direct_approve_transaction(id_tx)?;
        Ok((id_tx, record.height?, record.solid?))
      }
      None => {
        let (height, solid) = (0, if hash == NULL_HASH { 0b11 } else { 0b00 });
        let id_tx = mapper
          .insert_transaction_placeholder(counters, hash, height, solid)?;
        Ok((id_tx, height, solid))
      }
    }
  }
}
