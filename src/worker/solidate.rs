use mapper::Mapper;
use mysql;
use std::sync::{Arc, Mutex, mpsc};
use std::thread;
use utils;
use worker::Result;

pub type SolidateVec = Vec<(u64, Option<i32>)>;

pub struct Solidate {
  pub solidate_rx: Arc<Mutex<mpsc::Receiver<SolidateVec>>>,
}

impl Solidate {
  pub fn spawn(self, pool: &mysql::Pool, thread_number: usize, verbose: bool) {
    let mut mapper = Mapper::new(pool).expect("MySQL mapper failure");
    thread::spawn(move || loop {
      self.perform(&mut mapper, thread_number, verbose);
    });
  }

  fn perform(&self, mapper: &mut Mapper, thread_number: usize, verbose: bool) {
    let vec = self
      .solidate_rx
      .lock()
      .expect("Mutex is poisoned")
      .recv()
      .expect("Thread communication failure");
    match self.solidate(mapper, vec.clone()) {
      Ok(()) => {
        if verbose {
          println!("solidate_thread#{} {:?}", thread_number, vec);
        }
      }
      Err(err) => {
        eprintln!("Transaction solidity check error: {}", err);
      }
    }
  }

  fn solidate(
    &self,
    mapper: &mut Mapper,
    mut nodes: SolidateVec,
  ) -> Result<()> {
    let (timestamp, mut counter) = (utils::milliseconds_since_epoch()?, 0);
    while let Some((parent_id, parent_height)) = nodes.pop() {
      let (mut trunk, mut branch) = (Vec::new(), Vec::new());
      for record in mapper.select_child_transactions(parent_id)? {
        if record.id_trunk? == parent_id {
          trunk.push((record.id_tx?, record.height?, record.solid?));
        } else if record.id_branch? == parent_id {
          branch.push((record.id_tx?, record.height?, record.solid?));
        }
      }
      self
        .solidate_nodes(mapper, &mut nodes, &trunk, 0b10, parent_height)?;
      self
        .solidate_nodes(mapper, &mut nodes, &branch, 0b01, None)?;
      counter += trunk.len() as i32;
      counter += branch.len() as i32;
    }
    if counter > 0 {
      mapper.subtangle_solidation_event(timestamp, counter)?;
    }
    Ok(())
  }

  fn solidate_nodes(
    &self,
    mapper: &mut Mapper,
    nodes: &mut SolidateVec,
    ids: &[(u64, i32, u8)],
    solid: u8,
    height: Option<i32>,
  ) -> Result<()> {
    for &(id, mut node_height, mut node_solid) in ids {
      if node_solid & solid != 0b00 {
        continue;
      }
      node_solid |= solid;
      match height {
        Some(height) => {
          node_height = height + 1;
          mapper
            .solidate_trunk_transaction(id, node_height, node_solid)?;
        }
        None => {
          mapper.solidate_branch_transaction(id, node_solid)?;
        }
      }
      if node_solid == 0b11 {
        let node_height = if node_height > 0 {
          Some(node_height)
        } else {
          None
        };
        nodes.push((id, node_height));
      }
    }
    Ok(())
  }
}
