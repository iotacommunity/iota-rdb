use mysql;
use query::{FindChildTransactions, InsertEvent, SolidateTransaction};
use utils;
use worker::Result;

pub type SolidateVec = Vec<(u64, Option<i32>)>;

pub struct Solidate<'a> {
  find_child_transactions_query: FindChildTransactions<'a>,
  solidate_transaction_query: SolidateTransaction<'a>,
  insert_event_query: InsertEvent<'a>,
}

impl<'a> Solidate<'a> {
  pub fn new(pool: &mysql::Pool) -> Result<Self> {
    Ok(Self {
      find_child_transactions_query: FindChildTransactions::new(pool)?,
      solidate_transaction_query: SolidateTransaction::new(pool)?,
      insert_event_query: InsertEvent::new(pool)?,
    })
  }

  pub fn perform(&mut self, mut nodes: SolidateVec) -> Result<()> {
    let (timestamp, mut counter) = (utils::milliseconds_since_epoch()?, 0);
    while let Some((parent_id, parent_height)) = nodes.pop() {
      let (mut trunk, mut branch) = (Vec::new(), Vec::new());
      for record in self.find_child_transactions_query.exec(parent_id)? {
        if record.id_trunk == parent_id {
          trunk.push((record.id_tx, record.height, record.solid));
        } else if record.id_branch == parent_id {
          branch.push((record.id_tx, record.height, record.solid));
        }
      }
      self
        .solidate_nodes(&mut nodes, &trunk, 0b10, parent_height)?;
      self.solidate_nodes(&mut nodes, &branch, 0b01, None)?;
      counter += trunk.len() as i32;
      counter += branch.len() as i32;
    }
    if counter > 0 {
      self
        .insert_event_query
        .subtangle_solidation(timestamp, counter)?;
    }
    Ok(())
  }

  fn solidate_nodes(
    &mut self,
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
          self
            .solidate_transaction_query
            .trunk(id, node_height, node_solid)?;
        }
        None => {
          self.solidate_transaction_query.branch(id, node_solid)?;
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
