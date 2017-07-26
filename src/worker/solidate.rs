use mysql;
use query::{FindChildTransactions, FindChildTransactionsResult, InsertEvent,
            SolidateTransaction};
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
    while let Some((id, height)) = nodes.pop() {
      let (mut trunk, mut branch) = (Vec::new(), Vec::new());
      for record in self.find_child_transactions_query.exec(id)? {
        if record.id_trunk == id {
          trunk.push(record);
        } else if record.id_branch == id {
          branch.push(record);
        }
      }
      self.check_nodes(&mut nodes, &mut trunk, height, 0b10)?;
      self.check_nodes(&mut nodes, &mut branch, None, 0b01)?;
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

  fn check_nodes(
    &mut self,
    nodes: &mut SolidateVec,
    children: &mut [FindChildTransactionsResult],
    height: Option<i32>,
    solid: u8,
  ) -> Result<()> {
    for record in children {
      if record.solid & solid != 0b00 {
        continue;
      }
      record.solid |= solid;
      match height {
        Some(height) => {
          record.height = height + 1;
          self
            .solidate_transaction_query
            .trunk(record.id_tx, record.height, record.solid)?;
        }
        None => {
          self
            .solidate_transaction_query
            .branch(record.id_tx, record.solid)?;
        }
      }
      if record.solid == 0b11 {
        let height = if record.height > 0 {
          Some(record.height)
        } else {
          None
        };
        nodes.push((record.id_tx, height));
      }
    }
    Ok(())
  }
}
