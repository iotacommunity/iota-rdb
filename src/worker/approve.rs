use mysql;
use query::{ApproveTransaction, FindTransaction, InsertEvent, UpdateBundle};
use utils;
use worker::Result;

pub type ApproveVec = Vec<u64>;

pub struct Approve<'a> {
  find_transaction_query: FindTransaction<'a>,
  approve_transaction_query: ApproveTransaction<'a>,
  update_bundle_query: UpdateBundle<'a>,
  insert_event_query: InsertEvent<'a>,
}

impl<'a> Approve<'a> {
  pub fn new(pool: &mysql::Pool) -> Result<Self> {
    Ok(Self {
      find_transaction_query: FindTransaction::new(pool)?,
      approve_transaction_query: ApproveTransaction::new(pool)?,
      update_bundle_query: UpdateBundle::new(pool)?,
      insert_event_query: InsertEvent::new(pool)?,
    })
  }

  pub fn perform(&mut self, mut nodes: ApproveVec) -> Result<()> {
    let (timestamp, mut counter) = (utils::milliseconds_since_epoch()?, 0);
    while let Some(id) = nodes.pop() {
      let record = self.find_transaction_query.exec(id)?;
      if record.mst_a {
        continue;
      }
      if record.id_trunk != 0 {
        nodes.push(record.id_trunk);
      }
      if record.id_branch != 0 {
        nodes.push(record.id_branch);
      }
      if let Some(0) = record.current_idx {
        if let Some(id_bundle) = record.id_bundle {
          self.update_bundle_query.exec(id_bundle, timestamp)?;
        }
      }
      self.approve_transaction_query.exec(id)?;
      counter += 1;
    }
    if counter > 0 {
      self
        .insert_event_query
        .subtanble_confirmation(timestamp, counter)?;
    }
    Ok(())
  }
}
