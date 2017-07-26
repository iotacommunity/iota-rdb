use mapper::Mapper;
use utils;
use worker::Result;

pub type ApproveVec = Vec<u64>;

pub struct Approve<'a> {
  mapper: Mapper<'a>,
}

impl<'a> Approve<'a> {
  pub fn new(mapper: Mapper<'a>) -> Self {
    Self { mapper }
  }

  pub fn perform(&mut self, mut nodes: ApproveVec) -> Result<()> {
    let (timestamp, mut counter) = (utils::milliseconds_since_epoch()?, 0);
    while let Some(id) = nodes.pop() {
      let record = self.mapper.select_transaction_by_id(id)?;
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
          self.mapper.update_bundle(id_bundle, timestamp)?;
        }
      }
      self.mapper.approve_transaction(id)?;
      counter += 1;
    }
    if counter > 0 {
      self
        .mapper
        .subtanble_confirmation_event(timestamp, counter)?;
    }
    Ok(())
  }
}
