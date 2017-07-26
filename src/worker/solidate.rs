use mapper::Mapper;
use utils;
use worker::Result;

pub type SolidateVec = Vec<(u64, Option<i32>)>;

pub struct Solidate<'a> {
  mapper: Mapper<'a>,
}

impl<'a> Solidate<'a> {
  pub fn new(mapper: Mapper<'a>) -> Self {
    Self { mapper }
  }

  pub fn perform(&mut self, mut nodes: SolidateVec) -> Result<()> {
    let (timestamp, mut counter) = (utils::milliseconds_since_epoch()?, 0);
    while let Some((parent_id, parent_height)) = nodes.pop() {
      let (mut trunk, mut branch) = (Vec::new(), Vec::new());
      for record in self.mapper.select_child_transactions(parent_id)? {
        if record.id_trunk? == parent_id {
          trunk.push((record.id_tx?, record.height?, record.solid?));
        } else if record.id_branch? == parent_id {
          branch.push((record.id_tx?, record.height?, record.solid?));
        }
      }
      self
        .solidate_nodes(&mut nodes, &trunk, 0b10, parent_height)?;
      self.solidate_nodes(&mut nodes, &branch, 0b01, None)?;
      counter += trunk.len() as i32;
      counter += branch.len() as i32;
    }
    if counter > 0 {
      self.mapper.subtangle_solidation_event(timestamp, counter)?;
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
            .mapper
            .solidate_trunk_transaction(id, node_height, node_solid)?;
        }
        None => {
          self.mapper.solidate_branch_transaction(id, node_solid)?;
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
