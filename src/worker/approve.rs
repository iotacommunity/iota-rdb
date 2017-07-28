use mysql;
use query::{self, event};
use utils;
use worker::Result;

pub type ApproveVec = Vec<u64>;

pub struct Approve {
  conn: mysql::Conn,
}

impl Approve {
  pub fn new(mysql_uri: &str) -> Result<Self> {
    let conn = mysql::Conn::new(mysql_uri)?;
    Ok(Self { conn })
  }

  pub fn perform(&mut self, mut nodes: ApproveVec) -> Result<()> {
    let (timestamp, mut counter) = (utils::milliseconds_since_epoch()?, 0);
    nodes.dedup();
    while let Some(id) = nodes.pop() {
      let record = query::find_transaction(&mut self.conn, id)?;
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
          query::update_bundle(&mut self.conn, id_bundle, timestamp)?;
        }
      }
      query::approve_transaction(&mut self.conn, id)?;
      counter += 1;
    }
    if counter > 0 {
      event::subtanble_confirmation(&mut self.conn, timestamp, counter)?;
    }
    Ok(())
  }
}
