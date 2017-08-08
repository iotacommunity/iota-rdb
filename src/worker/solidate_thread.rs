use super::Result;
use mysql;
use query::{self, event, FindChildTransactionsResult};
use std::sync::mpsc;
use std::thread;
use utils;

pub type SolidateVec = Vec<(u64, Option<i32>)>;

pub struct SolidateThread<'a> {
  pub solidate_rx: mpsc::Receiver<SolidateVec>,
  pub mysql_uri: &'a str,
}

impl<'a> SolidateThread<'a> {
  pub fn spawn(self, verbose: bool) {
    let Self {
      solidate_rx,
      mysql_uri,
    } = self;
    let mut conn =
      mysql::Conn::new(mysql_uri).expect("MySQL connection failure");
    thread::spawn(move || loop {
      let vec = solidate_rx.recv().expect("Thread communication failure");
      match perform(&mut conn, vec.clone()) {
        Ok(()) => if verbose {
          println!("[sol] {:?}", vec);
        },
        Err(err) => {
          eprintln!("[sol] Error: {}", err);
        }
      }
    });
  }
}

pub fn perform(conn: &mut mysql::Conn, mut nodes: SolidateVec) -> Result<()> {
  let (timestamp, mut counter) = (utils::milliseconds_since_epoch()?, 0);
  while let Some((id, height)) = nodes.pop() {
    let (mut trunk, mut branch) = (Vec::new(), Vec::new());
    for record in query::find_child_transactions(conn, id)? {
      if record.id_trunk == id {
        trunk.push(record);
      } else if record.id_branch == id {
        branch.push(record);
      }
    }
    check_nodes(conn, &mut nodes, &mut trunk, height, 0b10)?;
    check_nodes(conn, &mut nodes, &mut branch, None, 0b01)?;
    counter += trunk.len() as i32;
    counter += branch.len() as i32;
  }
  if counter > 0 {
    event::subtangle_solidation(conn, timestamp, counter)?;
  }
  Ok(())
}

fn check_nodes(
  conn: &mut mysql::Conn,
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
        query::solidate_transaction_trunk(
          conn,
          record.id_tx,
          record.height,
          record.solid,
        )?;
      }
      None => {
        query::solidate_transaction_branch(conn, record.id_tx, record.solid)?;
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
