use super::Result;
use mapper::{Mapper, TransactionMapper};
use mysql;
use std::collections::VecDeque;
use std::sync::{mpsc, Arc};
use std::thread;
use std::time::Instant;
use utils::DurationUtils;

pub type CalculateMessage = u64;

pub struct CalculateThread<'a> {
  pub calculate_rx: mpsc::Receiver<CalculateMessage>,
  pub mysql_uri: &'a str,
  pub transaction_mapper: Arc<TransactionMapper>,
}

impl<'a> CalculateThread<'a> {
  pub fn spawn(self) {
    let Self {
      calculate_rx,
      mysql_uri,
      transaction_mapper,
    } = self;
    let mut conn =
      mysql::Conn::new(mysql_uri).expect("MySQL connection failure");
    thread::spawn(move || {
      let transaction_mapper = &*transaction_mapper;
      loop {
        let message =
          calculate_rx.recv().expect("Thread communication failure");
        let duration = Instant::now();
        let result = perform(&mut conn, transaction_mapper, &message);
        let duration = duration.elapsed().as_milliseconds();
        match result {
          Ok(()) => {
            info!("{}ms {:?}", duration, message);
          }
          Err(err) => {
            error!("{}ms {}", duration, err);
          }
        }
      }
    });
  }
}

pub fn perform(
  conn: &mut mysql::Conn,
  transaction_mapper: &TransactionMapper,
  &id: &CalculateMessage,
) -> Result<()> {
  let mut nodes = VecDeque::new();
  nodes.push_front(id);
  while let Some(id) = nodes.pop_back() {
    let _record = transaction_mapper.fetch(conn, id)?;
    // TODO
  }
  Ok(())
}
