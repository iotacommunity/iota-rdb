use counters::Counters;
use std::sync::mpsc;
use transaction::Transaction;
use worker::{ApproveVec, SolidateVec, Write};
use zmq;

pub struct MainLoop<'a> {
  pub socket: &'a zmq::Socket,
  pub approve_tx: &'a mpsc::Sender<ApproveVec>,
  pub solidate_tx: &'a mpsc::Sender<SolidateVec>,
  pub mysql_uri: &'a str,
  pub counters: Counters,
  pub milestone_address: &'a str,
  pub milestone_start_index: &'a str,
}

impl<'a> MainLoop<'a> {
  pub fn run(self, verbose: bool) {
    let mut worker = Write::new(self.mysql_uri, self.counters)
      .expect("Worker initialization failure");
    loop {
      match self.socket.recv_string(0) {
        Ok(Ok(string)) => {
          if verbose {
            println!("[zmq] {}", string);
          }
          match Transaction::new(
            &string,
            self.milestone_address,
            self.milestone_start_index,
          ) {
            Ok(transaction) => {
              match worker.perform(&transaction) {
                Ok((approve_data, solidate_data)) => {
                  if verbose {
                    println!("[rdb] {}", transaction.hash());
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
                  eprintln!("[rdb] Processing error: {}", err);
                }
              }
            }
            Err(err) => {
              eprintln!("[rdb] Parsing error: {}", err);
            }
          }
        }
        Ok(Err(err)) => {
          eprintln!("[zmq] Unexpected byte sequence: {:?}", err);
        }
        Err(err) => {
          eprintln!("{}", err);
        }
      }
    }
  }
}
