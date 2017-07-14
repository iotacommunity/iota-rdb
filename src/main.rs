#![cfg_attr(feature="clippy", feature(plugin))]
#![cfg_attr(feature="clippy", plugin(clippy))]

#[macro_use]
extern crate mysql;
extern crate zmq;

mod transaction;
mod mapper;
#[macro_use]
mod macros;

use mapper::Mapper;
use std::sync::{Arc, Mutex, mpsc};
use std::thread;
use transaction::Transaction;

const WRITE_THREADS_COUNT: usize = 4;
const APPROVE_THREADS_COUNT: usize = 4;
const ZMQ_URI: &str = "tcp://88.99.93.196:5556";
const MYSQL_URI: &str = "mysql://root:password@127.0.0.1:\
                         3306/iota?prefer_socket=false";

fn main() {
  let pool = mysql::Pool::new(MYSQL_URI).unwrap();
  let ctx = zmq::Context::new();
  let socket = ctx.socket(zmq::SUB).unwrap();
  let (write_tx, write_rx) = mpsc::channel::<String>();
  let (approve_tx, approve_rx) = mpsc::channel::<Vec<u64>>();
  socket.connect(ZMQ_URI).unwrap();
  socket.set_subscribe(b"tx ").unwrap();
  spawn_write_workers(&pool, write_rx, &approve_tx);
  spawn_approve_workers(&pool, approve_rx);
  zmq_listen(&socket, &write_tx);
}

fn zmq_listen(socket: &zmq::Socket, tx: &mpsc::Sender<String>) {
  loop {
    match socket.recv_string(0) {
      Ok(Ok(string)) => tx.send(string).expect("Thread communication failure"),
      Ok(Err(err)) => eprintln!("Unexpected byte sequence: {:?}", err),
      Err(err) => eprintln!("{}", err),
    }
  }
}

fn spawn_write_workers(
  pool: &mysql::Pool,
  rx: mpsc::Receiver<String>,
  tx: &mpsc::Sender<Vec<u64>>,
) {
  let rx = Arc::new(Mutex::new(rx));
  for i in 0..WRITE_THREADS_COUNT {
    let (tx, rx) = (tx.clone(), rx.clone());
    let mut mapper = Mapper::new(pool).expect("MySQL mapper failure");
    thread::spawn(move || loop {
      let rx = rx.lock().expect("Mutex is poisoned");
      let string = rx.recv().expect("Thread communication failure");
      match Transaction::parse(&string) {
        Ok(transaction) => {
          match transaction.process(&mut mapper) {
            Ok(Some(vec)) => {
              tx.send(vec).expect("Thread communication failure")
            }
            Ok(None) => println!("write_thread#{} {:?}", i, transaction),
            Err(err) => eprintln!("Transaction processing error: {}", err),
          }
        }
        Err(err) => eprintln!("Transaction parsing error: {}", err),
      }
    });
  }
}

fn spawn_approve_workers(pool: &mysql::Pool, rx: mpsc::Receiver<Vec<u64>>) {
  let rx = Arc::new(Mutex::new(rx));
  for i in 0..APPROVE_THREADS_COUNT {
    let rx = rx.clone();
    let mut mapper = Mapper::new(pool).expect("MySQL mapper failure");
    thread::spawn(move || loop {
      let rx = rx.lock().expect("Mutex is poisoned");
      let vec = rx.recv().expect("Thread communication failure");
      match Transaction::approve(&mut mapper, vec.clone()) {
        Ok(()) => println!("approve_thread#{} {:?}", i, vec),
        Err(err) => eprintln!("Transaction approve error: {}", err),
      }
    });
  }
}
