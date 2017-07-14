#![cfg_attr(feature="clippy", feature(plugin))]
#![cfg_attr(feature="clippy", plugin(clippy))]

#[macro_use]
extern crate clap;
extern crate zmq;
#[macro_use]
extern crate mysql;

mod transaction;
mod mapper;
#[macro_use]
mod macros;

use clap::Arg;
use mapper::Mapper;
use std::sync::{Arc, Mutex, mpsc};
use std::thread;
use transaction::Transaction;

fn main() {
  let matches = app_from_crate!()
    .arg(
      Arg::with_name("ZMQ_URI")
        .short("z")
        .long("zmq")
        .takes_value(true)
        .value_name("URI")
        .required(true)
        .help("ZMQ source server URI"),
    )
    .arg(
      Arg::with_name("MYSQL_URI")
        .short("m")
        .long("mysql")
        .takes_value(true)
        .value_name("URI")
        .required(true)
        .help("MySQL destination server URI"),
    )
    .arg(
      Arg::with_name("WRITE_THREADS_COUNT")
        .short("w")
        .long("write-threads")
        .takes_value(true)
        .value_name("COUNT")
        .default_value("1")
        .help("Count of regular write worker threads"),
    )
    .arg(
      Arg::with_name("APPROVE_THREADS_COUNT")
        .short("a")
        .long("approve-threads")
        .takes_value(true)
        .value_name("COUNT")
        .default_value("1")
        .help("Count of milestone approval worker threads"),
    )
    .get_matches();
  let zmq_uri = matches.value_of("ZMQ_URI").expect(
    "ZMQ_URI were not provided",
  );
  let mysql_uri = matches.value_of("MYSQL_URI").expect(
    "MYSQL_URI were not provided",
  );
  let write_threads_count: usize =
    matches
      .value_of("WRITE_THREADS_COUNT")
      .expect("WRITE_THREADS_COUNT were not provided")
      .parse()
      .expect("WRITE_THREADS_COUNT not a number");
  let approve_threads_count: usize =
    matches
      .value_of("APPROVE_THREADS_COUNT")
      .expect("APPROVE_THREADS_COUNT were not provided")
      .parse()
      .expect("APPROVE_THREADS_COUNT not a number");

  let pool = mysql::Pool::new(mysql_uri).expect("MySQL connect failure");
  let ctx = zmq::Context::new();
  let socket = ctx.socket(zmq::SUB).expect("ZMQ socket create failure");
  let (write_tx, write_rx) = mpsc::channel::<String>();
  let (approve_tx, approve_rx) = mpsc::channel::<Vec<u64>>();
  socket.connect(zmq_uri).expect("ZMQ socket connect failure");
  socket.set_subscribe(b"tx ").expect("ZMQ subscribe failure");
  spawn_write_workers(&pool, write_rx, &approve_tx, write_threads_count);
  spawn_approve_workers(&pool, approve_rx, approve_threads_count);
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
  threads_count: usize,
) {
  let rx = Arc::new(Mutex::new(rx));
  for i in 0..threads_count {
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

fn spawn_approve_workers(
  pool: &mysql::Pool,
  rx: mpsc::Receiver<Vec<u64>>,
  threads_count: usize,
) {
  let rx = Arc::new(Mutex::new(rx));
  for i in 0..threads_count {
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
