#![cfg_attr(feature = "clippy", feature(plugin))]
#![cfg_attr(feature = "clippy", plugin(clippy))]

#[macro_use]
extern crate clap;
extern crate zmq;
#[macro_use]
extern crate mysql;
extern crate iota_trytes;
extern crate iota_sign;
extern crate iota_curl_cpu;

#[macro_use]
mod macros;
mod app;
mod args;
mod worker;
mod transaction;
mod counter;
mod mapper;
mod query;
mod utils;

use args::Args;
use counter::Counter;
use mapper::{AddressMapper, BundleMapper, TransactionMapper};
use std::process::exit;
use std::sync::{mpsc, Arc};
use worker::{ApproveThread, InsertThread, SolidateThread, UpdateThread,
             ZmqLoop};

fn main() {
  let matches = app::build().get_matches();
  let args = Args::parse(&matches).unwrap_or_else(|err| {
    eprintln!("Invalid arguments: {}", err);
    exit(1);
  });
  let Args {
    zmq_uri,
    mysql_uri,
    milestone_address,
    milestone_start_index,
    verbose,
  } = args;

  let (insert_tx, insert_rx) = mpsc::channel();
  let (approve_tx, approve_rx) = mpsc::channel();
  let (solidate_tx, solidate_rx) = mpsc::channel();
  let counter = Arc::new(Counter::new(mysql_uri).expect("Counter failure"));
  let transaction_mapper = Arc::new(
    TransactionMapper::new(counter.clone())
      .expect("Transaction mapper failure"),
  );
  let address_mapper = Arc::new(
    AddressMapper::new(counter.clone()).expect("Address mapper failure"),
  );
  let bundle_mapper = Arc::new(
    BundleMapper::new(counter.clone()).expect("Bundle mapper failure"),
  );
  let ctx = zmq::Context::new();
  let socket = ctx.socket(zmq::SUB).expect("ZMQ socket create failure");
  socket.connect(zmq_uri).expect("ZMQ socket connect failure");
  socket.set_subscribe(b"tx ").expect("ZMQ subscribe failure");

  if verbose {
    println!("Milestone address: {}", milestone_address);
    println!("Milestone start index string: {}", milestone_start_index);
    println!("Highest ids: {}", counter);
  }

  let insert_thread = InsertThread {
    insert_rx,
    approve_tx,
    solidate_tx,
    mysql_uri,
    transaction_mapper: transaction_mapper.clone(),
    address_mapper: address_mapper.clone(),
    bundle_mapper: bundle_mapper.clone(),
    milestone_address,
    milestone_start_index,
  };
  let update_thread = UpdateThread {
    mysql_uri,
    transaction_mapper: transaction_mapper.clone(),
    address_mapper: address_mapper.clone(),
    bundle_mapper: bundle_mapper.clone(),
  };
  let approve_thread = ApproveThread {
    approve_rx,
    mysql_uri,
  };
  let solidate_thread = SolidateThread {
    solidate_rx,
    mysql_uri,
  };
  let zmq_loop = ZmqLoop { socket, insert_tx };

  insert_thread.spawn(verbose);
  update_thread.spawn(verbose);
  approve_thread.spawn(verbose);
  solidate_thread.spawn(verbose);
  zmq_loop.run(verbose);
}
