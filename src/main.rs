#![cfg_attr(feature = "clippy", feature(plugin))]
#![cfg_attr(feature = "clippy", plugin(clippy))]

#[macro_use]
extern crate log;
extern crate log4rs;
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
mod message;
mod mapper;
mod solid;
mod event;
mod utils;

use args::Args;
use mapper::{AddressMapper, BundleMapper, Mapper, TransactionMapper};
use std::process::exit;
use std::sync::{mpsc, Arc};
use worker::{ApproveThread, CalculateThread, InsertThread, SolidateThread,
             UpdateThread, ZmqLoop};

fn main() {
  let matches = app::build().get_matches();
  let args = Args::parse(&matches).unwrap_or_else(|err| {
    eprintln!("Invalid arguments: {}", err);
    exit(1);
  });
  let Args {
    zmq_uri,
    mysql_uri,
    update_interval,
    milestone_address,
    milestone_start_index,
    calculation_limit,
    generation_limit,
    log_config,
  } = args;
  log4rs::init_file(log_config, Default::default()).unwrap_or_else(|err| {
    eprintln!("Error while processing logger configuration file: {}", err);
    exit(1);
  });

  let (insert_tx, insert_rx) = mpsc::channel();
  let (approve_tx, approve_rx) = mpsc::channel();
  let (solidate_tx, solidate_rx) = mpsc::channel();
  let (calculate_tx, calculate_rx) = mpsc::channel();
  let ctx = zmq::Context::new();
  let socket = ctx.socket(zmq::SUB).expect("ZMQ socket create failure");
  socket.connect(zmq_uri).expect("ZMQ socket connect failure");
  socket.set_subscribe(b"tx ").expect("ZMQ subscribe failure");

  let mut conn = mysql::Conn::new(mysql_uri).expect("MySQL connection failure");
  let transaction_mapper = Arc::new(
    TransactionMapper::new(&mut conn).expect("Transaction mapper failure"),
  );
  let address_mapper = Arc::new(
    AddressMapper::new(&mut conn).expect("Address mapper failure"),
  );
  let bundle_mapper =
    Arc::new(BundleMapper::new(&mut conn).expect("Bundle mapper failure"));

  info!("Milestone address: {}", milestone_address);
  info!("Milestone start index string: {}", milestone_start_index);
  info!("Initial `id_tx`: {}", transaction_mapper.current_id());
  info!("Initial `id_address`: {}", address_mapper.current_id());
  info!("Initial `id_bundle`: {}", bundle_mapper.current_id());

  let insert_thread = InsertThread {
    insert_rx,
    approve_tx,
    solidate_tx,
    calculate_tx,
    mysql_uri,
    transaction_mapper: transaction_mapper.clone(),
    address_mapper: address_mapper.clone(),
    bundle_mapper: bundle_mapper.clone(),
    milestone_address,
    milestone_start_index,
  };
  let update_thread = UpdateThread {
    mysql_uri,
    update_interval,
    generation_limit,
    transaction_mapper: transaction_mapper.clone(),
    address_mapper: address_mapper.clone(),
    bundle_mapper: bundle_mapper.clone(),
  };
  let approve_thread = ApproveThread {
    approve_rx,
    mysql_uri,
    transaction_mapper: transaction_mapper.clone(),
    bundle_mapper: bundle_mapper.clone(),
  };
  let solidate_thread = SolidateThread {
    solidate_rx,
    mysql_uri,
    transaction_mapper: transaction_mapper.clone(),
  };
  let calculate_thread = CalculateThread {
    calculate_rx,
    mysql_uri,
    calculation_limit,
    transaction_mapper: transaction_mapper.clone(),
  };
  let zmq_loop = ZmqLoop { socket, insert_tx };

  insert_thread.spawn();
  update_thread.spawn();
  approve_thread.spawn();
  solidate_thread.spawn();
  calculate_thread.spawn();
  zmq_loop.run();
}
