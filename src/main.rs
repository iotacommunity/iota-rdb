#![cfg_attr(feature = "clippy", feature(plugin))]
#![cfg_attr(feature = "clippy", plugin(clippy))]
#![feature(iterator_for_each)]

#[macro_use]
extern crate clap;
extern crate iota_kerl;
extern crate iota_sign;
extern crate iota_trytes;
extern crate log4rs;
#[macro_use]
extern crate log;
#[macro_use]
extern crate mysql;
extern crate zmq;

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
use utils::MysqlConnUtils;
use worker::{ApproveThread, CalculateThreads, InsertThread, SolidateThread,
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
    retry_interval,
    update_interval,
    calculation_threads,
    calculation_limit,
    generation_limit,
    milestone_address,
    milestone_start_index,
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

  let mut conn = mysql::Conn::new_retry(mysql_uri, retry_interval);
  let transaction_mapper = Arc::new(
    TransactionMapper::new(&mut conn, retry_interval)
      .expect("Transaction mapper failure"),
  );
  let address_mapper = Arc::new(
    AddressMapper::new(&mut conn, retry_interval)
      .expect("Address mapper failure"),
  );
  let bundle_mapper = Arc::new(
    BundleMapper::new(&mut conn, retry_interval)
      .expect("Bundle mapper failure"),
  );

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
    retry_interval,
    transaction_mapper: transaction_mapper.clone(),
    address_mapper: address_mapper.clone(),
    bundle_mapper: bundle_mapper.clone(),
    milestone_address,
    milestone_start_index,
  };
  let update_thread = UpdateThread {
    mysql_uri,
    retry_interval,
    update_interval,
    generation_limit,
    transaction_mapper: transaction_mapper.clone(),
    address_mapper: address_mapper.clone(),
    bundle_mapper: bundle_mapper.clone(),
  };
  let approve_thread = ApproveThread {
    approve_rx,
    mysql_uri,
    retry_interval,
    transaction_mapper: transaction_mapper.clone(),
    bundle_mapper: bundle_mapper.clone(),
  };
  let solidate_thread = SolidateThread {
    solidate_rx,
    mysql_uri,
    retry_interval,
    transaction_mapper: transaction_mapper.clone(),
  };
  let calculate_threads = CalculateThreads {
    calculate_rx,
    mysql_uri,
    retry_interval,
    calculation_threads,
    calculation_limit,
    transaction_mapper: transaction_mapper.clone(),
  };
  let zmq_loop = ZmqLoop { socket, insert_tx };

  insert_thread.spawn();
  update_thread.spawn();
  approve_thread.spawn();
  solidate_thread.spawn();
  calculate_threads.spawn();
  zmq_loop.run();
}
