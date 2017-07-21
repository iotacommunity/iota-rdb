#![cfg_attr(feature="clippy", feature(plugin))]
#![cfg_attr(feature="clippy", plugin(clippy))]

#[macro_use]
extern crate clap;
extern crate zmq;
#[macro_use]
extern crate mysql;

#[macro_use]
mod macros;
mod app;
mod args;
mod worker;
mod transaction;
mod counters;
mod mapper;
mod utils;

use args::Args;
use counters::Counters;
use std::process::exit;
use std::sync::{Arc, mpsc};
use worker::{ApprovePool, SolidatePool, WritePool, ZmqReader};

fn main() {
  let matches = app::build().get_matches();
  let args = Args::parse(&matches).unwrap_or_else(|err| {
    eprintln!("Invalid arguments: {}", err);
    exit(1);
  });

  let pool = mysql::Pool::new(args.mysql_uri()).expect("MySQL connect failure");
  let counters =
    Arc::new(Counters::new(&pool).expect("MySQL counters failure"));
  let ctx = zmq::Context::new();
  let socket = ctx.socket(zmq::SUB).expect("ZMQ socket create failure");
  let (write_tx, write_rx) = mpsc::channel();
  let (approve_tx, approve_rx) = mpsc::channel();
  let (solidate_tx, solidate_rx) = mpsc::channel();

  if args.verbose() {
    println!("{:?}", counters);
  }

  socket.connect(args.zmq_uri()).expect(
    "ZMQ socket connect failure",
  );
  socket.set_subscribe(b"tx ").expect("ZMQ subscribe failure");
  WritePool {
    rx: write_rx,
    approve_tx: &approve_tx,
    solidate_tx: &solidate_tx,
    pool: &pool,
    counters: counters,
    milestone_address: args.milestone_address(),
    milestone_start_index: args.milestone_start_index(),
  }.run(args.write_threads_count(), args.verbose());
  ApprovePool {
    rx: approve_rx,
    pool: &pool,
  }.run(
    args.approve_threads_count(),
    args.verbose(),
  );
  SolidatePool {
    rx: solidate_rx,
    pool: &pool,
  }.run(args.solidate_threads_count(), args.verbose());
  ZmqReader {
    socket: &socket,
    tx: &write_tx,
  }.run();
}
