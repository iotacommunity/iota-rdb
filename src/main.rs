#![cfg_attr(feature="clippy", feature(plugin))]
#![cfg_attr(feature="clippy", plugin(clippy))]

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
mod counters;
mod query;
mod utils;

use args::Args;
use counters::Counters;
use std::process::exit;
use std::sync::mpsc;
use worker::{ApprovePool, MainLoop, SolidatePool};

fn main() {
  let matches = app::build().get_matches();
  let args = Args::parse(&matches).unwrap_or_else(|err| {
    eprintln!("Invalid arguments: {}", err);
    exit(1);
  });

  if args.verbose() {
    println!("Milestone address: {}", args.milestone_address());
    println!(
      "Milestone start index string: {}",
      args.milestone_start_index()
    );
  }

  let counters =
    Counters::new(args.mysql_uri()).expect("MySQL counters failure");
  let ctx = zmq::Context::new();
  let socket = ctx.socket(zmq::SUB).expect("ZMQ socket create failure");
  let (approve_tx, approve_rx) = mpsc::channel();
  let (solidate_tx, solidate_rx) = mpsc::channel();

  if args.verbose() {
    println!("Highest ids: {}", counters);
  }

  socket
    .connect(args.zmq_uri())
    .expect("ZMQ socket connect failure");
  socket.set_subscribe(b"tx ").expect("ZMQ subscribe failure");
  ApprovePool {
    approve_rx,
    mysql_uri: args.mysql_uri(),
  }.run(args.approve_threads_count(), args.verbose());
  SolidatePool {
    solidate_rx,
    mysql_uri: args.mysql_uri(),
  }.run(args.solidate_threads_count(), args.verbose());
  MainLoop {
    socket: &socket,
    approve_tx: &approve_tx,
    solidate_tx: &solidate_tx,
    mysql_uri: args.mysql_uri(),
    counters,
    milestone_address: args.milestone_address(),
    milestone_start_index: args.milestone_start_index(),
  }.run(args.verbose());
}
