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
mod counters;
mod query;
mod utils;

use args::Args;
use counters::Counters;
use std::process::exit;
use std::sync::mpsc;
use worker::{ApproveThread, SolidateThread, WriteThread, ZmqLoop};

fn main() {
  let matches = app::build().get_matches();
  let Args {
    zmq_uri,
    mysql_uri,
    milestone_address,
    milestone_start_index,
    verbose,
  } = Args::parse(&matches).unwrap_or_else(|err| {
    eprintln!("Invalid arguments: {}", err);
    exit(1);
  });

  let counters = Counters::new(mysql_uri).expect("MySQL counters failure");
  let ctx = zmq::Context::new();
  let socket = ctx.socket(zmq::SUB).expect("ZMQ socket create failure");
  let (write_tx, write_rx) = mpsc::channel();
  let (approve_tx, approve_rx) = mpsc::channel();
  let (solidate_tx, solidate_rx) = mpsc::channel();

  if verbose {
    println!("Milestone address: {}", milestone_address);
    println!("Milestone start index string: {}", milestone_start_index);
    println!("Highest ids: {}", counters);
  }

  socket.connect(zmq_uri).expect("ZMQ socket connect failure");
  socket.set_subscribe(b"tx ").expect("ZMQ subscribe failure");

  let write_thread = WriteThread {
    write_rx,
    approve_tx,
    solidate_tx,
    mysql_uri,
    counters,
    milestone_address,
    milestone_start_index,
  };
  let approve_thread = ApproveThread {
    approve_rx,
    mysql_uri,
  };
  let solidate_thread = SolidateThread {
    solidate_rx,
    mysql_uri,
  };
  let zmq_loop = ZmqLoop { socket, write_tx };

  write_thread.spawn(verbose);
  approve_thread.spawn(verbose);
  solidate_thread.spawn(verbose);
  zmq_loop.run(verbose);
}
