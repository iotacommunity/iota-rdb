#![cfg_attr(feature="clippy", feature(plugin))]
#![cfg_attr(feature="clippy", plugin(clippy))]

#[macro_use]
extern crate clap;
extern crate zmq;
#[macro_use]
extern crate mysql;

#[macro_use]
mod macros;
mod worker;
mod transaction;
mod counters;
mod mapper;
mod utils;

use clap::Arg;
use counters::Counters;
use std::sync::{Arc, mpsc};
use worker::{ApprovePool, WritePool, ZmqReader};

const DEFAULT_MILESTONE_ADDRESS: &str = "KPWCHICGJZXKE9GSUDXZYUAPLHAKAHYHDXNPHENTERYMMBQOPSQIDENXKLKCEYCPVTZQLEEJVYJZV9BWU";
const DEFAULT_MILESTONE_START_INDEX: &str = "HADC99999999999999999999999";

fn main() {
  let matches = app_from_crate!()
    .arg(
      Arg::with_name("zmq_uri")
        .short("z")
        .long("zmq")
        .takes_value(true)
        .value_name("URI")
        .required(true)
        .help("ZMQ source server URI"),
    )
    .arg(
      Arg::with_name("mysql_uri")
        .short("m")
        .long("mysql")
        .takes_value(true)
        .value_name("URI")
        .required(true)
        .help("MySQL destination server URI"),
    )
    .arg(
      Arg::with_name("write_threads_count")
        .short("w")
        .long("write-threads")
        .takes_value(true)
        .value_name("COUNT")
        .default_value("1")
        .help("Count of regular write worker threads"),
    )
    .arg(
      Arg::with_name("approve_threads_count")
        .short("a")
        .long("approve-threads")
        .takes_value(true)
        .value_name("COUNT")
        .default_value("1")
        .help("Count of milestone approval worker threads"),
    )
    .arg(
      Arg::with_name("milestone_address")
        .short("s")
        .long("milestone-address")
        .takes_value(true)
        .value_name("ADDRESS")
        .default_value(DEFAULT_MILESTONE_ADDRESS)
        .help("Milestone address"),
    )
    .arg(
      Arg::with_name("milestone_start_index")
        .short("i")
        .long("milestone-start-index")
        .takes_value(true)
        .value_name("INDEX")
        .default_value(DEFAULT_MILESTONE_START_INDEX)
        .help("Milestone start index"),
    )
    .arg(Arg::with_name("VERBOSE").short("v").long("verbose").help(
      "Prints flowing messages",
    ))
    .get_matches();
  let zmq_uri = matches.value_of("zmq_uri").expect(
    "ZMQ URI were not provided",
  );
  let mysql_uri = matches.value_of("mysql_uri").expect(
    "MYSQL URI were not provided",
  );
  let write_threads_count: usize = matches
    .value_of("write_threads_count")
    .expect("write-threads were not provided")
    .parse()
    .expect("write-threads not a number");
  let approve_threads_count: usize = matches
    .value_of("approve_threads_count")
    .expect("approve-threads were not provided")
    .parse()
    .expect("approve-threads not a number");
  let milestone_address = matches.value_of("milestone_address").expect(
    "milestone-address were not provided",
  );
  let milestone_start_index =
    matches.value_of("milestone_start_index").expect(
      "milestone-start-index were not provided",
    );
  let verbose = matches.is_present("VERBOSE");

  let pool = mysql::Pool::new(mysql_uri).expect("MySQL connect failure");
  let counters =
    Arc::new(Counters::new(&pool).expect("MySQL counters failure"));
  let ctx = zmq::Context::new();
  let socket = ctx.socket(zmq::SUB).expect("ZMQ socket create failure");
  let (write_tx, write_rx) = mpsc::channel::<String>();
  let (approve_tx, approve_rx) = mpsc::channel::<Vec<u64>>();

  if verbose {
    println!("{:?}", counters);
  }

  socket.connect(zmq_uri).expect("ZMQ socket connect failure");
  socket.set_subscribe(b"tx ").expect("ZMQ subscribe failure");
  WritePool {
    rx: write_rx,
    approve_tx: &approve_tx,
    pool: &pool,
    counters: counters,
    milestone_address: milestone_address,
    milestone_start_index: milestone_start_index,
  }.run(write_threads_count, verbose);
  ApprovePool {
    rx: approve_rx,
    pool: &pool,
  }.run(
    approve_threads_count,
    verbose,
  );
  ZmqReader {
    socket: &socket,
    tx: &write_tx,
  }.run();
}
