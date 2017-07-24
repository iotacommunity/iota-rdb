use clap::{App, Arg};

const DEFAULT_MILESTONE_ADDRESS: &str = "KPWCHICGJZXKE9GSUDXZYUAPLHAKAHYHDXNPHENTERYMMBQOPSQIDENXKLKCEYCPVTZQLEEJVYJZV9BWU";
const DEFAULT_MILESTONE_START_INDEX: &str = "62000";

pub fn build<'a, 'b>() -> App<'a, 'b> {
  app_from_crate!()
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
      Arg::with_name("solidate_threads_count")
        .short("s")
        .long("solidate-threads")
        .takes_value(true)
        .value_name("COUNT")
        .default_value("1")
        .help("Count of solidity check worker threads"),
    )
    .arg(
      Arg::with_name("milestone_address")
        .short("M")
        .long("milestone-address")
        .takes_value(true)
        .value_name("ADDRESS")
        .default_value(DEFAULT_MILESTONE_ADDRESS)
        .help("Milestone address"),
    )
    .arg(
      Arg::with_name("milestone_start_index")
        .short("I")
        .long("milestone-start-index")
        .takes_value(true)
        .value_name("INDEX")
        .default_value(DEFAULT_MILESTONE_START_INDEX)
        .help("Milestone start index"),
    )
    .arg(
      Arg::with_name("VERBOSE")
        .short("v")
        .long("verbose")
        .help("Prints flowing messages"),
    )
}
