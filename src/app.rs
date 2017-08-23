use clap::{App, Arg};

const DEFAULT_RETRY_INTERVAL: &str = "1000";
const DEFAULT_UPDATE_INTERVAL: &str = "1000";
const DEFAULT_CALCULATION_THREADS: &str = "1";
const DEFAULT_CALCULATION_LIMIT: &str = "1000";
const DEFAULT_GENERATION_LIMIT: &str = "10";
const DEFAULT_MILESTONE_ADDRESS: &str =
  "KPWCHICGJZXKE9GSUDXZYUAPLHAKAHYHDXNPHENTE\
   RYMMBQOPSQIDENXKLKCEYCPVTZQLEEJVYJZV9BWU";
const DEFAULT_MILESTONE_START_INDEX: &str = "62000";
const DEFAULT_LOG_CONFIG: &str = "log4rs.yaml";

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
      Arg::with_name("retry_interval")
        .short("r")
        .long("retry-interval")
        .takes_value(true)
        .value_name("INTERVAL")
        .default_value(DEFAULT_RETRY_INTERVAL)
        .help("MySQL connect retry interval in milliseconds"),
    )
    .arg(
      Arg::with_name("update_interval")
        .short("u")
        .long("update-interval")
        .takes_value(true)
        .value_name("INTERVAL")
        .default_value(DEFAULT_UPDATE_INTERVAL)
        .help("MySQL update interval in milliseconds"),
    )
    .arg(
      Arg::with_name("calculation_threads")
        .short("T")
        .long("calculation-threads")
        .takes_value(true)
        .value_name("THREADS")
        .default_value(DEFAULT_CALCULATION_THREADS)
        .help("Number of calculation threads"),
    )
    .arg(
      Arg::with_name("calculation_limit")
        .short("t")
        .long("calculation-limit")
        .takes_value(true)
        .value_name("LIMIT")
        .default_value(DEFAULT_CALCULATION_LIMIT)
        .help("Calculation depth limit"),
    )
    .arg(
      Arg::with_name("generation_limit")
        .short("g")
        .long("generation-limit")
        .takes_value(true)
        .value_name("LIMIT")
        .default_value(DEFAULT_GENERATION_LIMIT)
        .help("Garbage collector generation limit"),
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
      Arg::with_name("log_config")
        .short("C")
        .long("log-config")
        .takes_value(true)
        .value_name("FILE")
        .default_value(DEFAULT_LOG_CONFIG)
        .help("Path to log4rs configuration file"),
    )
}
