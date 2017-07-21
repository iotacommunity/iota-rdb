use clap::ArgMatches;

pub struct Args<'a> {
  zmq_uri: &'a str,
  mysql_uri: &'a str,
  write_threads_count: usize,
  approve_threads_count: usize,
  milestone_address: &'a str,
  milestone_start_index: &'a str,
  verbose: bool,
}

impl<'a> Args<'a> {
  pub fn parse(matches: &'a ArgMatches<'a>) -> Self {
    Self {
      zmq_uri: matches.value_of("zmq_uri").expect(
        "ZMQ URI were not provided",
      ),
      mysql_uri: matches.value_of("mysql_uri").expect(
        "MYSQL URI were not provided",
      ),
      write_threads_count: matches
        .value_of("write_threads_count")
        .expect("write-threads were not provided")
        .parse()
        .expect("write-threads not a number"),
      approve_threads_count: matches
        .value_of("approve_threads_count")
        .expect("approve-threads were not provided")
        .parse()
        .expect("approve-threads not a number"),
      milestone_address: matches.value_of("milestone_address").expect(
        "milestone-address were not provided",
      ),
      milestone_start_index: matches.value_of("milestone_start_index").expect(
        "milestone-start-index were not provided",
      ),
      verbose: matches.is_present("VERBOSE"),
    }
  }

  pub fn zmq_uri(&self) -> &str {
    self.zmq_uri
  }

  pub fn mysql_uri(&self) -> &str {
    self.mysql_uri
  }

  pub fn write_threads_count(&self) -> usize {
    self.write_threads_count
  }

  pub fn approve_threads_count(&self) -> usize {
    self.approve_threads_count
  }

  pub fn milestone_address(&self) -> &str {
    self.milestone_address
  }

  pub fn milestone_start_index(&self) -> &str {
    self.milestone_start_index
  }

  pub fn verbose(&self) -> bool {
    self.verbose
  }
}
