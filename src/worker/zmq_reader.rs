use std::sync::mpsc;
use zmq;

pub struct ZmqReader<'a> {
  pub socket: &'a zmq::Socket,
  pub tx: &'a mpsc::Sender<String>,
}

impl<'a> ZmqReader<'a> {
  pub fn run(self, verbose: bool) {
    loop {
      match self.socket.recv_string(0) {
        Ok(Ok(string)) => {
          if verbose {
            println!("ZeroMQ message received: {}", string);
          }
          self.tx.send(string).expect("Thread communication failure");
        }
        Ok(Err(err)) => {
          eprintln!("Unexpected byte sequence: {:?}", err);
        }
        Err(err) => {
          eprintln!("{}", err);
        }
      }
    }
  }
}
