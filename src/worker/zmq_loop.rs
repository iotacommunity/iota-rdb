use std::sync::mpsc;
use zmq;

pub struct ZmqLoop {
  pub socket: zmq::Socket,
  pub write_tx: mpsc::Sender<String>,
}

impl ZmqLoop {
  pub fn run(self, verbose: bool) -> ! {
    loop {
      match self.socket.recv_string(0) {
        Ok(Ok(string)) => {
          if verbose {
            println!("[zmq] {}", string);
          }
          self
            .write_tx
            .send(string)
            .expect("Thread communication failure");
        }
        Ok(Err(err)) => {
          eprintln!("[zmq] Unexpected byte sequence: {:?}", err);
        }
        Err(err) => {
          eprintln!("{}", err);
        }
      }
    }
  }
}
