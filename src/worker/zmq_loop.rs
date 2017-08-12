use std::sync::mpsc;
use zmq;

pub struct ZmqLoop {
  pub socket: zmq::Socket,
  pub insert_tx: mpsc::Sender<String>,
}

impl ZmqLoop {
  pub fn run(self) -> ! {
    loop {
      match self.socket.recv_string(0) {
        Ok(Ok(string)) => {
          info!("{}", string);
          self
            .insert_tx
            .send(string)
            .expect("Thread communication failure");
        }
        Ok(Err(err)) => {
          error!("Unexpected byte sequence: {:?}", err);
        }
        Err(err) => {
          error!("{}", err);
        }
      }
    }
  }
}
