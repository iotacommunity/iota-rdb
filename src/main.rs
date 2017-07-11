extern crate zmq;

mod transaction;

use transaction::Transaction;

fn main() {
  let ctx = zmq::Context::new();
  let socket = ctx.socket(zmq::SUB).unwrap();
  socket.connect("tcp://88.99.93.196:5556").unwrap();
  socket.set_subscribe("tx ".as_bytes()).unwrap();

  loop {
    let message = socket.recv_string(0).unwrap().unwrap();
    let transaction = Transaction::parse(&message).unwrap();
    println!("{:?}", transaction);
  }
}
