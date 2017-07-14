#[macro_use]
extern crate mysql;
extern crate zmq;

mod transaction;
mod mapper;

use mapper::Mapper;
use transaction::Transaction;

fn main() {
  let ctx = zmq::Context::new();
  let socket = ctx.socket(zmq::SUB).unwrap();
  socket.connect("tcp://88.99.93.196:5556").unwrap();
  socket.set_subscribe("tx ".as_bytes()).unwrap();

  let pool = mysql::Pool::new(
    "mysql://root:password@127.0.0.1:3306/iota?prefer_socket=false",
  ).unwrap();
  let mut mapper = Mapper::new(&pool).unwrap();

  loop {
    let message = socket.recv_string(0).unwrap().unwrap();
    let transaction = Transaction::parse(&message).unwrap();
    println!("{:?}", transaction);
    transaction.process(&mut mapper).unwrap();
  }
}
