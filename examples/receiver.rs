extern crate rmpv;

use std::net::TcpListener;
use std::io;

fn main() {

    let mut args = std::env::args();
    args.next().unwrap();
    let communication = args.next().unwrap() == "comm";

    let listener = TcpListener::bind(
        if communication { "127.0.0.1:34254" } else { "127.0.0.1:34255" }).unwrap();
    let mut i = 0;
    for socket in listener.incoming() {
        i += 1;
        ::std::thread::spawn(move || {
            let mut socket = io::BufReader::new(socket.unwrap());
            while let Ok(v) = rmpv::decode::value::read_value(&mut socket) {
                println!("{} -> {:?}", i, v);
            }
        });
    }

}
