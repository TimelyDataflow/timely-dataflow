extern crate abomonation;
extern crate timely;
extern crate timely_logging;

use std::net::TcpListener;
use std::io;
use std::io::Read;

use timely_logging::{CommsSetup, EventsSetup, Event as LogEvent, CommEvent};
use timely::dataflow::operators::capture::{Event, EventReader};
use timely::progress::nested::product::Product;
use timely::progress::timestamp::RootTimestamp;
use timely::dataflow::operators::capture::EventIterator;

fn main() {

    let mut args = std::env::args();
    args.next().unwrap();
    let communication = args.next().unwrap() == "comm";

    let listener = TcpListener::bind(
        if communication { "127.0.0.1:34254" } else { "127.0.0.1:34255" }).unwrap();
    let mut i = 0;
    for socket in listener.incoming() {
        i += 1;
        let thread = ::std::thread::spawn(move || {
            let mut socket = io::BufReader::new(socket.unwrap());
            if communication {
                let mut reader = EventReader::<Product<RootTimestamp, u64>, (u64, CommsSetup, CommEvent), _>::new(socket);
                println!("communication");
                loop {
                    if let Some(e) = reader.next() {
                        println!("{} →", i);
                    }
                }
            } else {
                let mut reader = EventReader::<Product<RootTimestamp, u64>, (u64, EventsSetup, LogEvent), _>::new(socket);
                println!("log");
                loop {
                    if let Some(e) = reader.next() {
                        println!("{} →", i);
                    }
                }
            }
        });
    }

}
