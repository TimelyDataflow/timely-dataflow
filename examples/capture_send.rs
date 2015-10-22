extern crate timely;

use std::rc::Rc;
use std::net::{TcpListener, TcpStream};
use timely::dataflow::Scope;
use timely::dataflow::operators::{Capture, ToStream, Inspect};
use timely::dataflow::operators::capture::{EventReader, EventWriter, Replay};

fn main() {
    timely::execute(timely::Configuration::Thread, |computation| {
        let send = TcpStream::connect("127.0.0.1:8000").unwrap();

        computation.scoped::<u64,_,_>(|scope1|
            (0..10u64)
                .to_stream(scope1)
                .capture_into(EventWriter::new(send))
        );
    });
}
