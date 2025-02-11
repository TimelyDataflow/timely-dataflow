use timely_communication::{Allocate, Bytesable};

/// A wrapper that indicates the serialization/deserialization strategy.
pub struct Message {
    /// Text contents.
    pub payload: String,
}

impl Bytesable for Message {
    fn from_bytes(bytes: timely_bytes::arc::Bytes) -> Self {
        Message { payload: std::str::from_utf8(&bytes[..]).unwrap().to_string() }
    }

    fn length_in_bytes(&self) -> usize {
        self.payload.len()
    }

    fn into_bytes<W: ::std::io::Write>(&self, writer: &mut W) {
        writer.write_all(self.payload.as_bytes()).unwrap();
    }
}

fn main() {

    // extract the configuration from user-supplied arguments, initialize the computation.
    let config = timely_communication::Config::from_args(std::env::args()).unwrap();
    let guards = timely_communication::initialize(config, |mut allocator| {

        println!("worker {} of {} started", allocator.index(), allocator.peers());

        // allocates a pair of senders list and one receiver.
        let (mut senders, mut receiver) = allocator.allocate(0);

        // send typed data along each channel
        for i in 0 .. allocator.peers() {
            senders[i].send(Message { payload: format!("hello, {}", i)});
            senders[i].done();
        }

        // no support for termination notification,
        // we have to count down ourselves.
        let mut received = 0;
        while received < allocator.peers() {

            allocator.receive();

            if let Some(message) = receiver.recv() {
                println!("worker {}: received: <{}>", allocator.index(), message.payload);
                received += 1;
            }

            allocator.release();
        }

        allocator.index()
    });

    // computation runs until guards are joined or dropped.
    if let Ok(guards) = guards {
        for guard in guards.join() {
            println!("result: {:?}", guard);
        }
    }
    else { println!("error in computation"); }
}
