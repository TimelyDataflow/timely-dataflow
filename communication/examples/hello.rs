extern crate timely_communication;

fn main() {

    // extract the configuration from user-supplied arguments, initialize the computation.
    let config = timely_communication::Configuration::from_args(std::env::args()).unwrap();
    let guards = timely_communication::initialize(config, |mut allocator| {

        println!("worker {} of {} started", allocator.index(), allocator.peers());

        // allocates pair of senders list and one receiver.
        let (mut senders, mut receiver) = allocator.allocate();

        // send typed data along each channel
        for i in 0 .. allocator.peers() {
            senders[i].send(format!("hello, {}", i));
            senders[i].done();
        }

        // no support for termination notification,
        // we have to count down ourselves.
        let mut received = 0;
        while received < allocator.peers() {
            if let Some(message) = receiver.recv() {
                println!("worker {}: received: <{}>", allocator.index(), message);
                received += 1;
            }
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
