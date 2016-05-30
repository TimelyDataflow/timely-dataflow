extern crate timely_communication;

fn main() {
    // configure for two threads, just one process.
    let config = timely_communication::Configuration::Process(2);

    // initailizes communication, spawns workers
    let guards = timely_communication::initialize(config, |mut allocator| {
        println!("worker {} started", allocator.index());

        // allocates pair of senders list and one receiver.
        let (mut senders, mut receiver) = allocator.allocate();

        // send typed data along each channel
        senders[0].send(format!("hello, {}", 0));
        senders[1].send(format!("hello, {}", 1));

        // no support for termination notification,
        // we have to count down ourselves.
        let mut expecting = 2;
        while expecting > 0 {
            if let Some(message) = receiver.recv() {
                println!("worker {}: received: <{}>", allocator.index(), message);
                expecting -= 1;
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
