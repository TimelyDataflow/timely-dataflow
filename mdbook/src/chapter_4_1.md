# Communication

Communication in timely dataflow starts from the `timely_communication` crate. This crate includes not only communication, but is actually where we start up the various worker threads and establish their identities. As in timely dataflow, everything starts by providing a per-worker closure, but this time we are given only a channel allocator as an argument.

Let's take a look at the example from the `timely_communication` documentation, which is not brief but shouldn't be wildly surprising either.

```rust,ignore
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
            senders[i].send(None);
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
```

There are a few steps here, and we'll talk through the important parts in each of them. 

## Configuration

There is only a limited amount of configuration you can currently do in a timely dataflow computation, and it all lives in the `initialize::Configuration` type. This type is a simple enumeration of three ways a timely computation could run:

```rust,ignore
/// Possible configurations for the communication infrastructure.
pub enum Configuration {
    /// Use one thread.
    Thread,
    /// Use one process with an indicated number of threads.
    Process(usize),
    /// Expect multiple processes indicated by `(threads, process, host_list, report)`.
    Cluster(usize, usize, Vec<String>, bool)
}
```

The first variant `Thread` indicates that we will simply have one worker thread. This is a helpful thing to know because it means that all of our exchange channels can be dramatically simplified, just down to simple queues. The second variant `Process(usize)` corresponds to multiple worker threads within one process. The number indicates the parameters. The third variant `Cluster` is how we indicate that this process will participate in a larger clustered computation; we supply the number of threads, this process' identifier, a list of addresses of all participants, and a boolean for whether we would like some diagnostics about the established connections.

The configuration is important because it determines how we build the channel allocator `allocator` that we eventually provide to each worker: `allocator` will be responsible for building communication channels to other workers, and it will need to know where these other workers are.

## Channel Allocators

The `allocator` reference bound by the worker closure is the only handle a worker has to the outside world (other than any values you move into the closure). It wraps up all the information we have about this workers place in the world, and provides the ability to assemble channels to the other workers.

There are a few implementations of the `Allocate` trait, which is defined as 

```rust,ignore
pub trait Allocate {
    fn index(&self) -> usize;
    fn peers(&self) -> usize;
    fn allocate<T: Data>(&mut self) -> (Vec<Box<Push<T>>>, Box<Pull<T>>);
}
```

These methods are the only functionality provided by `allocator`. A worker can ask for its own index, which is a number from zero up to the number of total peer workers (including itself), which it can also ask for. Perhaps most importantly, the worker can also request the allocation of a typed channel, which is returned as a pair of (i) a list of `Push` endpoints into which it can send data, and (ii) a single `Pull` endpoint from which it can extract data. The list has length equal to the number of peers, and data sent into push endpoint `i` will eventually be received by the worker with index `i`, if it keeps pulling on its pull endpoint.

The channels are various and interesting, but should be smartly arranged. The channel from the worker back to itself is just a queue, the channels within the same process are Rust's inter-thread channels, and the channels between processes will automatically serialize and deserialize the type `T` for you (this is part of the `T: Data` requirement).

## Push and Pull

The two traits `Push` and `Pull` are the heart of the communication underlying timely dataflow. They are very simple, but relatively subtle and interesting and perhaps even under-exploited.

### Push

The `Push` trait looks like so (with two helper methods elided):

```rust,ignore
pub trait Push<T> {
    fn push(&mut self, element: &mut Option<T>);
}```

That's all of it.

The `push` method takes a mutable reference to an option wrapped around a thing. This is your way of telling the communication layer that, (i) if the reference points to a thing, you'd really like to push it into the channel, and (ii) if the reference doesn't point to a thing this is the cue that you might walk away for a while. It is important to send a `None` if you would like to ensure that whatever you've `push`ed in the past is guaranteed to get through without further work on your part.

Now, we didn't need a mutable reference to do that; we could have just had the argument type be `Option<T>`, or had two methods `send` and `done` (those are the elided helper methods). 

This framing allows for fairly natural and *stable* zero-copy communication. When you want to send a buffer of records, you wrap it up as `Some(buffer)` and call `push`. Once `push` returns, the channel has probably taken your buffer, but it has the opportunity to leave something behind for you. This is a very easy way for the communication infrastructure to *return* resources to you. In fact, even if you have finished sending messages, it may make sense to repeatedly send mutable references to `None` for as long as the channel has memory to hand you.

Although not used by timely at the moment, this is also designed to support zero copy networking where the communication layer below (e.g. something like RDMA) operates more efficiently if it allocates the buffers for you (e.g. in dedicated memory pinned by the hardware). In this case, `push` is a great way to *request* resources from the channel. Similarly, it can serve as a decent back-channel to return owned resources for the underlying typed data (e.g., you `push`ed a list of `String` elements, and once used they could be returned to you to be re-used).

### Pull

The `Pull` trait is the dual to `Push`: it allows someone on the other end of a channel to request whatever the channel has in store next, also as a mutable reference to an option wrapped around the type.

pub trait Pull<T> {
    fn pull(&mut self) -> &mut Option<T>;
}

As before, the mutable reference and option allow the two participants to communicate about the availability of data, and to return resources if appropriate. For example, it is very natural in the call to `pull` to claim any `T` made available with a `::std::mem::swap` which puts something else in its place (either `Some(other)` or `None`). If the puller has some data to return, perhaps data it received from wherever it was pushing data at, this is a great opportunity to move it back up the communication chain.

I'm not aware of a circumstance where you might be obliged to call `pull` and set the result to `None` to signal that you may stop calling `Pull`. It seems like it could be important, if these methods really are dual, but I don't see how just yet.