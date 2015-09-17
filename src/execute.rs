//! Starts a timely dataflow execution from configuration information and per-worker logic.

use timely_communication::{initialize, Configuration, Allocator};
use dataflow::scopes::{Root, Child, Scope};

/// Executes a single-threaded timely dataflow computation.
///
/// The `example` method takes a closure on a `Scope` which it executes to initialize and run a
/// timely dataflow computation on a single thread. This method is intended for use in examples,
/// rather than programs that may need to run across multiple workers.
///
/// #Examples
/// ```
/// use timely::dataflow::operators::{ToStream, Inspect};
///
/// timely::example(|scope| {
///     (0..10).to_stream(scope)
///            .inspect(|x| println!("seen: {:?}", x));
/// });
/// ```
pub fn example<F: Fn(&mut Child<Root<Allocator>, u64>)+Send+Sync+'static>(func: F) {
    initialize(Configuration::Thread, move |allocator| {
        let mut root = Root::new(allocator);
        root.scoped::<u64,_,_>(|x| func(x));
        while root.step() { }
    })
}

/// Executes a timely dataflow from a configuration and per-communicator logic.
///
/// The `execute` method takes a `Configuration` and spins up some number of
/// workers threads, each of which execute the supplied closure to construct and run a timely
/// dataflow computation.
///
/// #Examples
/// ```
/// use timely::dataflow::Scope;
/// use timely::dataflow::operators::{ToStream, Inspect};
///
/// // execute a timely dataflow using three worker threads.
/// timely::execute(timely::Configuration::Process(3), |root| {
///     root.scoped::<u64,_,_>(|scope| {
///         (0..10).to_stream(scope)
///                .inspect(|x| println!("seen: {:?}", x));
///     })
/// });
/// ```
pub fn execute<F: Fn(&mut Root<Allocator>)+Send+Sync+'static>(config: Configuration, func: F) {
    initialize(config, move |allocator| {
        let mut root = Root::new(allocator);
        func(&mut root);
        while root.step() { }
    })
}


/// Executes a timely dataflow from supplied arguments and per-communicator logic.
///
/// The `execute` method takes arguments (typically `std::env::args()`) and spins up some number of
/// workers threads, each of which execute the supplied closure to construct and run a timely
/// dataflow computation.
///
/// The arguments `execute` currently understands are:
///
/// `-w, --workers`: number of per-process worker threads.
///
/// `-n, --processes`: number of processes involved in the computation.
///
/// `-p, --process`: identity of this process; from 0 to n-1.
///
/// `-h, --hostfile`: a text file whose lines are "hostname:port" in order of process identity.
/// If not specified, `localhost` will be used, with port numbers increasing from 2101 (chosen
/// arbitrarily).
///
/// #Examples
/// ```
/// use timely::dataflow::*;
/// use timely::dataflow::operators::{ToStream, Inspect};
///
/// // execute a timely dataflow using command line parameters
/// timely::execute_from_args(std::env::args(), |root| {
///     root.scoped::<u64,_,_>(|scope| {
///         (0..10).to_stream(scope)
///                .inspect(|x| println!("seen: {:?}", x));
///     })
/// });
/// ```
/// ```ignore
/// host0% cargo run -- -w 2 -n 4 -h hosts.txt -p 0
/// host1% cargo run -- -w 2 -n 4 -h hosts.txt -p 1
/// host2% cargo run -- -w 2 -n 4 -h hosts.txt -p 2
/// host3% cargo run -- -w 2 -n 4 -h hosts.txt -p 3
/// ```
/// ```ignore
/// % cat hosts.txt
/// host0:port
/// host1:port
/// host2:port
/// host3:port
/// ```
pub fn execute_from_args<I: Iterator<Item=String>, F: Fn(&mut Root<Allocator>)+Send+Sync+'static>(iter: I, func: F) {
    if let Some(config) = Configuration::from_args(iter) {
        execute(config, func);
    }
    else {
        println!("failed to initialize communication fabric");
    }
}
