//! Starts a timely dataflow execution from command line arguments and per-worker logic.

use timely_communication::{initialize, Configuration, Allocator};
use dataflow::scopes::Root;

/// Executes a timely dataflow computation with supplied arguments and per-communicator logic.
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
/// ```ignore
/// extern crate timely;
///
/// use timely::*;
/// use timely::construction::inspect::InspectExt;
///
/// // construct and execute a timely dataflow
/// timely::execute_from_args(std::env::args(), |root| {
///
///     // add an input and base computation off of it
///     let mut input = root.subcomputation(|subgraph| {
///         let (input, stream) = subgraph.new_input();
///         stream.inspect(|x| println!("hello {:?}", x));
///         input
///     });
///
///     // introduce input, advance computation
///     for round in 0..10 {
///         input.send_at(round, round..round+1);
///         input.advance_to(round + 1);
///         root.step();
///     }
///
///     input.close();          // close the input
///     while root.step() { }   // finish off the computation
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

pub fn execute<F: Fn(&mut Root<Allocator>)+Send+Sync+'static>(config: Configuration, func: F) {
    initialize(config, move |allocator| {
        func(&mut Root::new(allocator));
    })
}


pub fn execute_from_args<I: Iterator<Item=String>, F: Fn(&mut Root<Allocator>)+Send+Sync+'static>(iter: I, func: F) {
    if let Some(config) = Configuration::from_args(iter) {
        initialize(config, move |allocator| {
            func(&mut Root::new(allocator));
        })
    }
    else {
        println!("failed to initialize communication fabric");
    }
}
