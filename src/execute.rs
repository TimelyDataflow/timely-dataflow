//! Starts a timely dataflow execution from configuration information and per-worker logic.

use timely_communication::{initialize, Configuration, Allocator};
use dataflow::scopes::Root;


/// Executes a timely dataflow from a configuration and per-communicator logic.
///
/// The `execute` method takes a `Configuration` and spins up some number of
/// workers threads, each of which execute the supplied closure to construct and run a timely
/// dataflow computation.
///
/// #Examples
/// ```ignore
/// use timely::dataflow::*;
/// use timely::dataflow::operators::{Input, Inspect};
///
/// // execute a timely dataflow using three worker threads.
/// timely::execute(Configuration::Process(3), |root| {
///
///     // add an input and base computation off of it
///     let mut input = root.scoped(|scope| {
///         let (input, stream) = scope.new_input();
///         stream.inspect(|x| println!("hello {:?}", x));
///         input
///     });
///
///     // introduce input, advance computation
///     for round in 0..10 {
///         input.send(round);
///         input.advance_to(round + 1);
///         root.step();
///     }
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
/// use timely::dataflow::operators::{Input, Inspect};
///
/// // execute a timely dataflow using command line parameters
/// timely::execute_from_args(std::env::args(), |root| {
///
///     // add an input and base computation off of it
///     let mut input = root.scoped(|scope| {
///         let (input, stream) = scope.new_input();
///         stream.inspect(|x| println!("hello {:?}", x));
///         input
///     });
///
///     // introduce input, advance computation
///     for round in 0..10 {
///         input.send(round);
///         input.advance_to(round + 1);
///         root.step();
///     }
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
        // initialize(config, move |allocator| {
        //     func(&mut Root::new(allocator));
        // })
    }
    else {
        println!("failed to initialize communication fabric");
    }
}
