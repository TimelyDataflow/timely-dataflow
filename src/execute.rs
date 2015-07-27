use std::thread;
use std::io::BufRead;
use getopts;
use std::sync::Arc;

use communication::communicator::{Communicator, Thread, Process, Generic};
use construction::GraphRoot;
use networking::initialize_networking;

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
/// If not specified, `localhost` will be used, with port numbers increasing from 2101 (arbitrary).
///
/// #Examples
/// ```ignore
/// extern crate timely;
///
/// use timely::*;
/// use timely::construction::inspect::InspectExt;
///
/// // construct and execute a timely dataflow
/// timely::execute(std::env::args(), |root| {
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
pub fn execute<I: Iterator<Item=String>, F: Fn(&mut GraphRoot<Generic>)+Send+Sync+'static>(iter: I, func: F) {

    let mut opts = getopts::Options::new();
    opts.optopt("w", "workers", "number of per-process worker threads", "NUM");
    opts.optopt("p", "process", "identity of this process", "IDX");
    opts.optopt("n", "processes", "number of processes", "NUM");
    opts.optopt("h", "hostfile", "text file whose lines are process addresses", "FILE");

    if let Ok(matches) = opts.parse(iter) {
        let workers: u64 = matches.opt_str("w").map(|x| x.parse().unwrap_or(1)).unwrap_or(1);
        let process: u64 = matches.opt_str("p").map(|x| x.parse().unwrap_or(0)).unwrap_or(0);
        let processes: u64 = matches.opt_str("n").map(|x| x.parse().unwrap_or(1)).unwrap_or(1);

        let communicators = if processes > 1 {
            let addresses: Vec<_> = if let Some(hosts) = matches.opt_str("h") {
                let reader = ::std::io::BufReader::new(::std::fs::File::open(hosts).unwrap());
                reader.lines().take(processes as usize).map(|x| x.unwrap()).collect()
            }
            else {
                (0..processes).map(|index| format!("localhost:{}", 2101 + index).to_string()).collect()
            };
            if addresses.len() != processes as usize { panic!("{} addresses found, for {} processes", addresses.len(), processes); }
            initialize_networking(addresses, process, workers).ok().expect("error initializing networking").into_iter().map(|x| Generic::Binary(x)).collect()
        }
        else if workers > 1 {
            Process::new_vector(workers).into_iter().map(|x| Generic::Process(x)).collect()
        }
        else {
            vec![Generic::Thread(Thread)]
        };

        let logic = Arc::new(func);

        let mut guards = Vec::new();
        for graph_root in communicators.into_iter() {
            let clone = logic.clone();
            guards.push(thread::Builder::new().name(format!("worker thread {}", graph_root.index()))
                                              .spawn(move || (*clone)(&mut GraphRoot::new(graph_root)))
                                              .unwrap());
        }

        for guard in guards { guard.join().unwrap(); }
    }
    else { println!("{}", opts.usage("test")); }
}
