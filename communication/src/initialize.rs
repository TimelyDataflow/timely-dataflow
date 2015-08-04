//! Initialization logic for a generic instance of the `Allocate` channel allocation trait.
//!
//!

use std::thread;
use std::io::BufRead;
use getopts;
use std::sync::Arc;

use allocator::{Thread, Process, Generic};
use networking::initialize_networking;

// /// Configuration information for the desired number of threads, processes, and locations thereof.
// pub struct Configuration {
//     /// The number of threads each process should use.
//     pub threads: usize,
//
//     /// A process identifier in range `(0..addresses.len())`.
//     pub process: usize,
//
//     /// The list of host:port strings indicating where each process will bind.
//     pub addresses: Vec<String>,
//
//     /// Enables or suppresses network connection progress.
//     pub report: bool,
// }

pub enum Configuration {
    Thread,
    Process(usize),
    Cluster(usize, usize, Vec<String>, bool)
}

// impl Configuration {
//     fn threads(&self) -> usize {
//         match *self {
//             Configuration::Thread => 1,
//             Configuration::Process(t) => t,
//             Configuration::Cluster(t,_,_,_) => t,
//         }
//     }
//     fn process(&self) -> usize {
//         match *self {
//             Configuration::Thread => 0,
//             Configuration::Process(_) => 0,
//             Configuration::Cluster(_,p,_,_) => p,
//         }
//     }
//     fn addresses(&self) -> &[String] {
//         match *self {
//             Configuration::Thread => &[],
//             Configuration::Process(_) => &[],
//             Configuration::Cluster(_,_, ref a,_) => &a[..],
//         }
//     }
//     fn report(&self) -> bool {
//         match *self {
//             Configuration::Cluster(_,_,_,b) => b,
//             _ => false,
//         }
//     }
// }

impl Configuration {

    /// Constructs a new configuration by parsing supplied text arguments.
    pub fn from_args<I: Iterator<Item=String>>(args: I) -> Option<Configuration> {

        let mut opts = getopts::Options::new();
        opts.optopt("w", "threads", "number of per-process worker threads", "NUM");
        opts.optopt("p", "process", "identity of this process", "IDX");
        opts.optopt("n", "processes", "number of processes", "NUM");
        opts.optopt("h", "hostfile", "text file whose lines are process addresses", "FILE");
        opts.optflag("r", "report", "reports connection progress");

        opts.parse(args).ok().map(|matches| {

            // let mut config = Configuration::new(1, 0, Vec::new());
            let threads = matches.opt_str("w").map(|x| x.parse().unwrap_or(1)).unwrap_or(1);
            let process = matches.opt_str("p").map(|x| x.parse().unwrap_or(0)).unwrap_or(0);
            let processes = matches.opt_str("n").map(|x| x.parse().unwrap_or(1)).unwrap_or(1);
            let report = matches.opt_present("report");

            assert!(process < processes);

            if processes > 1 {
                let mut addresses = Vec::new();
                if let Some(hosts) = matches.opt_str("h") {
                    let reader = ::std::io::BufReader::new(::std::fs::File::open(hosts.clone()).unwrap());
                    for x in reader.lines().take(processes) {
                        addresses.push(x.unwrap());
                    }
                    if addresses.len() < processes {
                        panic!("could only read {} addresses from {}, but -n: {}", addresses.len(), hosts, processes);
                    }
                }
                else {
                    for index in (0..processes) {
                        addresses.push(format!("localhost:{}", 2101 + index));
                    }
                }

                assert!(processes == addresses.len());
                Configuration::Cluster(threads, process, addresses, report)
            }
            else {
                if threads > 1 { Configuration::Process(threads) }
                else { Configuration::Thread }
            }
        })
    }
}

/// Initializes an `allocator::Generic` for each thread, spawns the local threads, and invokes the
/// supplied function with the allocator. Returns only once all threads have returned.
pub fn initialize<F: Fn(Generic)+Send+Sync+'static>(config: Configuration, func: F) {

    let allocators = match config {
        Configuration::Thread => vec![Generic::Thread(Thread)],
        Configuration::Process(threads) => Process::new_vector(threads).into_iter().map(|x| Generic::Process(x)).collect(),
        Configuration::Cluster(threads, process, addresses, report) => {
            initialize_networking(addresses, process, threads, report)
                .ok()
                .expect("error initializing networking")
                .into_iter()
                .map(|x| Generic::Binary(x)).collect()
        },
    };

    let logic = Arc::new(func);

    let mut guards = Vec::new();
    for allocator in allocators.into_iter() {
        let clone = logic.clone();
        guards.push(thread::Builder::new().name(format!("worker thread {}", allocator.index()))
                                          .spawn(move || (*clone)(allocator))
                                          .unwrap());
    }

    for guard in guards { guard.join().unwrap(); }
}
