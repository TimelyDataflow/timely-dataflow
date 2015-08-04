use std::thread;
use std::io::BufRead;
use getopts;
use std::sync::Arc;

use allocator::{Thread, Process, Generic};
use networking::initialize_networking;

pub struct Configuration {
    threads: usize,
    process: usize,
    processes: usize,
    addresses: Vec<String>,
}

impl Configuration {
    pub fn new(threads: usize, process: usize, processes: usize, addresses: Vec<String>) -> Configuration {
        Configuration {
            threads: threads,
            process: process,
            processes: processes,
            addresses: addresses,
        }
    }
    pub fn from_args<I: Iterator<Item=String>>(args: I) -> Option<Configuration> {

        let mut opts = getopts::Options::new();
        opts.optopt("w", "threads", "number of per-process worker threads", "NUM");
        opts.optopt("p", "process", "identity of this process", "IDX");
        opts.optopt("n", "processes", "number of processes", "NUM");
        opts.optopt("h", "hostfile", "text file whose lines are process addresses", "FILE");

        opts.parse(args).ok().map(|matches| {

            let mut config = Configuration::new(1, 0, 1, Vec::new());
            config.threads = matches.opt_str("w").map(|x| x.parse().unwrap_or(1)).unwrap_or(1);
            config.process = matches.opt_str("p").map(|x| x.parse().unwrap_or(0)).unwrap_or(0);
            config.processes = matches.opt_str("n").map(|x| x.parse().unwrap_or(1)).unwrap_or(1);

            if config.processes > 1 {
                if let Some(hosts) = matches.opt_str("h") {
                    let reader = ::std::io::BufReader::new(::std::fs::File::open(hosts).unwrap());
                    for x in reader.lines().take(config.processes) {
                        config.addresses.push(x.unwrap());
                    }
                }
                else {
                    for index in (0..config.processes) {
                        config.addresses.push(format!("localhost:{}", 2101 + index));
                    }
                }
            }

            config
        })
    }
}

pub fn initialize<F: Fn(Generic)+Send+Sync+'static>(config: Configuration, func: F) {

    let allocators = if config.processes > 1 {
        if config.addresses.len() != config.processes {
            panic!("{} addresses found, for {} processes", config.addresses.len(), config.processes);
        }
        initialize_networking(config.addresses, config.process, config.threads)
            .ok()
            .expect("error initializing networking")
            .into_iter()
            .map(|x| Generic::Binary(x)).collect()
    }
    else if config.threads > 1 {
        Process::new_vector(config.threads).into_iter().map(|x| Generic::Process(x)).collect()
    }
    else {
        vec![Generic::Thread(Thread)]
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
