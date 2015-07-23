use std::thread;
use std::io::BufRead;
use getopts;
use std::sync::Arc;

use communication::{Communicator, ThreadCommunicator, ProcessCommunicator};
use communication::allocator::GenericCommunicator;
use networking::initialize_networking;

// initializes a timely dataflow computation with supplied arguments and per-communicator logic
pub fn initialize<I: Iterator<Item=String>, F: Fn(GenericCommunicator)+Send+Sync+'static>(iter: I, func: F) {

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
            initialize_networking(addresses, process, workers).ok().expect("error initializing networking").into_iter().map(|x| GenericCommunicator::Binary(x)).collect()
        }
        else if workers > 1 {
            ProcessCommunicator::new_vector(workers).into_iter().map(|x| GenericCommunicator::Process(x)).collect()
        }
        else {
            vec![GenericCommunicator::Thread(ThreadCommunicator)]
        };

        let logic = Arc::new(func);

        let mut guards = Vec::new();
        for communicator in communicators.into_iter() {
            let clone = logic.clone();
            guards.push(thread::Builder::new().name(format!("worker thread {}", communicator.index()))
                                              .spawn(move || (*clone)(communicator))
                                              .unwrap());
        }

        for guard in guards { guard.join().unwrap(); }
    }
    else { println!("{}", opts.usage("test")); }
}
