use std::thread;

use getopts;

use communication::{Communicator, ThreadCommunicator, ProcessCommunicator};
use communication::allocator::GenericCommunicator;
use networking::{initialize_networking, initialize_networking_from_file};

// initializes a timely dataflow computation with supplied arguments and per-communicator logic
pub fn initialize<I: Iterator<Item=String>, F: Fn(GenericCommunicator)+Send+Sync+'static, G: Fn()->F>(iter: I, func: G) {

    let mut opts = getopts::Options::new();
    opts.optopt("w", "workers", "number of per-process worker threads", "NUM");
    opts.optopt("p", "process", "identity of this process", "IDX");
    opts.optopt("n", "processes", "number of processes", "NUM");
    opts.optopt("h", "hostfile", "text file whose lines are process addresses", "FILE");

    if let Ok(matches) = opts.parse(iter.skip(1)) {
        let workers: u64 = matches.opt_str("w").map(|x| x.parse().unwrap_or(1)).unwrap_or(1);
        let process: u64 = matches.opt_str("p").map(|x| x.parse().unwrap_or(0)).unwrap_or(0);
        let processes: u64 = matches.opt_str("n").map(|x| x.parse().unwrap_or(1)).unwrap_or(1);

        let communicators = if processes > 1 {
            if let Some(hosts) = matches.opt_str("h") {
                initialize_networking_from_file(&hosts, process, workers).ok().expect("error initializing networking").into_iter().map(|x| GenericCommunicator::Binary(x)).collect()
            }
            else {
                let addresses = (0..processes).map(|index| format!("localhost:{}", 2101 + index).to_string()).collect();
                initialize_networking(addresses, process, workers).ok().expect("error initializing networking").into_iter().map(|x| GenericCommunicator::Binary(x)).collect()
            }
        }
        else if workers > 1 {
            ProcessCommunicator::new_vector(workers).into_iter().map(|x| GenericCommunicator::Process(x)).collect()
        }
        else {
            vec![GenericCommunicator::Thread(ThreadCommunicator)]
        };

        let mut guards = Vec::new();
        for communicator in communicators.into_iter() {
            let logic = func();
            guards.push(thread::Builder::new().name(format!("worker thread {}", communicator.index()))
                                              .spawn(move || logic(communicator))
                                              .unwrap());
        }

        for guard in guards { guard.join().unwrap(); }
    }
    else { println!("{}", opts.usage("test")); }
}
