//! Initialization logic for a generic instance of the `Allocate` channel allocation trait.

use std::thread;
#[cfg(feature = "getopts")]
use std::io::BufRead;
#[cfg(feature = "getopts")]
use getopts;
use std::sync::Arc;

use std::any::Any;

use crate::allocator::thread::ThreadBuilder;
use crate::allocator::{AllocateBuilder, Process, Generic, GenericBuilder};
use crate::allocator::zero_copy::initialize::initialize_networking;

use crate::logging::{CommunicationSetup, CommunicationEvent};
use logging_core::Logger;


/// Possible configurations for the communication infrastructure.
pub enum Configuration {
    /// Use one thread.
    Thread,
    /// Use one process with an indicated number of threads.
    Process(usize),
    /// Expect multiple processes.
    Cluster {
        /// Number of per-process worker threads
        threads: usize,
        /// Identity of this process
        process: usize,
        /// Addresses of all processes
        addresses: Vec<String>,
        /// Verbosely report connection process
        report: bool,
        /// Closure to create a new logger for a communication thread
        log_fn: Box<dyn Fn(CommunicationSetup) -> Option<Logger<CommunicationEvent, CommunicationSetup>> + Send + Sync>,
    }
}

#[cfg(feature = "getopts")]
impl Configuration {

    /// Returns a `getopts::Options` struct that can be used to print
    /// usage information in higher-level systems.
    pub fn options() -> getopts::Options {
        let mut opts = getopts::Options::new();
        opts.optopt("w", "threads", "number of per-process worker threads", "NUM");
        opts.optopt("p", "process", "identity of this process", "IDX");
        opts.optopt("n", "processes", "number of processes", "NUM");
        opts.optopt("h", "hostfile", "text file whose lines are process addresses", "FILE");
        opts.optflag("r", "report", "reports connection progress");

        opts
    }

    /// Constructs a new configuration by parsing supplied text arguments.
    ///
    /// Most commonly, this uses `std::env::Args()` as the supplied iterator.
    pub fn from_args<I: Iterator<Item=String>>(args: I) -> Result<Configuration,String> {
        let opts = Configuration::options();

        opts.parse(args)
            .map_err(|e| format!("{:?}", e))
            .map(|matches| {

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
                    for index in 0..processes {
                        addresses.push(format!("localhost:{}", 2101 + index));
                    }
                }

                assert!(processes == addresses.len());
                Configuration::Cluster {
                    threads,
                    process,
                    addresses,
                    report,
                    log_fn: Box::new( | _ | None),
                }
            }
            else if threads > 1 { Configuration::Process(threads) }
            else { Configuration::Thread }
        })
    }

    /// Attempts to assemble the described communication infrastructure.
    pub fn try_build(self) -> Result<(Vec<GenericBuilder>, Box<dyn Any+Send>), String> {
        match self {
            Configuration::Thread => {
                Ok((vec![GenericBuilder::Thread(ThreadBuilder)], Box::new(())))
            },
            Configuration::Process(threads) => {
                Ok((Process::new_vector(threads).into_iter().map(|x| GenericBuilder::Process(x)).collect(), Box::new(())))
            },
            Configuration::Cluster { threads, process, addresses, report, log_fn } => {
                match initialize_networking(addresses, process, threads, report, log_fn) {
                    Ok((stuff, guard)) => {
                        Ok((stuff.into_iter().map(|x| GenericBuilder::ZeroCopy(x)).collect(), Box::new(guard)))
                    },
                    Err(err) => Err(format!("failed to initialize networking: {}", err))
                }
            },
        }
    }
}

/// Initializes communication and executes a distributed computation.
///
/// This method allocates an `allocator::Generic` for each thread, spawns local worker threads,
/// and invokes the supplied function with the allocator.
/// The method returns a `WorkerGuards<T>` which can be `join`ed to retrieve the return values
/// (or errors) of the workers.
///
///
/// # Examples
/// ```
/// use timely_communication::Allocate;
///
/// // configure for two threads, just one process.
/// let config = timely_communication::Configuration::Process(2);
///
/// // initializes communication, spawns workers
/// let guards = timely_communication::initialize(config, |mut allocator| {
///     println!("worker {} started", allocator.index());
///
///     // allocates pair of senders list and one receiver.
///     let (mut senders, mut receiver) = allocator.allocate(0);
///
///     // send typed data along each channel
///     use timely_communication::Message;
///     senders[0].send(Message::from_typed(format!("hello, {}", 0)));
///     senders[1].send(Message::from_typed(format!("hello, {}", 1)));
///
///     // no support for termination notification,
///     // we have to count down ourselves.
///     let mut expecting = 2;
///     while expecting > 0 {
///         allocator.receive();
///         if let Some(message) = receiver.recv() {
///             use std::ops::Deref;
///             println!("worker {}: received: <{}>", allocator.index(), message.deref());
///             expecting -= 1;
///         }
///         allocator.release();
///     }
///
///     // optionally, return something
///     allocator.index()
/// });
///
/// // computation runs until guards are joined or dropped.
/// if let Ok(guards) = guards {
///     for guard in guards.join() {
///         println!("result: {:?}", guard);
///     }
/// }
/// else { println!("error in computation"); }
/// ```
///
/// The should produce output like:
///
/// ```ignore
/// worker 0 started
/// worker 1 started
/// worker 0: received: <hello, 0>
/// worker 1: received: <hello, 1>
/// worker 0: received: <hello, 0>
/// worker 1: received: <hello, 1>
/// result: Ok(0)
/// result: Ok(1)
/// ```
pub fn initialize<T:Send+'static, F: Fn(Generic)->T+Send+Sync+'static>(
    config: Configuration,
    func: F,
) -> Result<WorkerGuards<T>,String> {
    let (allocators, others) = config.try_build()?;
    initialize_from(allocators, others, func)
}

/// Initializes computation and runs a distributed computation.
///
/// This version of `initialize` allows you to explicitly specify the allocators that
/// you want to use, by providing an explicit list of allocator builders. Additionally,
/// you provide `others`, a `Box<Any>` which will be held by the resulting worker guard
/// and dropped when it is dropped, which allows you to join communication threads.
///
/// # Examples
/// ```
/// use timely_communication::Allocate;
///
/// // configure for two threads, just one process.
/// let builders = timely_communication::allocator::process::Process::new_vector(2);
///
/// // initializes communication, spawns workers
/// let guards = timely_communication::initialize_from(builders, Box::new(()), |mut allocator| {
///     println!("worker {} started", allocator.index());
///
///     // allocates pair of senders list and one receiver.
///     let (mut senders, mut receiver) = allocator.allocate(0);
///
///     // send typed data along each channel
///     use timely_communication::Message;
///     senders[0].send(Message::from_typed(format!("hello, {}", 0)));
///     senders[1].send(Message::from_typed(format!("hello, {}", 1)));
///
///     // no support for termination notification,
///     // we have to count down ourselves.
///     let mut expecting = 2;
///     while expecting > 0 {
///         allocator.receive();
///         if let Some(message) = receiver.recv() {
///             use std::ops::Deref;
///             println!("worker {}: received: <{}>", allocator.index(), message.deref());
///             expecting -= 1;
///         }
///         allocator.release();
///     }
///
///     // optionally, return something
///     allocator.index()
/// });
///
/// // computation runs until guards are joined or dropped.
/// if let Ok(guards) = guards {
///     for guard in guards.join() {
///         println!("result: {:?}", guard);
///     }
/// }
/// else { println!("error in computation"); }
/// ```
pub fn initialize_from<A, T, F>(
    builders: Vec<A>,
    others: Box<dyn Any+Send>,
    func: F,
) -> Result<WorkerGuards<T>,String>
where
    A: AllocateBuilder+'static,
    T: Send+'static,
    F: Fn(<A as AllocateBuilder>::Allocator)->T+Send+Sync+'static
{
    let logic = Arc::new(func);
    let mut guards = Vec::new();
    for (index, builder) in builders.into_iter().enumerate() {
        let clone = logic.clone();
        guards.push(thread::Builder::new()
                            .name(format!("timely:work-{}", index))
                            .spawn(move || {
                                let communicator = builder.build();
                                (*clone)(communicator)
                            })
                            .map_err(|e| format!("{:?}", e))?);
    }

    Ok(WorkerGuards { guards, others })
}

/// Maintains `JoinHandle`s for worker threads.
pub struct WorkerGuards<T:Send+'static> {
    guards: Vec<::std::thread::JoinHandle<T>>,
    others: Box<dyn Any+Send>,
}

impl<T:Send+'static> WorkerGuards<T> {

    /// Returns a reference to the indexed guard.
    pub fn guards(&self) -> &[std::thread::JoinHandle<T>] {
        &self.guards[..]
    }

    /// Provides access to handles that are not worker threads.
    pub fn others(&self) -> &Box<dyn Any+Send> {
        &self.others
    }

    /// Waits on the worker threads and returns the results they produce.
    pub fn join(mut self) -> Vec<Result<T, String>> {
        self.guards
            .drain(..)
            .map(|guard| guard.join().map_err(|e| format!("{:?}", e)))
            .collect()
    }
}

impl<T:Send+'static> Drop for WorkerGuards<T> {
    fn drop(&mut self) {
        for guard in self.guards.drain(..) {
            guard.join().expect("Worker panic");
        }
        // println!("WORKER THREADS JOINED");
    }
}
