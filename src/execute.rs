//! Starts a timely dataflow execution from configuration information and per-worker logic.

use timely_communication::{initialize, Configuration, Allocator, WorkerGuards};
use dataflow::scopes::{Root, Child};
use logging::LoggerConfig;

/// Executes a single-threaded timely dataflow computation.
///
/// The `example` method takes a closure on a `Scope` which it executes to initialize and run a
/// timely dataflow computation on a single thread. This method is intended for use in examples,
/// rather than programs that may need to run across multiple workers. 
///
/// The `example` method returns whatever the single worker returns from its closure.
/// This is often nothing, but the worker can return something about the data it saw in order to
/// test computations.
///
/// The method aggressively unwraps returned `Result<_>` types.
///
/// #Examples
///
/// The simplest example creates a stream of data and inspects it.
///
/// ```
/// use timely::dataflow::operators::{ToStream, Inspect};
///
/// timely::example(|scope| {
///     (0..10).to_stream(scope)
///            .inspect(|x| println!("seen: {:?}", x));
/// });
/// ```
///
/// This next example captures the data and displays them once the computation is complete.
///
/// More precisely, the example captures a stream of events (receiving batches of data,
/// updates to input capabilities) and displays these events.
///
/// ```
/// use timely::dataflow::operators::{ToStream, Inspect, Capture};
/// use timely::dataflow::operators::capture::Extract;
///
/// let data = timely::example(|scope| {
///     (0..10).to_stream(scope)
///            .inspect(|x| println!("seen: {:?}", x))
///            .capture()
/// });
///
/// // the extracted data should have data (0..10) at timestamp 0.
/// assert_eq!(data.extract()[0].1, (0..10).collect::<Vec<_>>());
/// ```
pub fn example<T, F>(func: F) -> T
where T: Send+'static,
      F: Fn(&mut Child<Root<Allocator>,u64>)->T+Send+Sync+'static {
    let logging_config: LoggerConfig = Default::default();
    let timely_logging = logging_config.timely_logging.clone();
    let guards = initialize(Configuration::Thread, move |allocator| {
        let mut root = Root::new(allocator, timely_logging.clone());
        let result = root.dataflow(|x| func(x));
        while root.step() { }
        result
    }, logging_config.communication_logging);

    guards.unwrap() // assert the computation started correctly
          .join()   // wait for the worker to finish
          .pop()    // grab the known-to-exist result
          .unwrap() // assert that the result exists
          .unwrap() // crack open the result to get a T
}

/// Executes a timely dataflow from a configuration and per-communicator logic.
///
/// The `execute` method takes a `Configuration` and spins up some number of
/// workers threads, each of which execute the supplied closure to construct
/// and run a timely dataflow computation.
///
/// The closure may return a `T: Send+'static`, and `execute` returns a result
/// containing a `WorkerGuards<T>` (or error information), which can be joined
/// to recover the result `T` values from the local workers.
///
/// #Examples
/// ```
/// use timely::dataflow::operators::{ToStream, Inspect};
///
/// // execute a timely dataflow using three worker threads.
/// timely::execute(timely::Configuration::Process(3), |worker| {
///     worker.dataflow::<(),_,_>(|scope| {
///         (0..10).to_stream(scope)
///                .inspect(|x| println!("seen: {:?}", x));
///     })
/// }).unwrap();
/// ```
///
/// The following example demonstrates how one can extract data from a multi-worker execution.
/// In a multi-process setting, each process will only receive those records present at workers 
/// in the process.
///
/// ```
/// use std::sync::{Arc, Mutex};
/// use timely::dataflow::operators::{ToStream, Inspect, Capture};
/// use timely::dataflow::operators::capture::Extract;
///
/// // get send and recv endpoints, wrap send to share
/// let (send, recv) = ::std::sync::mpsc::channel();
/// let send = Arc::new(Mutex::new(send));
///
/// // execute a timely dataflow using three worker threads.
/// timely::execute(timely::Configuration::Process(3), move |worker| {
///     let send = send.lock().unwrap().clone();
///     worker.dataflow::<(),_,_>(move |scope| {
///         (0..10).to_stream(scope)
///                .inspect(|x| println!("seen: {:?}", x))
///                .capture_into(send);
///     });
/// }).unwrap();
///
/// // the extracted data should have data (0..10) thrice at timestamp 0.
/// assert_eq!(recv.extract()[0].1, (0..30).map(|x| x / 3).collect::<Vec<_>>());
/// ```
pub fn execute<T, F>(config: Configuration, func: F) -> Result<WorkerGuards<T>,String> 
where T:Send+'static,
      F: Fn(&mut Root<Allocator>)->T+Send+Sync+'static {
    // let logging_config = ::logging::blackhole();
    execute_logging(config, Default::default(), func)
}

/// Executes a timely dataflow from a configuration and per-communicator logic.
///
/// TODO(andreal)
///
/// Supports providing a logging configuration.
/// 
/// ```
/// use timely::dataflow::operators::{ToStream, Inspect};
/// use timely::logging::{LogManager, LoggerConfig};
///
/// let mut log_manager = LogManager::new();
///
/// // execute a timely dataflow using command line parameters
/// timely::execute_logging(timely::Configuration::Process(3), LoggerConfig::new(log_manager), |worker| {
///     worker.dataflow::<(),_,_>(|scope| {
///         (0..10).to_stream(scope)
///                .inspect(|x| println!("seen: {:?}", x));
///     })
/// }).unwrap();
/// ```
pub fn execute_logging<T, F>(config: Configuration, logging_config: LoggerConfig, func: F) -> Result<WorkerGuards<T>,String> 
where T:Send+'static,
      F: Fn(&mut Root<Allocator>)->T+Send+Sync+'static {
    let timely_logging = logging_config.timely_logging.clone();
    initialize(config, move |allocator| {
        let mut root = Root::new(allocator, timely_logging.clone());
        let result = func(&mut root);
        while root.step() { }
        result
    }, logging_config.communication_logging.clone())
}

/// Executes a timely dataflow from supplied arguments and per-communicator logic.
///
/// The `execute` method takes arguments (typically `std::env::args()`) and spins up some number of
/// workers threads, each of which execute the supplied closure to construct and run a timely
/// dataflow computation.
///
/// The closure may return a `T: Send+'static`, and `execute` returns a result
/// containing a `WorkerGuards<T>` (or error information), which can be joined
/// to recover the result `T` values from the local workers.
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
/// use timely::dataflow::operators::{ToStream, Inspect};
///
/// // execute a timely dataflow using command line parameters
/// timely::execute_from_args(std::env::args(), |worker| {
///     worker.dataflow::<(),_,_>(|scope| {
///         (0..10).to_stream(scope)
///                .inspect(|x| println!("seen: {:?}", x));
///     })
/// }).unwrap();
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
pub fn execute_from_args<I, T, F>(iter: I, func: F) -> Result<WorkerGuards<T>,String> 
    where I: Iterator<Item=String>, 
          T:Send+'static,
          F: Fn(&mut Root<Allocator>)->T+Send+Sync+'static, {
    // let logging_config = ::logging::blackhole();
    execute_from_args_logging(iter, Default::default(), func)
}

/// Executes a timely dataflow from supplied arguments and per-communicator logic.
///
/// TODO(andreal)
///
/// Supports providing a logging configuration.
/// 
/// ```
/// use timely::dataflow::operators::{ToStream, Inspect};
/// use timely::logging::{LogManager, LoggerConfig};
///
/// let mut log_manager = LogManager::new();
///
/// // execute a timely dataflow using command line parameters
/// timely::execute_from_args_logging(std::env::args(), LoggerConfig::new(log_manager), |worker| {
///     worker.dataflow::<(),_,_>(|scope| {
///         (0..10).to_stream(scope)
///                .inspect(|x| println!("seen: {:?}", x));
///     })
/// }).unwrap();
/// ```
pub fn execute_from_args_logging<I, T, F>(iter: I, logging_config: LoggerConfig, func: F) -> Result<WorkerGuards<T>,String> 
    where I: Iterator<Item=String>, 
          T:Send+'static,
          F: Fn(&mut Root<Allocator>)->T+Send+Sync+'static, {
    execute_logging(try!(Configuration::from_args(iter)), logging_config, func)
}
