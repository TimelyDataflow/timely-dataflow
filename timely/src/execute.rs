//! Starts a timely dataflow execution from configuration information and per-worker logic.

use std::io::BufRead;

use crate::communication::{initialize_from, CommunicationConfig, Allocator, allocator::AllocateBuilder, WorkerGuards};
use crate::dataflow::scopes::Child;
use crate::worker::{ProgressMode, Worker, WorkerConfig};

/// Configures the execution of a timely dataflow computation.
pub struct ExecuteConfig {
    /// Configuration for the communication infrastructure.
    pub communication: CommunicationConfig,
    /// Configuration for the worker threads.
    pub worker: WorkerConfig,
}

impl ExecuteConfig {
    /// Constructs an `ExecuteConfig` that uses one worker thread and the
    /// defaults for all other parameters.
    pub fn thread() -> ExecuteConfig {
        ExecuteConfig {
            communication: CommunicationConfig::Thread,
            worker: WorkerConfig::default(),
        }
    }

    /// Constructs an `ExecuteConfig` that uses `n` worker threads and the
    /// defaults for all other parameters.
    pub fn process(n: usize) -> ExecuteConfig {
        ExecuteConfig {
            communication: CommunicationConfig::Process(n),
            worker: WorkerConfig::default(),
        }
    }
}

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
/// # Examples
///
/// The simplest example creates a stream of data and inspects it.
///
/// ```rust
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
/// ```rust
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
where
    T: Send+'static,
    F: FnOnce(&mut Child<Worker<crate::communication::allocator::thread::Thread>,u64>)->T+Send+Sync+'static
{
    crate::execute::execute_directly(|worker| worker.dataflow(|scope| func(scope)))
}


/// Executes a single-threaded timely dataflow computation.
///
/// The `execute_directly` constructs a `Worker` and directly executes the supplied
/// closure to construct and run a timely dataflow computation. It does not create any
/// worker threads, and simply uses the current thread of control.
///
/// The closure may return a result, which will be returned from the computation.
///
/// # Examples
/// ```rust
/// use timely::dataflow::operators::{ToStream, Inspect};
///
/// // execute a timely dataflow using three worker threads.
/// timely::execute_directly(|worker| {
///     worker.dataflow::<(),_,_>(|scope| {
///         (0..10).to_stream(scope)
///                .inspect(|x| println!("seen: {:?}", x));
///     })
/// });
/// ```
pub fn execute_directly<T, F>(func: F) -> T
where
    T: Send+'static,
    F: FnOnce(&mut Worker<crate::communication::allocator::thread::Thread>)->T+Send+Sync+'static
{
    let alloc = crate::communication::allocator::thread::Thread::new();
    let mut worker = crate::worker::Worker::new(WorkerConfig::default(), alloc);
    let result = func(&mut worker);
    while worker.step_or_park(None) { }
    result
}

/// Executes a timely dataflow from a configuration and per-communicator logic.
///
/// The `execute` method takes a `Configuration` and spins up some number of
/// workers threads, each of which execute the supplied closure to construct
/// and run a timely dataflow computation.
///
/// The closure may return a `T: Send+'static`.  The `execute` method returns
/// immediately after initializing the timely computation with a result
/// containing a `WorkerGuards<T>` (or error information), which can be joined
/// to recover the result `T` values from the local workers.
///
/// *Note*: if the caller drops the result of `execute`, the drop code will
/// block awaiting the completion of the timely computation. If the result
/// of the method is not captured it will be dropped, which gives the experience
/// of `execute` blocking; to regain control after `execute` be sure to
/// capture the results and drop them only when the calling thread has no
/// other work to perform.
///
/// # Examples
/// ```rust
/// use timely::dataflow::operators::{ToStream, Inspect};
///
/// // execute a timely dataflow using three worker threads.
/// timely::execute(timely::ExecuteConfig::process(3), |worker| {
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
/// ```rust
/// use std::sync::{Arc, Mutex};
/// use timely::dataflow::operators::{ToStream, Inspect, Capture};
/// use timely::dataflow::operators::capture::Extract;
///
/// // get send and recv endpoints, wrap send to share
/// let (send, recv) = ::std::sync::mpsc::channel();
/// let send = Arc::new(Mutex::new(send));
///
/// // execute a timely dataflow using three worker threads.
/// timely::execute(timely::ExecuteConfig::process(3), move |worker| {
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
pub fn execute<T, F>(
    mut config: ExecuteConfig,
    func: F
) -> Result<WorkerGuards<T>,String>
where
    T:Send+'static,
    F: Fn(&mut Worker<Allocator>)->T+Send+Sync+'static {

    if let CommunicationConfig::Cluster { ref mut log_fn, .. } = config.communication {

        *log_fn = Box::new(|events_setup| {

            let mut result = None;
            if let Ok(addr) = ::std::env::var("TIMELY_COMM_LOG_ADDR") {

                use ::std::net::TcpStream;
                use crate::logging::BatchLogger;
                use crate::dataflow::operators::capture::EventWriter;

                eprintln!("enabled COMM logging to {}", addr);

                if let Ok(stream) = TcpStream::connect(&addr) {
                    let writer = EventWriter::new(stream);
                    let mut logger = BatchLogger::new(writer);
                    result = Some(crate::logging_core::Logger::new(
                        ::std::time::Instant::now(),
                        ::std::time::Duration::default(),
                        events_setup,
                        move |time, data| logger.publish_batch(time, data)
                    ));
                }
                else {
                    panic!("Could not connect to communication log address: {:?}", addr);
                }
            }
            result
        });
    }

    let (allocators, other) = config.communication.try_build()?;

    initialize_from(allocators, other, move |allocator| {

        let mut worker = Worker::new(WorkerConfig::default(), allocator);

        // If an environment variable is set, use it as the default timely logging.
        if let Ok(addr) = ::std::env::var("TIMELY_WORKER_LOG_ADDR") {

            use ::std::net::TcpStream;
            use crate::logging::{BatchLogger, TimelyEvent};
            use crate::dataflow::operators::capture::EventWriter;

            if let Ok(stream) = TcpStream::connect(&addr) {
                let writer = EventWriter::new(stream);
                let mut logger = BatchLogger::new(writer);
                worker.log_register()
                    .insert::<TimelyEvent,_>("timely", move |time, data|
                        logger.publish_batch(time, data)
                    );
            }
            else {
                panic!("Could not connect logging stream to: {:?}", addr);
            }
        }

        let result = func(&mut worker);
        while worker.step_or_park(None) { }
        result
    })
}

/// Executes a timely dataflow from supplied arguments and per-communicator logic.
///
/// The `execute` method takes arguments (typically `std::env::args()`) and spins up some number of
/// workers threads, each of which execute the supplied closure to construct and run a timely
/// dataflow computation.
///
/// The closure may return a `T: Send+'static`.  The `execute_from_args` method
/// returns immediately after initializing the timely computation with a result
/// containing a `WorkerGuards<T>` (or error information), which can be joined
/// to recover the result `T` values from the local workers.
///
/// *Note*: if the caller drops the result of `execute_from_args`, the drop code
/// will block awaiting the completion of the timely computation.
///
/// The arguments `execute_from_args` currently understands are:
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
/// # Examples
///
/// ```rust
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
          F: Fn(&mut Worker<Allocator>)->T+Send+Sync+'static, {
    let mut opts = getopts::Options::new();
    opts.optopt("w", "threads", "number of per-process worker threads", "NUM");
    opts.optopt("p", "process", "identity of this process", "IDX");
    opts.optopt("n", "processes", "number of processes", "NUM");
    opts.optopt("h", "hostfile", "text file whose lines are process addresses", "FILE");
    opts.optflag("r", "report", "reports connection progress");
    opts.optopt("", "progress-mode", "progress tracking mode (eager or demand)", "MODE");

    let matches = opts.parse(iter).map_err(|e| e.to_string())?;

    let threads = matches.opt_get_default("w", 1_usize).map_err(|e| e.to_string())?;
    let process = matches.opt_get_default("p", 0_usize).map_err(|e| e.to_string())?;
    let processes = matches.opt_get_default("n", 1_usize).map_err(|e| e.to_string())?;
    let report = matches.opt_present("report");

    assert!(process < processes);

    let comm_config = if processes > 1 {
        let mut addresses = Vec::new();
        if let Some(hosts) = matches.opt_str("h") {
            let file = ::std::fs::File::open(hosts.clone()).map_err(|e| e.to_string())?;
            let reader = ::std::io::BufReader::new(file);
            for line in reader.lines().take(processes) {
                addresses.push(line.map_err(|e| e.to_string())?);
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
        CommunicationConfig::Cluster {
            threads,
            process,
            addresses,
            report,
            log_fn: Box::new( | _ | None),
        }
    } else if threads > 1 {
        CommunicationConfig::Process(threads)
    } else {
        CommunicationConfig::Thread
    };

    let progress_mode = matches
        .opt_get_default("progress-mode", ProgressMode::Eager)
        .map_err(|e| e.to_string())?;

    let config = ExecuteConfig {
        communication: comm_config,
        worker: WorkerConfig::default().progress_mode(progress_mode),
    };

    execute(config, func)
}

/// Executes a timely dataflow from supplied allocators and logging.
///
/// Refer to [`execute`](execute()) for more details.
///
/// ```rust
/// use timely::dataflow::operators::{ToStream, Inspect};
/// use timely::worker::WorkerConfig;
///
/// // execute a timely dataflow using command line parameters
/// let (builders, other) = timely::CommunicationConfig::Process(3).try_build().unwrap();
/// timely::execute::execute_from(builders, other, WorkerConfig::default(), |worker| {
///     worker.dataflow::<(),_,_>(|scope| {
///         (0..10).to_stream(scope)
///                .inspect(|x| println!("seen: {:?}", x));
///     })
/// }).unwrap();
/// ```
pub fn execute_from<A, T, F>(
    builders: Vec<A>,
    others: Box<dyn ::std::any::Any+Send>,
    worker_config: WorkerConfig,
    func: F,
) -> Result<WorkerGuards<T>, String>
where
    A: AllocateBuilder+'static,
    T: Send+'static,
    F: Fn(&mut Worker<<A as AllocateBuilder>::Allocator>)->T+Send+Sync+'static {
    initialize_from(builders, others, move |allocator| {
        let mut worker = Worker::new(worker_config.clone(), allocator);
        let result = func(&mut worker);
        while worker.step_or_park(None) { }
        result
    })
}
