use std::cell::RefCell;
use std::rc::Rc;
use std::time::Duration;
use timely::dataflow::operators::{Input, Map, Probe};
use timely::logging::{TimelyLogger, TimelySummaryLogger};
use timely::progress::{Antichain, ChangeBatch, Operate, Source, SubgraphBuilder, Target, Timestamp};
use timely::progress::operate::SharedProgress;
use timely::progress::subgraph::SubgraphBuilderT;
use timely::progress::timestamp::Refines;
use timely::scheduling::Schedule;
use timely::worker::AsWorker;

struct ThreadStatSubgraphBuilder<SG> {
    inner: SG,
}

impl<S: Schedule> Schedule for ThreadStatSubgraphBuilder<S> {
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn path(&self) -> &[usize] {
        self.inner.path()
    }

    fn schedule(&mut self) -> bool {
        let start = std::time::Instant::now();
        let stats = stats::Stats::from_self();
        let done = self.inner.schedule();
        let elapsed = start.elapsed();
        if elapsed >= Duration::from_millis(10) {
            let stats_after = stats::Stats::from_self();
            if let (Ok(stats), Ok(stats_after)) = (stats, stats_after) {
                println!("schedule delta utime {}\tdelta stime {}\telapsed {elapsed:?}",
                         stats_after.utime - stats.utime,
                         stats_after.stime - stats.stime);
            }
        }
        done
    }
}

impl<TOuter: Timestamp, OP: Operate<TOuter>> Operate<TOuter> for ThreadStatSubgraphBuilder<OP> {
    fn local(&self) -> bool {
        self.inner.local()
    }

    fn inputs(&self) -> usize {
        self.inner.inputs()
    }

    fn outputs(&self) -> usize {
        self.inner.outputs()
    }

    fn get_internal_summary(&mut self) -> (Vec<Vec<Antichain<TOuter::Summary>>>, Rc<RefCell<SharedProgress<TOuter>>>) {
        self.inner.get_internal_summary()
    }

    fn set_external_summary(&mut self) {
        self.inner.set_external_summary();
    }

    fn notify_me(&self) -> bool {
        self.inner.notify_me()
    }
}

impl<TOuter, TInner, SG> SubgraphBuilderT<TOuter, TInner> for ThreadStatSubgraphBuilder<SG>
where
    TOuter: Timestamp,
    TInner: Timestamp,
    SG: SubgraphBuilderT<TOuter, TInner>,
{
    type Subgraph = ThreadStatSubgraphBuilder<SG::Subgraph>;

    fn new_from(path: Rc<[usize]>, identifier: usize, logging: Option<TimelyLogger>, summary_logging: Option<TimelySummaryLogger<TInner::Summary>>, name: &str) -> Self {
        Self { inner: SG::new_from(path, identifier, logging, summary_logging, name)}
    }

    fn build<A: AsWorker>(self, worker: &mut A) -> Self::Subgraph {
        ThreadStatSubgraphBuilder{ inner: self.inner.build(worker) }
    }

    fn name(&self) -> &str {
        self.inner.name()
    }

    fn path(&self) -> Rc<[usize]> {
        self.inner.path()
    }

    fn connect(&mut self, source: Source, target: Target) {
        self.inner.connect(source, target)
    }

    fn add_child(&mut self, child: Box<dyn Operate<TInner>>, index: usize, identifier: usize) {
        self.inner.add_child(child, index, identifier)
    }

    fn allocate_child_id(&mut self) -> usize {
        self.inner.allocate_child_id()
    }

    fn new_input(&mut self, shared_counts: Rc<RefCell<ChangeBatch<TInner>>>) -> Target
    where
        TInner: Refines<TOuter>
    {
        self.inner.new_input(shared_counts)
    }

    fn new_output(&mut self) -> Source
    where
        TInner: Refines<TOuter>
    {
        self.inner.new_output()
    }
}

pub mod stats {
    use std::str::FromStr;

    /// based on https://elixir.bootlin.com/linux/v5.19.17/source/fs/proc/array.c#L567
    #[derive(Debug)]
    pub struct Stats {
        pub pid: usize,
        pub name: String,
        pub state: char,
        pub ppid: isize,
        pub pgid: isize,
        pub psid: isize,
        pub tty_nr: isize,
        pub tty_grp: isize,
        pub flags: usize,
        pub min_flt: usize,
        pub cmin_flt: usize,
        pub maj_flt: usize,
        pub cmaj_flt: usize,
        pub utime: usize,
        pub stime: usize,
        pub cutime: isize,
        pub cstime: isize,
        pub priority: isize,
        pub nice: isize,
        pub num_threads: isize,
        pub _zero0: usize,
        pub start_time: usize,
        pub vsize: usize,
        pub rss: usize,
        pub rsslim: usize,
        pub start_code: usize,
        pub end_code: usize,
        pub start_stack: usize,
        pub esp: usize,
        pub eip: usize,
        pub pending: usize,
        pub blocked: usize,
        pub sigign: usize,
        pub sigcatch: usize,
        pub wchan: usize,
        pub _zero1: usize,
        pub _zero2: usize,
        pub exit_signal: isize,
        pub task_cpu: isize,
        pub rt_priority: isize,
        pub policy: isize,
        pub blkio_ticks: usize,
        pub gtime: usize,
        pub cgtime: isize,
        pub start_data: usize,
        pub end_data: usize,
        pub start_brk: usize,
        pub arg_start: usize,
        pub arg_end: usize,
        pub env_start: usize,
        pub env_end: usize,
        pub exit_code: isize,
    }

    pub enum Error {
        Underflow,
        ParseIntError(std::num::ParseIntError),
        IOError(std::io::Error),
    }

    impl From<Option<&str>> for Error {
        fn from(_: Option<&str>) -> Self {
            Error::Underflow
        }
    }

    impl From<std::num::ParseIntError> for Error {
        fn from(e: std::num::ParseIntError) -> Self {
            Error::ParseIntError(e)
        }
    }

    impl From<std::io::Error> for Error {
        fn from(value: std::io::Error) -> Self {
            Error::IOError(value)
        }
    }

    impl FromStr for Stats {
        type Err = Error;

        fn from_str(s: &str) -> Result<Self, Self::Err> {
            let mut split = s.split_whitespace();
            Ok(Self {
                pid: split.next().ok_or(Error::Underflow)?.parse()?,
                name: split.next().ok_or(Error::Underflow)?.to_string(),
                state: split.next().ok_or(Error::Underflow)?.chars().next().ok_or(Error::Underflow)?,
                ppid: split.next().ok_or(Error::Underflow)?.parse()?,
                pgid: split.next().ok_or(Error::Underflow)?.parse()?,
                psid: split.next().ok_or(Error::Underflow)?.parse()?,
                tty_nr: split.next().ok_or(Error::Underflow)?.parse()?,
                tty_grp: split.next().ok_or(Error::Underflow)?.parse()?,
                flags: split.next().ok_or(Error::Underflow)?.parse()?,
                min_flt: split.next().ok_or(Error::Underflow)?.parse()?,
                cmin_flt: split.next().ok_or(Error::Underflow)?.parse()?,
                maj_flt: split.next().ok_or(Error::Underflow)?.parse()?,
                cmaj_flt: split.next().ok_or(Error::Underflow)?.parse()?,
                utime: split.next().ok_or(Error::Underflow)?.parse()?,
                stime: split.next().ok_or(Error::Underflow)?.parse()?,
                cutime: split.next().ok_or(Error::Underflow)?.parse()?,
                cstime: split.next().ok_or(Error::Underflow)?.parse()?,
                priority: split.next().ok_or(Error::Underflow)?.parse()?,
                nice: split.next().ok_or(Error::Underflow)?.parse()?,
                num_threads: split.next().ok_or(Error::Underflow)?.parse()?,
                _zero0: split.next().ok_or(Error::Underflow)?.parse()?,
                // constant 0,
                start_time: split.next().ok_or(Error::Underflow)?.parse()?,
                vsize: split.next().ok_or(Error::Underflow)?.parse()?,
                rss: split.next().ok_or(Error::Underflow)?.parse()?,
                rsslim: split.next().ok_or(Error::Underflow)?.parse()?,
                start_code: split.next().ok_or(Error::Underflow)?.parse()?,
                end_code: split.next().ok_or(Error::Underflow)?.parse()?,
                start_stack: split.next().ok_or(Error::Underflow)?.parse()?,
                esp: split.next().ok_or(Error::Underflow)?.parse()?,
                eip: split.next().ok_or(Error::Underflow)?.parse()?,
                pending: split.next().ok_or(Error::Underflow)?.parse()?,
                blocked: split.next().ok_or(Error::Underflow)?.parse()?,
                sigign: split.next().ok_or(Error::Underflow)?.parse()?,
                sigcatch: split.next().ok_or(Error::Underflow)?.parse()?,
                wchan: split.next().ok_or(Error::Underflow)?.parse()?,
                _zero1: split.next().ok_or(Error::Underflow)?.parse()?,
                // constant 0,
                _zero2: split.next().ok_or(Error::Underflow)?.parse()?,
                // constant 0,
                exit_signal: split.next().ok_or(Error::Underflow)?.parse()?,
                task_cpu: split.next().ok_or(Error::Underflow)?.parse()?,
                rt_priority: split.next().ok_or(Error::Underflow)?.parse()?,
                policy: split.next().ok_or(Error::Underflow)?.parse()?,
                blkio_ticks: split.next().ok_or(Error::Underflow)?.parse()?,
                gtime: split.next().ok_or(Error::Underflow)?.parse()?,
                cgtime: split.next().ok_or(Error::Underflow)?.parse()?,
                start_data: split.next().ok_or(Error::Underflow)?.parse()?,
                end_data: split.next().ok_or(Error::Underflow)?.parse()?,
                start_brk: split.next().ok_or(Error::Underflow)?.parse()?,
                arg_start: split.next().ok_or(Error::Underflow)?.parse()?,
                arg_end: split.next().ok_or(Error::Underflow)?.parse()?,
                env_start: split.next().ok_or(Error::Underflow)?.parse()?,
                env_end: split.next().ok_or(Error::Underflow)?.parse()?,
                exit_code: split.next().ok_or(Error::Underflow)?.parse()?,
            })
        }
    }

    impl Stats {
        pub fn from_self() -> Result<Self, Error> {
            let mut buffer = String::new();
            use std::io::Read;
            std::fs::File::open("/proc/thread-self/stat")?.read_to_string(&mut buffer)?;
            buffer.parse()
        }
    }
}

fn main() {
    // initializes and runs a timely dataflow.
    timely::execute_from_args(std::env::args(), |worker| {

        let timer = std::time::Instant::now();

        let mut args = std::env::args();
        args.next();

        let dataflows = args.next().unwrap().parse::<usize>().unwrap();
        let length = args.next().unwrap().parse::<usize>().unwrap();
        let record = args.next() == Some("record".to_string());

        let mut inputs = Vec::new();
        let mut probes = Vec::new();

        // create a new input, exchange data, and inspect its output
        for _dataflow in 0 .. dataflows {
            let logging = worker.log_register().get("timely").map(Into::into);
            worker.dataflow_subgraph::<_, _, _, _, ThreadStatSubgraphBuilder<SubgraphBuilder<_, _>>>("Dataflow", logging, (), |(), scope| {
                let (input, mut stream) = scope.new_input();
                for _step in 0 .. length {
                    stream = stream.map(|x: ()| {
                        // Simluate CPU intensive task
                        for i in 0..1_000_000 {
                            std::hint::black_box(i);
                        }
                        // If we were to sleep here, `utime` would not increase.
                        x
                    });
                }
                let probe = stream.probe();
                inputs.push(input);
                probes.push(probe);
            });
        }

        println!("{:?}\tdataflows built ({} x {})", timer.elapsed(), dataflows, length);

        for round in 0 .. 10_000  {
            let dataflow = round % dataflows;
            if record {
                inputs[dataflow].send(());
            }
            inputs[dataflow].advance_to(round);
            let mut steps = 0;
            while probes[dataflow].less_than(&round) {
                worker.step();
                steps += 1;
            }
            println!("{:?}\tround {} complete in {} steps", timer.elapsed(), round, steps);
        }

    }).unwrap();
}
