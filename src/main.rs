#![feature(unsafe_destructor)]

extern crate core;
extern crate test;
extern crate serialize;
extern crate columnar;

extern crate docopt;
use docopt::Docopt;

use test::Bencher;

use std::default::Default;

use progress::{Timestamp, PathSummary, Graph, Scope};
use progress::graph::GraphExtension;
use progress::subgraph::{Subgraph, Summary, new_graph};
use progress::subgraph::Summary::Local;
use progress::subgraph::Source::ScopeOutput;
use progress::subgraph::Target::ScopeInput;

use progress::broadcast::{Progcaster, MultiThreadedBroadcaster};
use communication::ChannelAllocator;

use communication::channels::Data;
use std::hash::{Hash, SipHasher};
use std::collections::hash_map::Hasher;
use core::fmt::Show;

use example::input::{InputExtensionTrait, InputHelper};
use example::concat::{ConcatExtensionTrait};
use example::feedback::{FeedbackExtensionTrait};
use example::queue::{QueueExtensionTrait};
use example::distinct::{DistinctExtensionTrait};
use example::stream::Stream;
use example::command::{CommandExtensionTrait};

use example::graph_builder::GraphBoundary;
use example::barrier::BarrierScope;

use std::rc::{Rc, try_unwrap};
use std::cell::RefCell;

use std::thread::Thread;

// use std::os::args;

use networking::initialize_networking;

mod progress;
mod example;
mod networking;
mod communication;

static USAGE: &'static str = "
Usage: timely queue [options] [<arguments>...]
       timely barrier [options] [<arguments>...]
       timely command [options] [<arguments>...]
       timely networking [options] [<arguments>...]

Options:
    -t <arg>, --threads <arg>    number of threads per worker [default: 1]
    -p <arg>, --processid <arg>  identity of this process     [default: 0]
    -n <arg>, --processes <arg>  number of processes involved [default: 1]
";

fn main()
{
    let args = Docopt::new(USAGE).and_then(|dopt| dopt.parse()).unwrap_or_else(|e| e.exit());

    let threads = if let Some(threads) = args.get_str("-t").parse() { threads }
                  else { panic!("invalid setting for --threads: {}", args.get_str("-t")) };
    let process_id = if let Some(proc_id) = args.get_str("-p").parse() { proc_id }
                     else { panic!("invalid setting for --processid: {}", args.get_str("-p")) };
    let processes: u64 = if let Some(processes) = args.get_str("-n").parse() { processes }
                    else { panic!("invalid setting for --processes: {}", args.get_str("-n")) };

    // let threads = 3u;
    // let process_id = 0u;
    // let processes = 1u;

    println!("Hello, world!");

    println!("Starting timely with\n\tthreads:\t{}\n\tprocesses:\t{}\n\tprocessid:\t{}", threads, processes, process_id);

    // _queue_multi(threads); println!("started queue test");
    // _barrier_multi(threads); println!("started barrier test");
    // _networking(process_id, processes);

    if args.get_bool("queue") { _queue_multi(threads); println!("started queue test"); }
    if args.get_bool("barrier") { _barrier_multi(threads); println!("started barrier test"); }
    if args.get_bool("command") { _command_multi(threads); println!("started command test"); }

    if args.get_bool("networking") {
        if processes > 1u64 {
            println!("testing networking with {} processes", processes);
            _networking(process_id, threads, processes);
        }
        else { println!("set --processes > 1 for networking test"); }
    }
}

fn _networking(my_id: u64, threads: u64, processes: u64) {
    let addresses = range(0, processes).map(|index| format!("localhost:{}", 2101 + index).to_string()).collect();
    let _connections = initialize_networking(addresses, my_id, threads);
}

#[bench]
fn queue_bench(bencher: &mut Bencher) { _queue(ChannelAllocator::new_vector(1).swap_remove(0), Some(bencher)); }
fn _queue_multi(threads: u64) {
    let mut guards = Vec::new();
    for allocator in ChannelAllocator::new_vector(threads).into_iter() {
        guards.push(Thread::spawn(move || _queue(allocator, None)));
    }
}


// #[bench]
// fn command_bench(bencher: &mut Bencher) { _command(ChannelAllocator::new_vector(1).swap_remove(0).unwrap(), Some(bencher)); }
fn _command_multi(threads: u64) {
    let mut guards = Vec::new();
    for allocator in ChannelAllocator::new_vector(threads).into_iter() {
        guards.push(Thread::spawn(move || _command(allocator, None)));
    }
}


#[bench]
fn barrier_bench(bencher: &mut Bencher) { _barrier(ChannelAllocator::new_vector(1).swap_remove(0), Some(bencher)); }
fn _barrier_multi(threads: u64)
{
    let mut guards = Vec::new();
    for allocator in ChannelAllocator::new_vector(threads).into_iter()
    {
        guards.push(Thread::spawn(move || _barrier(allocator, None)));
    }
}

fn _create_subgraph<T1, T2, S1, S2, D>(graph: &mut Rc<RefCell<Subgraph<T1, S1, T2, S2>>>,
                                               source1: &mut Stream<(T1, T2), Summary<S1, S2>, D>,
                                               source2: &mut Stream<(T1, T2), Summary<S1, S2>, D>,
                                               progcaster: Progcaster<((T1,T2),u64)>)
                                                -> (Stream<(T1, T2), Summary<S1, S2>, D>, Stream<(T1, T2), Summary<S1, S2>, D>)
where T1: Timestamp, S1: PathSummary<T1>,
      T2: Timestamp, S2: PathSummary<T2>,
      D:  Data+Hash<SipHasher>+Eq+Show,
{
    // build up a subgraph using the concatenated inputs/feedbacks
    let mut subgraph = graph.new_subgraph::<_, u64>(0, progcaster);

    let (sub_egress1, sub_egress2) = {
        // create new ingress nodes, passing in a reference to the subgraph for them to use.
        let mut sub_ingress1 = subgraph.add_input(source1);
        let mut sub_ingress2 = subgraph.add_input(source2);

        // putting a queueing scope into the subgraph!
        let mut queue = sub_ingress1//.queue()
                                    .distinct()
                                    ;
        // egress each of the streams from the subgraph.
        let sub_egress1 = subgraph.add_output_to_graph(&mut queue, graph.as_box());
        let sub_egress2 = subgraph.add_output_to_graph(&mut sub_ingress2, graph.as_box());

        (sub_egress1, sub_egress2)
    };

    // sort of a mess, but the way to get the subgraph out of the Rc<RefCell<...>>.
    // will explode if anyone else is still sitting on a reference to subgraph.
    graph.add_scope(try_unwrap(subgraph).ok().expect("hm").into_inner());

    return (sub_egress1, sub_egress2);
}

fn _queue(allocator: ChannelAllocator, bencher: Option<&mut Bencher>) {
    let allocator = Rc::new(RefCell::new(allocator));

    // no "base scopes" yet, so the root pretends to be a subscope of some parent with a () timestamp type.
    let mut graph: Rc<RefCell<Subgraph<(), (), _, _>>> = Rc::new(RefCell::new(Default::default()));
    graph.borrow_mut().progcaster = Progcaster::Process(MultiThreadedBroadcaster::from(&mut (*allocator.borrow_mut())));

    // try building some input scopes
    let (mut input1, mut stream1) = graph.new_input(allocator.clone());
    let (mut input2, mut stream2) = graph.new_input(allocator.clone());

    // prepare some feedback edges
    let (mut feedback1, mut feedback1_output) = stream1.feedback(((), 1000000), Local(1));
    let (mut feedback2, mut feedback2_output) = stream2.feedback(((), 1000000), Local(1));

    // build up a subgraph using the concatenated inputs/feedbacks
    let progcaster = Progcaster::Process(MultiThreadedBroadcaster::from(&mut (*allocator.borrow_mut())));
    let (mut egress1, mut egress2) = _create_subgraph(&mut graph.clone(),
                                                      &mut stream1.concat(&mut feedback1_output),
                                                      &mut stream2.concat(&mut feedback2_output),
                                                      progcaster);

    // connect feedback sources. notice that we have swapped indices ...
    feedback1.connect_input(&mut egress2);
    feedback2.connect_input(&mut egress1);

    // finalize the graph/subgraph
    graph.borrow_mut().get_internal_summary();
    graph.borrow_mut().set_external_summary(Vec::new(), &Vec::new());

    // do one round of push progress, pull progress ...
    graph.borrow_mut().push_external_progress(&Vec::new());
    graph.borrow_mut().pull_internal_progress(&mut Vec::new(), &mut Vec::new(), &mut Vec::new());

    // move some data into the dataflow graph.
    input1.send_messages(&((), 0), vec![1u64]);
    input2.send_messages(&((), 0), vec![2u64]);

    // see what everyone thinks about that ...
    graph.borrow_mut().pull_internal_progress(&mut Vec::new(), &mut Vec::new(), &mut Vec::new());

    // indicate that eaah input sent a message, and won't send again until epoch 1000000.
    // obviously pretty clunky. will be important to make a more friendly interface later on.
    input1.advance(&((), 0), &((), 1000000));
    input2.advance(&((), 0), &((), 1000000));
    input1.close_at(&((), 1000000));
    input2.close_at(&((), 1000000));

    // see what everyone thinks about that ...
    graph.borrow_mut().pull_internal_progress(&mut Vec::new(), &mut Vec::new(), &mut Vec::new());

    // spin
    match bencher {
        Some(bencher) => bencher.iter(|| { graph.borrow_mut().pull_internal_progress(&mut Vec::new(), &mut Vec::new(), &mut Vec::new()); }),
        None          => while graph.borrow_mut().pull_internal_progress(&mut Vec::new(), &mut Vec::new(), &mut Vec::new()) { }
    }
}


fn _command(allocator: ChannelAllocator, bencher: Option<&mut Bencher>) {
    let allocator = Rc::new(RefCell::new(allocator));

    // no "base scopes" yet, so the root pretends to be a subscope of some parent with a () timestamp type.
    let mut graph = new_graph(Progcaster::Process(MultiThreadedBroadcaster::from(&mut (*allocator.borrow_mut()))));

    // try building some input scopes
    let mut input: (InputHelper<_, u64>, _) = graph.new_input(allocator.clone());

    // prepare some feedback edges
    let mut feedback = input.1.feedback(((), 1000), Local(1));

    // build up a subgraph using the concatenated inputs/feedbacks
    let mut result = input.1.concat(&mut feedback.1)
                            .command("./target/release/command".to_string());

    // connect feedback sources. notice that we have swapped indices ...
    feedback.0.connect_input(&mut result);

    // start things up!
    graph.borrow_mut().get_internal_summary();
    graph.borrow_mut().set_external_summary(Vec::new(), &Vec::new());
    graph.borrow_mut().push_external_progress(&Vec::new());

    input.0.close_at(&((), 0));

    // spin
    match bencher {
        Some(b) => b.iter(|| { graph.borrow_mut().pull_internal_progress(&mut Vec::new(), &mut Vec::new(), &mut Vec::new()); }),
        None    => while graph.borrow_mut().pull_internal_progress(&mut Vec::new(), &mut Vec::new(), &mut Vec::new()) { },
    }
}

fn _barrier(allocator: ChannelAllocator, bencher: Option<&mut Bencher>)
{
    let allocator = Rc::new(RefCell::new(allocator));

    // no "base scopes" yet, so the root pretends to be a subscope of some parent with a () timestamp type.
    let mut graph = new_graph(Progcaster::Process(MultiThreadedBroadcaster::from(&mut (*allocator.borrow_mut()))));

    // add a new, relatively pointless, scope.
    // It has a non-trivial path summary, so the timely dataflow graph is not improperly cyclic.
    graph.add_scope(BarrierScope { epoch: 0, ready: true, degree: allocator.borrow().multiplicity() as i64, ttl: 10000000 });

    // attach the scope's output back to its input.
    graph.connect(ScopeOutput(0, 0), ScopeInput(0, 0));

    // start things up!
    graph.borrow_mut().get_internal_summary();
    graph.borrow_mut().set_external_summary(Vec::new(), &Vec::new());
    graph.borrow_mut().push_external_progress(&Vec::new());

    // spin
    match bencher
    {
        Some(bencher) => bencher.iter(|| { graph.borrow_mut().pull_internal_progress(&mut Vec::new(), &mut Vec::new(), &mut Vec::new()); }),
        None          => while graph.borrow_mut().pull_internal_progress(&mut Vec::new(), &mut Vec::new(), &mut Vec::new()) { },
    }
}
