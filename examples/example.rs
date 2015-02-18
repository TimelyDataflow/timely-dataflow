#![feature(test)]
#![feature(core)]
#![feature(hash)]
#![feature(alloc)]
#![feature(std_misc)]
#![feature(unsafe_destructor)]

extern crate core;
extern crate timely;
extern crate test;
// extern crate serialize;
extern crate columnar;

extern crate docopt;
use docopt::Docopt;

use test::Bencher;

use timely::progress::{Timestamp, PathSummary, Graph, Scope};
use timely::progress::graph::GraphExtension;
use timely::progress::subgraph::{Subgraph, Summary, new_graph};
use timely::progress::subgraph::Summary::Local;
use timely::progress::subgraph::Source::ScopeOutput;
use timely::progress::subgraph::Target::ScopeInput;
use timely::progress::broadcast::Progcaster;
use timely::communication::{ProcessCommunicator, Communicator};
use timely::communication::channels::Data;

use std::hash::{Hash, SipHasher};
use core::fmt::Debug;

use timely::example::input::{InputExtensionTrait, InputHelper};
use timely::example::concat::{ConcatExtensionTrait};
use timely::example::feedback::FeedbackExtensionTrait;
// use timely::example::queue::{QueueExtensionTrait};
use timely::example::distinct::{DistinctExtensionTrait};
use timely::example::stream::Stream;
use timely::example::command::{CommandExtensionTrait};

use timely::example::graph_builder::GraphBoundary;
use timely::example::barrier::BarrierScope;

use std::rc::{Rc, try_unwrap};
use std::cell::RefCell;

use std::thread::Thread;

use timely::networking::initialize_networking;

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

fn main() {
    let args = Docopt::new(USAGE).and_then(|dopt| dopt.parse()).unwrap_or_else(|e| e.exit());

    let threads: u64 = if let Ok(threads) = args.get_str("-t").parse() { threads }
                  else { panic!("invalid setting for --threads: {}", args.get_str("-t")) };
    let process_id: u64 = if let Ok(proc_id) = args.get_str("-p").parse() { proc_id }
                     else { panic!("invalid setting for --processid: {}", args.get_str("-p")) };
    let processes: u64 = if let Ok(processes) = args.get_str("-n").parse() { processes }
                         else { panic!("invalid setting for --processes: {}", args.get_str("-n")) };

    println!("Hello, world!");
    println!("Starting timely with\n\tthreads:\t{}\n\tprocesses:\t{}\n\tprocessid:\t{}", threads, processes, process_id);

    if args.get_bool("queue") {
        _queue_multi(threads); println!("started queue test");
    }
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
fn queue_bench(bencher: &mut Bencher) { _queue(ProcessCommunicator::new_vector(1).swap_remove(0), Some(bencher)); }
fn _queue_multi(threads: u64) {
    let mut guards = Vec::new();
    for communicator in ProcessCommunicator::new_vector(threads).into_iter() {
        guards.push(Thread::scoped(move || _queue(communicator, None)));
    }
}


// #[bench]
// fn command_bench(bencher: &mut Bencher) { _command(ProcessCommunicator::new_vector(1).swap_remove(0).unwrap(), Some(bencher)); }
fn _command_multi(threads: u64) {
    let mut guards = Vec::new();
    for communicator in ProcessCommunicator::new_vector(threads).into_iter() {
        guards.push(Thread::scoped(move || _command(communicator, None)));
    }
}


#[bench]
fn barrier_bench(bencher: &mut Bencher) { _barrier(ProcessCommunicator::new_vector(1).swap_remove(0), Some(bencher)); }
fn _barrier_multi(threads: u64) {
    let mut guards = Vec::new();
    for communicator in ProcessCommunicator::new_vector(threads).into_iter() {
        guards.push(Thread::scoped(move || _barrier(communicator, None)));
    }
}

fn _create_subgraph<T1, T2, S1, S2, D>(graph: &mut Rc<RefCell<Subgraph<T1, S1, T2, S2>>>,
                                       source1: &mut Stream<(T1, T2), Summary<S1, S2>, D>,
                                       source2: &mut Stream<(T1, T2), Summary<S1, S2>, D>,
                                       progcaster: Progcaster<((T1,T2),u64)>)
                                        -> (Stream<(T1, T2), Summary<S1, S2>, D>, Stream<(T1, T2), Summary<S1, S2>, D>)
where T1: Timestamp, S1: PathSummary<T1>,
      T2: Timestamp, S2: PathSummary<T2>,
      D:  Data+Hash<SipHasher>+Eq+Debug,
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

fn _queue(allocator: Communicator, bencher: Option<&mut Bencher>) {
    let allocator = Rc::new(RefCell::new(allocator));
    // no "base scopes" yet, so the root pretends to be a subscope of some parent with a () timestamp type.
    let mut graph = new_graph(Progcaster::new(&mut (*allocator.borrow_mut())));

    // try building some input scopes
    let (mut input1, mut stream1) = graph.new_input::<u64>(allocator.clone());
    let (mut input2, mut stream2) = graph.new_input::<u64>(allocator.clone());

    // prepare some feedback edges
    let (mut feedback1, mut feedback1_output) = stream1.feedback(((), 1000), Local(1));
    let (mut feedback2, mut feedback2_output) = stream2.feedback(((), 1000), Local(1));

    // build up a subgraph using the concatenated inputs/feedbacks
    let progcaster = Progcaster::new(&mut (*allocator.borrow_mut()));
    let (mut egress1, mut egress2) = _create_subgraph(&mut graph.clone(),
                                                      &mut stream1.concat(&mut feedback1_output),
                                                      &mut stream2.concat(&mut feedback2_output),
                                                      progcaster);

    // connect feedback sources. notice that we have swapped indices ...
    feedback1.connect_input(&mut egress2);
    feedback2.connect_input(&mut egress1);

    // finalize the graph/subgraph
    graph.borrow_mut().get_internal_summary();
    graph.borrow_mut().set_external_summary(Vec::new(), &mut Vec::new());

    // do one round of push progress, pull progress ...
    graph.borrow_mut().push_external_progress(&mut Vec::new());
    graph.borrow_mut().pull_internal_progress(&mut Vec::new(), &mut Vec::new(), &mut Vec::new());

    // move some data into the dataflow graph.
    input1.send_messages(&((), 0), vec![1u64]);
    input2.send_messages(&((), 0), vec![2u64]);

    // see what everyone thinks about that ...
    graph.borrow_mut().pull_internal_progress(&mut Vec::new(), &mut Vec::new(), &mut Vec::new());

    input1.advance(&((), 0), &((), 1000000));
    input2.advance(&((), 0), &((), 1000000));
    input1.close_at(&((), 1000000));
    input2.close_at(&((), 1000000));

    // spin
    match bencher {
        Some(bencher) => bencher.iter(|| { graph.borrow_mut().pull_internal_progress(&mut Vec::new(), &mut Vec::new(), &mut Vec::new()); }),
        None          => while graph.borrow_mut().pull_internal_progress(&mut Vec::new(), &mut Vec::new(), &mut Vec::new()) { }
    }
}

fn _command(allocator: Communicator, bencher: Option<&mut Bencher>) {
    let allocator = Rc::new(RefCell::new(allocator));

    // no "base scopes" yet, so the root pretends to be a subscope of some parent with a () timestamp type.
    let mut graph: Rc<RefCell<Subgraph<(), (), u64, u64>>> = new_graph(Progcaster::new(&mut (*allocator.borrow_mut())));
    let mut input: (InputHelper<((), u64), u64>, _) = graph.new_input(allocator);
    let mut feedback = input.1.feedback(((), 1000), Local(1));
    let mut result: Stream<((), u64), Summary<(), u64>, u64> = input.1.concat(&mut feedback.1)
                            .command("./target/release/command".to_string());

    feedback.0.connect_input(&mut result);

    // start things up!
    graph.borrow_mut().get_internal_summary();
    graph.borrow_mut().set_external_summary(Vec::new(), &mut Vec::new());
    graph.borrow_mut().push_external_progress(&mut Vec::new());

    input.0.close_at(&((), 0));

    // spin
    match bencher {
        Some(b) => b.iter(|| { graph.borrow_mut().pull_internal_progress(&mut Vec::new(), &mut Vec::new(), &mut Vec::new()); }),
        None    => while graph.borrow_mut().pull_internal_progress(&mut Vec::new(), &mut Vec::new(), &mut Vec::new()) { },
    }
}

fn _barrier(mut allocator: Communicator, bencher: Option<&mut Bencher>) {
    let mut graph: Rc<RefCell<Subgraph<(), (), u64, u64>>> = new_graph(Progcaster::new(&mut allocator));
    graph.add_scope(BarrierScope { epoch: 0, ready: true, degree: allocator.peers(), ttl: 10000000 });
    graph.connect(ScopeOutput(0, 0), ScopeInput(0, 0));

    // start things up!
    graph.borrow_mut().get_internal_summary();
    graph.borrow_mut().set_external_summary(Vec::new(), &mut Vec::new());
    graph.borrow_mut().push_external_progress(&mut Vec::new());

    // spin
    match bencher
    {
        Some(bencher) => bencher.iter(|| { graph.borrow_mut().pull_internal_progress(&mut Vec::new(), &mut Vec::new(), &mut Vec::new()); }),
        None          => while graph.borrow_mut().pull_internal_progress(&mut Vec::new(), &mut Vec::new(), &mut Vec::new()) { },
    }
}
