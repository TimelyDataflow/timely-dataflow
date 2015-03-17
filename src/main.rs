#![feature(unsafe_destructor)]
#![feature(test)]
#![feature(alloc)]
#![feature(core)]
#![feature(std_misc)]
#![feature(collections)]
#![feature(old_io)]
#![feature(io)]
#![feature(net)]
#![feature(hash)]
#![feature(libc)]

#![allow(dead_code)]
#![allow(missing_copy_implementations)]

extern crate core;
extern crate test;
extern crate columnar;
extern crate byteorder;

extern crate docopt;
use docopt::Docopt;

use test::Bencher;

use columnar::Columnar;

use progress::{Graph, Scope};
use progress::subgraph::new_graph;
use progress::subgraph::Summary::Local;
use progress::subgraph::Source::ScopeOutput;
use progress::subgraph::Target::ScopeInput;
use progress::broadcast::Progcaster;
use communication::{ThreadCommunicator, ProcessCommunicator, Communicator};

use communication::channels::Data;
use std::hash::Hash;
use core::fmt::Debug;

use example::stream::Stream;
use example::input::InputExtensionTrait;
use example::concat::ConcatExtensionTrait;
use example::feedback::FeedbackExtensionTrait;
use example::distinct::DistinctExtensionTrait;
use example::command::CommandExtensionTrait;

use example::graph_builder::{EnterSubgraphExt, LeaveSubgraphExt};
use example::barrier::BarrierScope;

use std::rc::{Rc, try_unwrap};
use std::cell::RefCell;

use std::thread;

use networking::initialize_networking;

mod progress;
mod example;
mod networking;
mod communication;

static USAGE: &'static str = "
Usage: timely distinct [options] [<arguments>...]
       timely barrier [options] [<arguments>...]
       timely command [options] [<arguments>...]

Options:
    -w <arg>, --workers <arg>    number of workers per process [default: 1]
    -p <arg>, --processid <arg>  identity of this process      [default: 0]
    -n <arg>, --processes <arg>  number of processes involved  [default: 1]
";

fn main() {
    let args = Docopt::new(USAGE).and_then(|dopt| dopt.parse()).unwrap_or_else(|e| e.exit());

    let workers: u64 = if let Ok(threads) = args.get_str("-w").parse() { threads }
                       else { panic!("invalid setting for --workers: {}", args.get_str("-t")) };
    let process_id: u64 = if let Ok(proc_id) = args.get_str("-p").parse() { proc_id }
                          else { panic!("invalid setting for --processid: {}", args.get_str("-p")) };
    let processes: u64 = if let Ok(processes) = args.get_str("-n").parse() { processes }
                         else { panic!("invalid setting for --processes: {}", args.get_str("-n")) };

    println!("Hello, world!");
    println!("Starting timely with\n\tworkers:\t{}\n\tprocesses:\t{}\n\tprocessid:\t{}", workers, processes, process_id);

    // vector holding communicators to use; one per local worker.
    if processes > 1 {
        let addresses = (0..processes).map(|index| format!("localhost:{}", 2101 + index).to_string()).collect();
        let communicators = initialize_networking(addresses, process_id, workers).ok().expect("error initializing networking");
        if args.get_bool("distinct") { _distinct_multi(communicators); }
        else if args.get_bool("barrier") { _barrier_multi(communicators); }
        else if args.get_bool("command") { _command_multi(communicators); }
    }
    else if workers > 1 {
        let communicators = ProcessCommunicator::new_vector(workers);
        if args.get_bool("distinct") { _distinct_multi(communicators); }
        else if args.get_bool("barrier") { _barrier_multi(communicators); }
        else if args.get_bool("command") { _command_multi(communicators); }
    }
    else {
        let communicators = vec![ThreadCommunicator];
        if args.get_bool("distinct") { _distinct_multi(communicators); }
        else if args.get_bool("barrier") { _barrier_multi(communicators); }
        else if args.get_bool("command") { _command_multi(communicators); }
    };
}

// fn _networking(my_id: u64, threads: u64, processes: u64) {
//     let addresses = range(0, processes).map(|index| format!("localhost:{}", 2101 + index).to_string()).collect();
//     let _connections = initialize_networking(addresses, my_id, threads);
// }

#[bench]
fn distinct_bench(bencher: &mut Bencher) { _distinct(ProcessCommunicator::new_vector(1).swap_remove(0), Some(bencher)); }
fn _distinct_multi<C: Communicator+Send>(communicators: Vec<C>) {
    let mut guards = Vec::new();
    for communicator in communicators.into_iter() {
        guards.push(thread::Builder::new().name(format!("worker thread {}", communicator.index()))
                                          .scoped(move || _distinct(communicator, None))
                                          .unwrap());
    }
}

// #[bench]
// fn command_bench(bencher: &mut Bencher) { _command(ProcessCommunicator::new_vector(1).swap_remove(0).unwrap(), Some(bencher)); }
fn _command_multi<C: Communicator+Send>(communicators: Vec<C>) {
    let mut guards = Vec::new();
    for communicator in communicators.into_iter() {
        guards.push(thread::scoped(move || _command(communicator, None)));
    }
}


#[bench]
fn barrier_bench(bencher: &mut Bencher) { _barrier(ProcessCommunicator::new_vector(1).swap_remove(0), Some(bencher)); }
fn _barrier_multi<C: Communicator+Send>(communicators: Vec<C>) {
    let mut guards = Vec::new();
    for communicator in communicators.into_iter() {
        guards.push(thread::scoped(move || _barrier(communicator, None)));
    }
}

fn _create_subgraph<G: Graph, C: Communicator, D: Data+Hash+Eq+Debug+Columnar>( graph: &mut G, source1: &mut Stream<G, D, C>, source2: &mut Stream<G, D, C>) -> (Stream<G, D, C>, Stream<G, D, C>) {
    // build up a subgraph using the concatenated inputs/feedbacks
    let subgraph = Rc::new(RefCell::new(graph.new_subgraph::<u64>(0, Progcaster::new(&mut source1.allocator))));

    let sub_egress1 = source1.enter(&subgraph).distinct().leave(graph);
    let sub_egress2 = source2.enter(&subgraph).leave(graph);

    // sort of a mess, but the way to get the subgraph out of the Rc<RefCell<_>>.
    // will explode if anyone else is still sitting on a reference to subgraph.
    graph.add_scope(try_unwrap(subgraph).ok().expect("hm").into_inner());

    return (sub_egress1, sub_egress2);
}

fn _distinct<C: Communicator>(allocator: C, bencher: Option<&mut Bencher>) {
    let mut allocator = Rc::new(RefCell::new(allocator));

    // no "base scopes" yet, so the root pretends to be a subscope of some parent with a () timestamp type.
    let mut graph = new_graph(Progcaster::new(&mut allocator));

    // try building some input scopes
    let (mut input1, mut stream1) = graph.new_input::<u64>(allocator.clone());
    let (mut input2, mut stream2) = graph.new_input::<u64>(allocator.clone());

    // prepare some feedback edges
    let (mut feedback1, mut feedback1_output) = stream1.feedback(((), 100000), Local(1));
    let (mut feedback2, mut feedback2_output) = stream2.feedback(((), 100000), Local(1));

    // build up a subgraph using the concatenated inputs/feedbacks
    let (mut egress1, mut egress2) = _create_subgraph(&mut graph.clone(),
                                                      &mut stream1.concat(&mut feedback1_output),
                                                      &mut stream2.concat(&mut feedback2_output));

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
        Some(b) => b.iter(|| { graph.borrow_mut().pull_internal_progress(&mut Vec::new(), &mut Vec::new(), &mut Vec::new()); }),
        None    => while graph.borrow_mut().pull_internal_progress(&mut Vec::new(), &mut Vec::new(), &mut Vec::new()) { }
    }
}

fn _command<C: Communicator>(allocator: C, bencher: Option<&mut Bencher>) {
    let allocator = Rc::new(RefCell::new(allocator));

    // no "base scopes" yet, so the root pretends to be a subscope of some parent with a () timestamp type.
    let mut graph = new_graph(Progcaster::new(&mut (*allocator.borrow_mut())));
    let mut input = graph.new_input::<u64>(allocator);
    let mut feedback = input.1.feedback(((), 1000), Local(1));
    let mut result: Stream<_, u64, _> = input.1.concat(&mut feedback.1)
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

fn _barrier<C: Communicator>(mut allocator: C, bencher: Option<&mut Bencher>) {
    let mut graph = new_graph(Progcaster::new(&mut allocator));
    graph.add_scope(BarrierScope { epoch: 0, ready: true, degree: allocator.peers(), ttl: 1000000 });
    graph.connect(ScopeOutput(0, 0), ScopeInput(0, 0));

    // start things up!
    graph.borrow_mut().get_internal_summary();
    graph.borrow_mut().set_external_summary(Vec::new(), &mut Vec::new());
    graph.borrow_mut().push_external_progress(&mut Vec::new());

    // spin
    match bencher {
        Some(b) => b.iter(|| { graph.borrow_mut().pull_internal_progress(&mut Vec::new(), &mut Vec::new(), &mut Vec::new()); }),
        None    => while graph.borrow_mut().pull_internal_progress(&mut Vec::new(), &mut Vec::new(), &mut Vec::new()) { },
    }
}
