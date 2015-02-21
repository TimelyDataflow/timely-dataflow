#![feature(core)]
#![feature(alloc)]
#![feature(unsafe_destructor)]

extern crate timely;
extern crate core;

extern crate docopt;
use docopt::Docopt;

use timely::communication::Communicator;
use timely::communication::channels::Data;
use timely::progress::subgraph::new_graph;
use timely::progress::broadcast::Progcaster;
use timely::progress::subgraph::Summary::Local;
use timely::progress::scope::Scope;
use timely::progress::graph::Graph;
use timely::example::input::InputExtensionTrait;
use timely::example::concat::ConcatExtensionTrait;
use timely::example::feedback::FeedbackExtensionTrait;
use timely::example::distinct::DistinctExtensionTrait;
use timely::example::stream::Stream;
use timely::example::graph_builder::GraphBoundary;
use timely::networking::initialize_networking;

use core::fmt::Debug;
use std::thread;

use std::rc::{Rc, try_unwrap};
use std::cell::RefCell;
use std::hash::Hash;

static USAGE: &'static str = "
Usage: networked [options] [<arguments>...]

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

    let addresses = range(0, processes).map(|index| format!("localhost:{}", 2101 + index).to_string()).collect();
    let network_communicator = initialize_networking(addresses, process_id, threads).ok().expect("error initializing networking");

    let mut guards = Vec::new();
    for communicator in network_communicator.into_iter() {
        guards.push(thread::scoped(move || _queue(communicator)));
    }
}


fn _queue(allocator: Communicator) {
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
    while graph.borrow_mut().pull_internal_progress(&mut Vec::new(), &mut Vec::new(), &mut Vec::new()) { }
}

fn _create_subgraph<G: Graph, D: Data+Hash+Eq+Debug>(graph: &mut G,
                                 source1: &mut Stream<G, D>,
                                 source2: &mut Stream<G, D>,
                                 progcaster: Progcaster<(G::Timestamp,u64)>)
                            -> (Stream<G, D>, Stream<G, D>) {
    // build up a subgraph using the concatenated inputs/feedbacks
    let mut subgraph = Rc::new(RefCell::new(graph.new_subgraph::<u64>(0, progcaster)));

    let (sub_egress1, sub_egress2) = {
        // create new ingress nodes, passing in a reference to the subgraph for them to use.
        let mut sub_ingress1 = subgraph.add_input(source1);
        let mut sub_ingress2 = subgraph.add_input(source2);

        // putting a distinct'ing scope into the subgraph!
        let mut distinct = sub_ingress1.distinct();

        // egress each of the streams from the subgraph.
        let sub_egress1 = subgraph.add_output_to_graph(&mut distinct, graph);
        let sub_egress2 = subgraph.add_output_to_graph(&mut sub_ingress2, graph);

        (sub_egress1, sub_egress2)
    };

    // sort of a mess, but the way to get the subgraph out of the Rc<RefCell<...>>.
    // will explode if anyone else is still sitting on a reference to subgraph.
    graph.add_scope(try_unwrap(subgraph).ok().expect("hm").into_inner());

    return (sub_egress1, sub_egress2);
}
