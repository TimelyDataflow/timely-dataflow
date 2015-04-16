#![feature(test)]
#![feature(core)]
#![feature(std_misc)]
#![feature(collections)]
#![feature(hash)]

#![allow(dead_code)]

extern crate core;
extern crate test;
extern crate columnar;
extern crate byteorder;

extern crate docopt;
use docopt::Docopt;

use test::Bencher;

use columnar::Columnar;

use progress::{Graph, Scope};
use progress::nested::subgraph::new_graph;
use progress::nested::Summary::Local;
use progress::nested::Source::ScopeOutput;
use progress::nested::Target::ScopeInput;
use progress::nested::product::Product;
use communication::{ThreadCommunicator, ProcessCommunicator, Communicator};

use communication::channels::Data;
use std::hash::Hash;
use core::fmt::Debug;

use example::*;
use example::distinct::DistinctExtensionTrait;
use example::barrier::BarrierScope;

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
    println!("Starting timely with");
    println!("\tworkers:\t{}", workers);
    println!("\tprocesses:\t{}", processes);
    println!("\tprocessid:\t{}", process_id);

    // vector holding communicators to use; one per local worker.
    if processes > 1 {
        println!("Initializing BinaryCommunicator");
        let addresses = (0..processes).map(|index| format!("localhost:{}", 2101 + index).to_string()).collect();
        let communicators = initialize_networking(addresses, process_id, workers).ok().expect("error initializing networking");
        if args.get_bool("distinct") { _distinct_multi(communicators); }
        else if args.get_bool("barrier") { _barrier_multi(communicators); }
        else if args.get_bool("command") { _command_multi(communicators); }
    }
    else if workers > 1 {
        println!("Initializing ProcessCommunicator");
        let communicators = ProcessCommunicator::new_vector(workers);
        if args.get_bool("distinct") { _distinct_multi(communicators); }
        else if args.get_bool("barrier") { _barrier_multi(communicators); }
        else if args.get_bool("command") { _command_multi(communicators); }
    }
    else {
        println!("Initializing ThreadCommunicator");
        let communicators = vec![ThreadCommunicator];
        if args.get_bool("distinct") { _distinct_multi(communicators); }
        else if args.get_bool("barrier") { _barrier_multi(communicators); }
        else if args.get_bool("command") { _command_multi(communicators); }
    };
}

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
fn _command_multi<C: Communicator+Send>(_communicators: Vec<C>) {
    println!("command currently disabled awaiting io reform");
    // let mut guards = Vec::new();
    // for communicator in communicators.into_iter() {
    //     guards.push(thread::scoped(move || _command(communicator, None)));
    // }
}


#[bench]
fn barrier_bench(bencher: &mut Bencher) { _barrier(ProcessCommunicator::new_vector(1).swap_remove(0), Some(bencher)); }
fn _barrier_multi<C: Communicator+Send>(communicators: Vec<C>) {
    let mut guards = Vec::new();
    for communicator in communicators.into_iter() {
        guards.push(thread::scoped(move || _barrier(communicator, None)));
    }
}

fn _create_subgraph<'a, 'b, G, D>(graph: &'a RefCell<&'b mut G>,
                                  source1: &mut Stream<'a, 'b, G, D>,
                                  source2: &mut Stream<'a, 'b, G, D>) -> (Stream<'a, 'b, G, D>,
                                                                          Stream<'a, 'b, G, D>)
where 'b: 'a,
      G: Graph+'b,
      G::Communicator: Clone,
      D: Data+Hash+Eq+Debug+Columnar {
    // build up a subgraph using the concatenated inputs/feedbacks
    let comm = graph.borrow_mut().communicator().clone();
    let mut subgraph = (graph.borrow_mut().new_subgraph::<u64>(), comm);

    let (sub_egress1, sub_egress2) = {
        let subgraph_builder = subgraph.builder();//RefCell::new(&mut subgraph);

        (
            source1.enter(&subgraph_builder).distinct().leave(graph),
            source2.enter(&subgraph_builder).leave(graph)
        )
    };

    graph.borrow_mut().add_scope(subgraph.0);

    return (sub_egress1, sub_egress2);
}

fn _distinct<C: Communicator>(communicator: C, bencher: Option<&mut Bencher>) {
    // no "base scopes" yet, so the root pretends to be a subscope of some parent with a () timestamp type.
    let mut graph = new_graph(communicator);

    let (mut input1, mut input2) = {
        let builder = graph.builder();

        // try building some input scopes
        let (input1, mut stream1) = builder.new_input::<u64>();
        let (input2, mut stream2) = builder.new_input::<u64>();

        // prepare some feedback edges
        let (mut feedback1, mut feedback1_output) = builder.feedback(Product::new((), 1000000), Local(1));
        let (mut feedback2, mut feedback2_output) = builder.feedback(Product::new((), 1000000), Local(1));

        // build up a subgraph using the concatenated inputs/feedbacks
        let (mut egress1, mut egress2) = _create_subgraph(&builder,
                                                          &mut stream1.concat(&mut feedback1_output),
                                                          &mut stream2.concat(&mut feedback2_output));

        // connect feedback sources. notice that we have swapped indices ...
        feedback1.connect_input(&mut egress2);
        feedback2.connect_input(&mut egress1);

        (input1, input2)
    };

    // finalize the graph/subgraph
    graph.0.get_internal_summary();
    graph.0.set_external_summary(Vec::new(), &mut []);

    // do one round of push progress, pull progress ...
    graph.0.push_external_progress(&mut []);
    graph.0.pull_internal_progress(&mut [], &mut [], &mut []);

    // move some data into the dataflow graph.
    input1.send_messages(&Product::new((), 0), vec![1u64]);
    input2.send_messages(&Product::new((), 0), vec![2u64]);

    // see what everyone thinks about that ...
    graph.0.pull_internal_progress(&mut [], &mut [], &mut []);

    input1.advance(&Product::new((), 0), &Product::new((), 1000000));
    input2.advance(&Product::new((), 0), &Product::new((), 1000000));
    input1.close_at(&Product::new((), 1000000));
    input2.close_at(&Product::new((), 1000000));

    // spin
    match bencher {
        Some(b) => b.iter(|| { graph.0.pull_internal_progress(&mut [], &mut [], &mut []); }),
        None    => while graph.0.pull_internal_progress(&mut [], &mut [], &mut []) { }
    }
}

// fn _command<C: Communicator>(communicator: C, bencher: Option<&mut Bencher>) {
//     let communicator = Rc::new(RefCell::new(communicator));
//
//     // no "base scopes" yet, so the root pretends to be a subscope of some parent with a () timestamp type.
//     let mut graph = new_graph(Progcaster::new(&mut (*communicator.borrow_mut())));
//     let mut input = graph.new_input::<u64>(communicator);
//     let mut feedback = input.1.feedback(((), 1000), Local(1));
//     let mut result: Stream<_, u64, _> = input.1.concat(&mut feedback.1)
//                                                .command("./target/release/command".to_string());
//
//     feedback.0.connect_input(&mut result);
//
//     // start things up!
//     graph.borrow_mut().get_internal_summary();
//     graph.borrow_mut().set_external_summary(Vec::new(), &mut []);
//     graph.borrow_mut().push_external_progress(&mut []);
//
//     input.0.close_at(&((), 0));
//
//     // spin
//     match bencher {
//         Some(b) => b.iter(|| { graph.borrow_mut().pull_internal_progress(&mut [], &mut [], &mut []); }),
//         None    => while graph.borrow_mut().pull_internal_progress(&mut [], &mut [], &mut []) { },
//     }
// }

fn _barrier<C: Communicator>(communicator: C, bencher: Option<&mut Bencher>) {
    let mut graph = new_graph(communicator);

    let peers = graph.communicator().peers();

    {
        let graph = &graph.builder();

        graph.borrow_mut().add_scope(BarrierScope { epoch: 0, ready: true, degree: peers, ttl: 1000000 });
        graph.borrow_mut().connect(ScopeOutput(0, 0), ScopeInput(0, 0));
    }
    // start things up!
    graph.0.get_internal_summary();
    graph.0.set_external_summary(Vec::new(), &mut []);
    graph.0.push_external_progress(&mut []);

    // spin
    match bencher {
        Some(b) => b.iter(|| { graph.0.pull_internal_progress(&mut [], &mut [], &mut []); }),
        None    => while graph.0.pull_internal_progress(&mut [], &mut [], &mut []) { },
    }
}
