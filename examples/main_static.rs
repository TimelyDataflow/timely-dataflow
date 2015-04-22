#![feature(test)]
#![allow(dead_code)]

extern crate test;
extern crate columnar;
extern crate byteorder;
extern crate timely;

extern crate docopt;
use docopt::Docopt;

use test::Bencher;
use std::thread;
use std::hash::Hash;
use std::fmt::Debug;

use columnar::Columnar;

use timely::progress::Scope;
use timely::progress::nested::Summary::Local;
use timely::progress::timestamp::RootTimestamp;
use timely::communication::{ThreadCommunicator, ProcessCommunicator, Communicator};
use timely::communication::channels::Data;
use timely::networking::initialize_networking;

use timely::example_static::*;

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

fn create_subgraph<G: GraphBuilder, D>(builder: &mut G,
                                        source1: &Stream<G::Timestamp, D>,
                                        source2: &Stream<G::Timestamp, D>) ->
                                            (Stream<G::Timestamp, D>, Stream<G::Timestamp, D>)
where D: Data+Hash+Eq+Debug+Columnar {

    let mut subgraph = builder.new_subgraph::<u64>();

    (subgraph.enter(source1).distinct().leave(),
     subgraph.enter(source2).leave())
}

fn _distinct<C: Communicator>(communicator: C, bencher: Option<&mut Bencher>) {

    let mut root = GraphRoot::new(communicator);

    let (mut input1, mut input2) = {

        // allocate a new graph builder
        let mut graph = root.new_subgraph();

        // try building some input scopes
        let (input1, stream1) = graph.new_input::<u64>();
        let (input2, stream2) = graph.new_input::<u64>();

        // prepare some feedback edges
        let (loop1_source, loop1) = graph.loop_variable(RootTimestamp::new(100), Local(1));
        let (loop2_source, loop2) = graph.loop_variable(RootTimestamp::new(100), Local(1));

        let concat1 = (&mut graph).concatenate(vec![stream1, loop1]).disable();
        let concat2 = (&mut graph).concatenate(vec![stream2, loop2]).disable();

        // build up a subgraph using the concatenated inputs/feedbacks
        let (egress1, egress2) = create_subgraph(&mut graph, &concat1, &concat2);

        // connect feedback sources. notice that we have swapped indices ...
        egress1.enable(&mut graph).connect_loop(loop2_source);
        egress2.enable(&mut graph).connect_loop(loop1_source);

        (input1, input2)
    };

    root.step();

    // move some data into the dataflow graph.
    input1.send_messages(&RootTimestamp::new(0), vec![1u64]);
    input2.send_messages(&RootTimestamp::new(0), vec![2u64]);

    // see what everyone thinks about that ...
    root.step();

    input1.advance(&RootTimestamp::new(0), &RootTimestamp::new(1000000));
    input2.advance(&RootTimestamp::new(0), &RootTimestamp::new(1000000));
    input1.close_at(&RootTimestamp::new(1000000));
    input2.close_at(&RootTimestamp::new(1000000));

    // spin
    match bencher {
        Some(b) => b.iter(|| { root.step(); }),
        None    => while root.step() { }
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

    let mut root = GraphRoot::new(communicator);
    //
    // {
    //     let borrow = root.builder();
    //     let mut graph = SubgraphBuilder::new(&borrow);
    //
    //     let peers = graph.with_communicator(|x| x.peers());
    //     {
    //         let builder = &graph.builder();
    //
    //         builder.borrow_mut().add_scope(BarrierScope { epoch: 0, ready: true, degree: peers, ttl: 1000000 });
    //         builder.borrow_mut().connect(ScopeOutput(0, 0), ScopeInput(0, 0));
    //     }
    //
    //     graph.seal();
    // }
    //
    // // spin
    match bencher {
        Some(b) => b.iter(|| { root.step(); }),
        None    => while root.step() { },
    }
}
