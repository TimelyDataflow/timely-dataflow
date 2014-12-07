extern crate core;
extern crate test;

use test::Bencher;

use std::default::Default;

use progress::{Graph, Scope};
use progress::subgraph::Subgraph;
use progress::subgraph::Summary::Local;
use progress::subgraph::Source::ScopeOutput;
use progress::subgraph::Target::ScopeInput;

use example::input::{InputExtensionTrait};
use example::concat::{ConcatExtensionTrait};
use example::feedback::{FeedbackScope, FeedbackExtensionTrait};
use example::queue::{QueueExtensionTrait};

use example::graph_builder::GraphBuilder;
use example::barrier::BarrierScope;

use std::rc::{Rc, try_unwrap};
use std::cell::RefCell;

mod progress;
mod example;

fn main()
{
    println!("Hello, world!")
    test_barrier_no_bench();
    test_queue_no_bench();
}

#[bench]
fn test_queue_bench(b: &mut Bencher) { test_queue(Some(b)); }
fn test_queue_no_bench() { println!("performing 1,000,000 iterations of queue micro-benchmark"); test_queue(None); }

fn test_queue(b: Option<&mut Bencher>)
{
    // no "base scopes" yet, so the root pretends to be a subscope of some parent with a () timestamp type.
    let mut graph: Rc<RefCell<Subgraph<(), (), uint, uint>>> = Rc::new(RefCell::new(Default::default()));

    // try building some input scopes (scopes 0 and 1)
    let (input1, mut target1, mut stream1) = graph.new_input();
    let (input2, mut target2, mut stream2) = graph.new_input();

    // prepare some feedback edges (scopes 2 and 3)
    let (feedback1, mut feedback1_output) = stream1.feedback(Local(1));
    let (feedback2, mut feedback2_output) = stream2.feedback(Local(1));

    // prepare the concatenation of those edges and the inputs (scopes 4 and 5)
    let mut concat1 = stream1.concat(&mut feedback1_output);
    let mut concat2 = stream2.concat(&mut feedback2_output);

    // build up a subgraph using the concatenated inputs/feedbacks (scope 6)
    let mut subgraph = graph.borrow_mut().new_subgraph(0, 0);

    let (mut sub_egress1, mut sub_egress2) =
    {
        // create new ingress nodes, passing in a reference to the subgraph for them to use.
        let mut sub_ingress1 = subgraph.add_input(&mut concat1);
        let mut sub_ingress2 = subgraph.add_input(&mut concat2);

        // putting a queueing scope into the subgraph!
        let mut queue = sub_ingress1.queue();

        // egress each of the streams from the subgraph.
        let sub_egress1 = subgraph.add_output_to_graph(&mut queue, box graph.clone());
        let sub_egress2 = subgraph.add_output_to_graph(&mut sub_ingress2, box graph.clone());

        (sub_egress1, sub_egress2)
    };

    // sort of a mess, but the way to get the subgraph out of the Rc<RefCell<...>>
    // will explode if anyone else is still sitting on a reference to subgraph.
    graph.add_scope(box try_unwrap(subgraph).ok().expect("hm").into_inner());

    // connect feedback sources. notice that we have swapped indices ...
    FeedbackScope::connect_input(&mut graph, feedback1, &mut sub_egress2);
    FeedbackScope::connect_input(&mut graph, feedback2, &mut sub_egress1);

    // finalize the graph/subgraph
    graph.borrow_mut().get_internal_summary();
    graph.borrow_mut().set_external_summary(Vec::new(), &Vec::new());

    // do one round of push progress, pull progress ...
    graph.borrow_mut().push_external_progress(&Vec::new());
    graph.borrow_mut().pull_internal_progress(&mut Vec::new(), &mut Vec::new(), &mut Vec::new());

    // move some data into the dataflow graph.
    target1.deliver_data(&((), 0), &vec![1u]);
    target2.deliver_data(&((), 0), &vec![2u]);

    // see what everyone thinks about that ...
    graph.borrow_mut().pull_internal_progress(&mut Vec::new(), &mut Vec::new(), &mut Vec::new());

    // indicate that ecah input sent a message, and won't send again until epoch 1000000.
    // obviously pretty clunky. will be important to make a more friendly interface later on.
    input1.borrow_mut().sent_messages(((), 0), 1, ((), 1000000));
    input2.borrow_mut().sent_messages(((), 0), 1, ((), 1000000));

    // see what everyone thinks about that ...
    graph.borrow_mut().pull_internal_progress(&mut Vec::new(), &mut Vec::new(), &mut Vec::new());

    // spin
    match b
    {
        Some(x) => x.iter(||                { graph.borrow_mut().pull_internal_progress(&mut Vec::new(), &mut Vec::new(), &mut Vec::new()); }),
        None => for _ in range(0u, 1000000) { graph.borrow_mut().pull_internal_progress(&mut Vec::new(), &mut Vec::new(), &mut Vec::new()); },
    }
}


#[bench]
fn test_barrier_bench(b: &mut Bencher) { test_barrier(Some(b)); }
fn test_barrier_no_bench() { println!("performing 1,000,000 iterations of barrier micro-benchmark"); test_barrier(None); }

fn test_barrier(b: Option<&mut Bencher>)
{
    // no "base scopes" yet, so the root pretends to be a subscope of some parent with a () timestamp type.
    let mut graph: Rc<RefCell<Subgraph<(), (), uint, uint>>> = Rc::new(RefCell::new(Default::default()));

    // add a new, relatively pointless, scope.
    // It has a non-trivial path summary, so the timely dataflow graph is not improperly cyclic.
    graph.add_scope(box BarrierScope { epoch: 0u, ready: true });

    // attach the scope's output back to its input.
    graph.connect(ScopeOutput(0, 0), ScopeInput(0, 0));

    // start things up!
    graph.borrow_mut().get_internal_summary();
    graph.borrow_mut().set_external_summary(Vec::new(), &Vec::new());
    graph.borrow_mut().push_external_progress(&Vec::new());

    // spin
    match b
    {
        Some(x) => x.iter(||                { graph.borrow_mut().pull_internal_progress(&mut Vec::new(), &mut Vec::new(), &mut Vec::new()); }),
        None => for _ in range(0u, 1000000) { graph.borrow_mut().pull_internal_progress(&mut Vec::new(), &mut Vec::new(), &mut Vec::new()); },
    }
}
