#![feature(core)]

/* Based on src/main.rs from timely-dataflow by Frank McSherry,
*
* The MIT License (MIT)
*
* Copyright (c) 2014 Frank McSherry
*
* Permission is hereby granted, free of charge, to any person obtaining a copy
* of this software and associated documentation files (the "Software"), to deal
* in the Software without restriction, including without limitation the rights
* to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
* copies of the Software, and to permit persons to whom the Software is
* furnished to do so, subject to the following conditions:
*
* The above copyright notice and this permission notice shall be included in all
* copies or substantial portions of the Software.
*
* THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
* IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
* FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
* AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
* LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
* OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
* SOFTWARE.
*/

extern crate timely;
extern crate core;
extern crate columnar;

use columnar::Columnar;

use timely::communication::{Communicator, ThreadCommunicator};
use timely::communication::channels::Data;
use timely::progress::subgraph::new_graph;
use timely::progress::subgraph::Summary::Local;
use timely::progress::scope::Scope;
use timely::progress::graph::Graph;
use timely::example::input::InputExtensionTrait;
use timely::example::concat::ConcatExtensionTrait;
use timely::example::feedback::FeedbackExtensionTrait;
use timely::example::distinct::DistinctExtensionTrait;
use timely::example::stream::Stream;
use timely::example::graph_builder::{EnterSubgraphExt, LeaveSubgraphExt};

use core::fmt::Debug;

use std::cell::RefCell;
use std::hash::Hash;

fn main() {
    _distinct(ThreadCommunicator);
}


fn _distinct<C: Communicator>(communicator: C) {
    // no "base scopes" yet, so the root pretends to be a subscope of some parent with a () timestamp type.
    let mut graph = new_graph(communicator);

    // try building some input scopes
    let (mut input1, mut stream1) = graph.new_input::<u64>();
    let (mut input2, mut stream2) = graph.new_input::<u64>();

    // prepare some feedback edges
    let (mut feedback1, mut feedback1_output) = stream1.feedback(((), 1000000), Local(1));
    let (mut feedback2, mut feedback2_output) = stream2.feedback(((), 1000000), Local(1));

    // build up a subgraph using the concatenated inputs/feedbacks
    let (mut egress1, mut egress2) = _create_subgraph(&mut graph.clone(),
                                                      &mut stream1.concat(&mut feedback1_output),
                                                      &mut stream2.concat(&mut feedback2_output));

    // connect feedback sources. notice that we have swapped indices ...
    feedback1.connect_input(&mut egress2);
    feedback2.connect_input(&mut egress1);

    // finalize the graph/subgraph
    graph.0.borrow_mut().get_internal_summary();
    graph.0.borrow_mut().set_external_summary(Vec::new(), &mut Vec::new());

    // do one round of push progress, pull progress ...
    graph.0.borrow_mut().push_external_progress(&mut Vec::new());
    graph.0.borrow_mut().pull_internal_progress(&mut Vec::new(), &mut Vec::new(), &mut Vec::new());

    // move some data into the dataflow graph.
    input1.send_messages(&((), 0), vec![1u64]);
    input2.send_messages(&((), 0), vec![2u64]);

    // see what everyone thinks about that ...
    graph.0.borrow_mut().pull_internal_progress(&mut Vec::new(), &mut Vec::new(), &mut Vec::new());

    input1.advance(&((), 0), &((), 1000000));
    input2.advance(&((), 0), &((), 1000000));
    input1.close_at(&((), 1000000));
    input2.close_at(&((), 1000000));

    // spin
    while graph.0.borrow_mut().pull_internal_progress(&mut Vec::new(), &mut Vec::new(), &mut Vec::new()) { }
}

fn _create_subgraph<G: Graph, D: Data+Hash+Eq+Debug+Columnar>(graph: &mut G, source1: &mut Stream<G, D>, source2: &mut Stream<G, D>) -> (Stream<G, D>, Stream<G, D>) {
    // build up a subgraph using the concatenated inputs/feedbacks
    let mut subgraph = graph.new_subgraph::<u64>();

    let (sub_egress1, sub_egress2) = {
        let shared_subgraph = RefCell::new(&mut subgraph);

        (
            source1.enter(&shared_subgraph, graph.communicator()).distinct().leave(graph),
            source2.enter(&shared_subgraph, graph.communicator()).leave(graph)
        )
    };

    graph.add_scope(subgraph);

    return (sub_egress1, sub_egress2);
}
