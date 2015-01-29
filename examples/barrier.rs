#![allow(unstable)]
#![feature(unsafe_destructor)]

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
extern crate test;

use timely::communication::{ProcessCommunicator, Communicator};
use timely::progress::subgraph::{Subgraph, new_graph};
use timely::progress::broadcast::Progcaster;
use timely::progress::scope::Scope;
use timely::progress::graph::{Graph, GraphExtension};
use timely::progress::subgraph::Source::ScopeOutput;
use timely::progress::subgraph::Target::ScopeInput;
use timely::example::barrier::BarrierScope;

use test::Bencher;

use std::rc::Rc;
use std::cell::RefCell;
use std::thread::Thread;


fn main() {
    _barrier_multi(1);
}

#[bench]
fn barrier_bench(bencher: &mut Bencher) { _barrier(ProcessCommunicator::new_vector(1).swap_remove(0), Some(bencher)); }
fn _barrier_multi(threads: u64) {
    let mut guards = Vec::new();
    for allocator in ProcessCommunicator::new_vector(threads).into_iter() {
        guards.push(Thread::scoped(move || _barrier(allocator, None)));
    }
}

fn _barrier(mut allocator: ProcessCommunicator, bencher: Option<&mut Bencher>) {
    let mut graph: Rc<RefCell<Subgraph<(), (), u64, u64>>> = new_graph(Progcaster::new(&mut allocator));
    graph.add_scope(BarrierScope { epoch: 0, ready: true, degree: allocator.peers(), ttl: 1000 });
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
