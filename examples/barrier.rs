#![feature(test)]

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
extern crate test;

use timely::communication::{ProcessCommunicator, Communicator};
use timely::progress::nested::builder::Builder as SubgraphBuilder;
use timely::progress::graph::{Graph, Root};
use timely::progress::nested::Source::ScopeOutput;
use timely::progress::nested::Target::ScopeInput;
use timely::example::barrier::BarrierScope;

use test::Bencher;

use std::thread;


fn main() {
    _barrier_multi(1);
}

#[bench]
fn barrier_bench(bencher: &mut Bencher) { _barrier(ProcessCommunicator::new_vector(1).swap_remove(0), Some(bencher)); }
fn _barrier_multi(threads: u64) {
    let mut guards = Vec::new();
    for communicator in ProcessCommunicator::new_vector(threads).into_iter() {
        guards.push(thread::scoped(move || _barrier(communicator, None)));
    }
}

fn _barrier<C: Communicator>(communicator: C, bencher: Option<&mut Bencher>) {

    let mut root = Root::new(communicator);

    {
        let borrow = root.builder();
        let mut graph = SubgraphBuilder::new(&borrow);

        let peers = graph.with_communicator(|x| x.peers());
        {
            let builder = &graph.builder();

            builder.borrow_mut().add_scope(BarrierScope { epoch: 0, ready: true, degree: peers, ttl: 1000000 });
            builder.borrow_mut().connect(ScopeOutput(0, 0), ScopeInput(0, 0));
        }

        graph.seal();
    }

    // spin
    match bencher {
        Some(b) => b.iter(|| { root.step(); }),
        None    => while root.step() { },
    }
}
