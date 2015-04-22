#![feature(scoped)]
#![feature(test)]

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

            builder.borrow_mut().add_scope(BarrierScope { epoch: 0, ready: true, degree: peers, ttl: 1000 });
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
