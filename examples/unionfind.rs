extern crate rand;
extern crate timely;
extern crate timely_sort;

use std::cmp::Ordering;

use rand::{Rng, SeedableRng, StdRng};

use timely::dataflow::*;
use timely::dataflow::operators::*;
use timely::dataflow::channels::pact::Pipeline;

fn main() {

    // command-line args: numbers of nodes and edges in the random graph.
    let nodes: usize = std::env::args().nth(1).unwrap().parse().unwrap();
    let edges: usize = std::env::args().nth(2).unwrap().parse().unwrap();
    let batch: usize = std::env::args().nth(3).unwrap().parse().unwrap();

    let start = ::std::time::Instant::now();

    timely::execute_from_args(std::env::args().skip(4), move |root| {

        let index = root.index();
        let peers = root.peers();

        let (mut input, probe) = root.scoped(move |scope| {

            let (handle, stream) = scope.new_input();

            // optionally, de-load worker zero who will have enough to do when `peers` is large.
            let probe = stream//.exchange(move |x: &(usize, usize)| (x.0 % (peers - 1)) as u64 + 1)
                              .union_find()
                              .exchange(|_| 0)
                              .union_find()
                              .probe().0;

            (handle, probe)
        });

        let seed: &[_] = &[1, 2, 3, index];
        let mut rng: StdRng = SeedableRng::from_seed(seed);

        for edge in 0 .. edges {
            if edge % peers == index { 
                input.send((rng.gen_range(0, nodes), rng.gen_range(0, nodes)));
            }

            // at batch boundaries, advance epoch and sync.
            if edge % batch == (batch - 1) {
                let next = input.epoch() + 1;
                input.advance_to(next);
                root.step_while(|| probe.lt(input.time()));
            }
        }

    }).unwrap(); // asserts error-free execution;

    let elapsed = start.elapsed();

    println!("elapsed: {:?}; avg latency: {:?}", elapsed, elapsed / ((edges / batch) as u32));
}

trait UnionFind {
    fn union_find(&self) -> Self;
}

impl<G: Scope> UnionFind for Stream<G, (usize, usize)> {
    fn union_find(&self) -> Stream<G, (usize, usize)> {

        let mut roots = vec![];  // u32 can work and is smaller than usize
        let mut ranks = vec![];  // u8 should be large enough (n < 2^256)

        self.unary_stream(Pipeline, "UnionFind", move |input, output| {

            // extract timestamped edges from our input.
            input.for_each(|time, data| {

                let mut session = output.session(&time);

                for &(mut x, mut y) in data.iter() {

                    // grow arrays if required.
                    let m = ::std::cmp::max(x, y);
                    for i in roots.len() .. (m + 1) {
                        roots.push(i);
                        ranks.push(0);
                    }

                    // look up roots for `x` and `y`.    
                    while x != roots[x] { x = roots[x]; }
                    while y != roots[y] { y = roots[y]; }

                    // if not the same, send edge and merge.
                    if x != y {
                        session.give((x, y));
                        match ranks[x].cmp(&ranks[y]) {
                            Ordering::Less    => { roots[x] = y },
                            Ordering::Greater => { roots[y] = x },
                            Ordering::Equal   => { roots[y] = x; ranks[x] += 1 },
                        }
                    }
                }
            });
        })
    }
}