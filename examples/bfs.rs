extern crate time;
extern crate rand;
extern crate timely;
extern crate radix_sort;

use rand::{Rng, SeedableRng, StdRng};
use radix_sort::RadixSorter;

use timely::dataflow::*;
use timely::dataflow::operators::*;
use timely::dataflow::channels::pact::Exchange;

fn main() {

    // command-line args: numbers of nodes and edges in the random graph.
    let nodes: usize = std::env::args().nth(1).unwrap().parse().unwrap();
    let edges: usize = std::env::args().nth(2).unwrap().parse().unwrap();

    timely::execute_from_args(std::env::args().skip(3), move |root| {

        let index = root.index();
        let peers = root.peers();

        let seed: &[_] = &[1, 2, 3, root.index()];
        let mut rng: StdRng = SeedableRng::from_seed(seed);
        let mut sorter = RadixSorter::new();

        let mut offsets = Vec::new();
        let mut targets = Vec::new();
        let mut recents = Vec::new();
        let mut parents = vec![u32::max_value(); nodes / peers];

        let start = time::precise_time_s();

        root.scoped(move |scope| {

            // generate part of a random graph.
            let graph = (0..edges / peers)
                .map(move |_| (rng.gen_range(0u32, nodes as u32), rng.gen_range(0u32, nodes as u32)))
                .to_stream(scope);

            // define a loop variable, for the (node, root) pairs.
            let (handle, stream) = scope.loop_variable(usize::max_value(), 1);

            graph.binary_notify(
                &stream,
                Exchange::new(|x: &(u32, u32)| x.0 as u64),
                Exchange::new(|x: &(u32, u32)| x.0 as u64),
                "BFS",
                vec![],
                move |input1, input2, output, notify| {

                    // receive edges, start to sort them
                    input1.for_each(|time, data| {
                        notify.notify_at(time);
                        for &edge in data.iter() { sorter.push(edge, &|x| x.0); }
                    });

                    // receive (node, root) pairs, note any new ones.
                    input2.for_each(|time, data| {
                        notify.notify_at(time);
                        for &(node, root) in data.iter() {
                            // println!("received: {:?}, parents[{}] = {}", (node, root), node, parents[node as usize]);
                            if parents[(node as usize) / peers] == u32::max_value() {
                                // println!("keeping: {:?}", node);
                                parents[(node as usize) / peers] = root;
                                recents.push(node);
                            }
                        }
                    });

                    notify.for_each(|time, _count| {

                        // maybe process the graph
                        if time.inner == 0 {

                            println!("{}:\tstarting sort", time::precise_time_s() - start);

                            // construct the graph
                            offsets.push(0);
                            let mut prev_node = 0;
                            let sorted = sorter.finish(&|x| x.0);
                            for buffer in &sorted {
                                for &(node, edge) in buffer {
                                    while prev_node < node {
                                        prev_node += 1;
                                        offsets.push(targets.len())
                                    }
                                    targets.push(edge);
                                }
                            }
                            offsets.push(targets.len())

                        }

                        println!("{}:\tworker: {}, todo.len(): {}", time::precise_time_s() - start, index, recents.len());

                        let mut session = output.session(&time);
                        while let Some(next) = recents.pop() {
                            let lower = offsets[(next as usize) / peers];
                            let upper = offsets[((next as usize) / peers) + 1];
                            // println!("degree[{}]: {}", next, upper-lower);
                            for &target in &targets[lower..upper] {
                                session.give((target, next));
                            }
                        }

                    });
                }
            )
            .concat(&(0..1).map(|x| (x,x)).to_stream(scope))
            .connect_loop(handle);

        });

    });

}
