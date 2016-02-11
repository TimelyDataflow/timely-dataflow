extern crate time;
extern crate rand;
extern crate timely;
extern crate timely_sort;

use std::collections::HashMap;

use rand::{Rng, SeedableRng, StdRng};
use timely_sort::LSBRadixSorter as RadixSorter;

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

        // pending edges and node updates.
        let mut edge_list = Vec::new();
        let mut node_lists = HashMap::new();

        // graph data; offsets into targets.
        let mut offsets = Vec::new();
        let mut targets = Vec::new();

        // holds the bfs parent of each node, or u32::max_value() if unset.
        let mut done = vec![u32::max_value(); 1 + (nodes / peers)];

        let start = time::precise_time_s();

        root.scoped(move |scope| {

            // generate part of a random graph.
            let graph = (0..edges / peers)
                .map(move |_| (rng.gen_range(0u32, nodes as u32), rng.gen_range(0u32, nodes as u32)))
                .to_stream(scope);

            // define a loop variable, for the (node, root) pairs.
            let (handle, stream) = scope.loop_variable(usize::max_value(), 1);

            // use the stream of edges
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
                        edge_list.push(data.replace_with(Vec::new()));
                    });

                    // receive (node, root) pairs, note any new ones.
                    input2.for_each(|time, data| {
                        node_lists.entry(time.time())
                                  .or_insert_with(|| {
                                      notify.notify_at(time.clone());
                                      Vec::new()
                                  })
                                  .push(data.replace_with(Vec::new()));
                    });

                    notify.for_each(|time, _num| {

                        // maybe process the graph
                        if time.inner == 0 {

                            // print some diagnostic timing information
                            if index == 0 { println!("{}:\tsorting", time::precise_time_s() - start); }

                            // sort the edges
                            sorter.sort(&mut edge_list, &|x| x.0);

                            let mut count = 0;
                            for buffer in &edge_list { count += buffer.len(); }

                            // allocate sufficient memory, to avoid resizing.
                            offsets = Vec::with_capacity(1 + (nodes / peers));
                            targets = Vec::with_capacity(count);

                            // construct the graph
                            offsets.push(0);
                            let mut prev_node = 0;
                            for buffer in edge_list.drain(..) {
                                for (node, edge) in buffer {
                                    let temp = node / peers as u32;
                                    while prev_node < temp {
                                        prev_node += 1;
                                        offsets.push(targets.len() as u32)
                                    }
                                    targets.push(edge);
                                }
                            }
                            offsets.push(targets.len() as u32);
                        }

                        // print some diagnostic timing information
                        if index == 0 { println!("{}:\ttime: {:?}", time::precise_time_s() - start, time.time()); }

                        if let Some(mut todo) = node_lists.remove(&time) {
                            let mut session = output.session(&time);

                            // we could sort these, or not. try it out!
                            // sorter.sort(&mut todo, &|x| x.0);

                            for buffer in todo.drain(..) {
                                for (node, prev) in buffer {
                                    let temp = (node as usize) / peers;
                                    if done[temp] == u32::max_value() {
                                        done[temp] = prev;
                                        let lower = offsets[temp] as usize;
                                        let upper = offsets[temp + 1] as usize;
                                        for &target in &targets[lower..upper] {
                                            session.give((target, node));
                                        }
                                    }
                                }
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
