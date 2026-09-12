use std::collections::{BTreeMap, HashMap};

use timely::dataflow::operators::{ToStream, Concat, Feedback, ConnectLoop, Capability};
use timely::dataflow::operators::generic::operator::Operator;
use timely::dataflow::channels::pact::Exchange;

fn main() {

    // command-line args: numbers of nodes and edges in the random graph.
    let nodes: usize = std::env::args().nth(1).unwrap().parse().unwrap();
    let edges: usize = std::env::args().nth(2).unwrap().parse().unwrap();

    // let logging = ::timely::logging::to_tcp_socket();
    timely::execute_from_args(std::env::args().skip(3), move |worker| {

        let index = worker.index();
        let peers = worker.peers();

        // pending edges and node updates.
        let mut edge_list = Vec::new();
        let mut node_lists = HashMap::new();

        // graph data; offsets into targets.
        let mut offsets = Vec::new();
        let mut targets = Vec::new();

        // holds the bfs parent of each node, or u32::MAX if unset.
        let mut done = vec![u32::MAX; 1 + (nodes / peers)];

        let start = std::time::Instant::now();

        worker.dataflow::<usize,_,_>(move |scope| {

            // generate part of a random graph.
            use std::hash::{BuildHasher, BuildHasherDefault, DefaultHasher};
            let hasher = BuildHasherDefault::<DefaultHasher>::new();
            let graph =
            (0..edges/peers)
                .map(move |i| (hasher.hash_one(&(i,index,0)) as usize % nodes,
                               hasher.hash_one(&(i,index,1)) as usize % nodes))
                .map(|(src,dst)| (src as u32, dst as u32))
                .to_stream(scope)
                .container::<Vec<_>>();

            // define a loop variable, for the (node, worker) pairs.
            let (handle, stream) = scope.feedback(1usize);

            // use the stream of edges
            graph.binary_frontier(
                stream,
                Exchange::new(|x: &(u32, u32)| u64::from(x.0)),
                Exchange::new(|x: &(u32, u32)| u64::from(x.0)),
                "BFS",
                move |_capability, _info| {

                    // capabilities for times with pending work, in time order.
                    let mut pending: BTreeMap<usize, Capability<usize>> = BTreeMap::new();

                    move |(input1, frontier1), (input2, frontier2), output| {

                    // receive edges, start to sort them
                    input1.for_each_stamp(|cap, data| {
                        if let Some(cap) = cap.retain_least(output.output_index()) {
                            pending.entry(*cap.time()).or_insert(cap);
                            edge_list.extend(data.map(std::mem::take));
                        }
                    });

                    // receive (node, worker) pairs, note any new ones.
                    input2.for_each_stamp(|cap, data| {
                        if let Some(cap) = cap.retain_least(output.output_index()) {
                            node_lists.entry(*cap.time()).or_insert_with(Vec::new).extend(data.map(std::mem::take));
                            pending.entry(*cap.time()).or_insert(cap);
                        }
                    });

                    // process each time once neither input can produce data at it, in time order.
                    while let Some(time) = pending.keys().next().copied() {
                        if frontier1.frontier().less_equal(&time) || frontier2.frontier().less_equal(&time) { break; }
                        let cap = pending.remove(&time).unwrap();

                        // maybe process the graph
                        if *cap.time() == 0 {

                            // print some diagnostic timing information
                            if index == 0 { println!("{:?}:\tsorting", start.elapsed()); }

                            // sort the edges (previously: radix sorted).
                            edge_list.sort();

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
                            while offsets.len() < offsets.capacity() {
                                offsets.push(targets.len() as u32);
                            }
                        }

                        // print some diagnostic timing information
                        if index == 0 { println!("{:?}:\ttime: {:?}", start.elapsed(), cap.time()); }

                        if let Some(mut todo) = node_lists.remove(cap.time()) {
                            let mut session = output.session(&cap);

                            // we could sort these, or not (previously: radix sorted).
                            // todo.sort();

                            for buffer in todo.drain(..) {
                                for (node, prev) in buffer {
                                    let temp = (node as usize) / peers;
                                    if done[temp] == u32::MAX {
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
                    }
                    }
                }
            )
            .concat((0..1).map(|x| (x,x)).to_stream(scope))
            .connect_loop(handle);
        });
    }).unwrap(); // asserts error-free execution;
}
