extern crate timely;

fn main() {
    let nodes: usize = std::env::args().nth(1).unwrap().parse().unwrap();
    let rounds: usize = std::env::args().nth(2).unwrap().parse().unwrap();

    test_alt(nodes, rounds);
    test_neu(nodes, rounds);
}


fn test_alt(nodes: usize, rounds: usize) {

    use timely::progress::frontier::Antichain;
    use timely::progress::nested::subgraph::{Source, Target};
    use timely::progress::nested::reachability::{Builder, Tracker};

    // This test means to exercise the efficiency of the tracker by performing local changes and expecting
    // that it will respond efficiently.

    // allocate a new empty topology builder.
    let mut builder = Builder::<usize>::new();

    // Each node with one input connected to one output.
    for index in 1 .. nodes {
        builder.add_node(index - 1, 1, 1, vec![vec![Antichain::from_elem(0)]]);
    }
    builder.add_node(nodes - 1, 1, 1, vec![vec![Antichain::from_elem(1)]]);

    // Connect nodes in sequence, looping around to the first from the last.
    for index in 1 .. nodes {
        builder.add_edge(Source { index: index - 1, port: 0 }, Target { index, port: 0 } );
    }
    builder.add_edge(Source { index: nodes - 1, port: 0 }, Target { index: 0, port: 0 } );

    // Construct a reachability tracker.
    let mut tracker = Tracker::allocate_from(builder.summarize());

    let timer = ::std::time::Instant::now();

    tracker.update_target(Target { index: 0, port: 0 }, 0, 1);
    tracker.propagate_all();
    for index in 0 .. nodes { tracker.pushed_mut(index).iter_mut().for_each(|x| { x.drain(); }); }

    for round in 0 .. rounds {

        for index in 1 .. nodes {
            tracker.update_target(Target { index: index - 1, port: 0 }, round, -1);
            tracker.update_target(Target { index, port: 0 }, round, 1);
            tracker.propagate_all();
            for index in 0 .. nodes { tracker.pushed_mut(index).iter_mut().for_each(|x| { x.drain(); }); }
        }

        tracker.update_target(Target { index: nodes - 1, port: 0 }, round, -1);
        tracker.update_target(Target { index: 0, port: 0 }, round + 1, 1);
        tracker.propagate_all();
        for index in 0 .. nodes { tracker.pushed_mut(index).iter_mut().for_each(|x| { x.drain(); }); }
    }

    let elapsed = timer.elapsed();
    println!("Reachability (alt) elapsed for {} nodes; avg: {:?}, total: {:?}", nodes, elapsed / ((nodes * rounds) as u32), elapsed);

    let timer = ::std::time::Instant::now();
    for round in rounds .. 2 * rounds {

        tracker.update_target(Target { index: 0, port: 0 }, round, -1);
        tracker.update_target(Target { index: 0, port: 0 }, round + 1, 1);
        tracker.propagate_all();
        for index in 0 .. nodes { tracker.pushed_mut(index).iter_mut().for_each(|x| { x.drain(); }); }
    }

    let elapsed = timer.elapsed();
    println!("Reachability (alt) elapsed for {} nodes; avg: {:?}, total: {:?}", nodes, elapsed / (rounds as u32), elapsed);
}

fn test_neu(nodes: usize, rounds: usize) {

    use timely::progress::frontier::Antichain;
    use timely::progress::nested::subgraph::{Source, Target};
    use timely::progress::nested::reachability_neu::Builder;

    // This test means to exercise the efficiency of the tracker by performing local changes and expecting
    // that it will respond efficiently.

    // allocate a new empty topology builder.
    let mut builder = Builder::<usize>::new();

    // Each node with one input connected to one output.
    for index in 1 .. nodes {
        builder.add_node(index - 1, 1, 1, vec![vec![Antichain::from_elem(0)]]);
    }
    builder.add_node(nodes - 1, 1, 1, vec![vec![Antichain::from_elem(1)]]);

    // Connect nodes in sequence, looping around to the first from the last.
    for index in 1 .. nodes {
        builder.add_edge(Source { index: index - 1, port: 0 }, Target { index, port: 0 } );
    }
    builder.add_edge(Source { index: nodes - 1, port: 0 }, Target { index: 0, port: 0 } );

    // Construct a reachability tracker.
    let mut tracker = builder.build();

    let timer = ::std::time::Instant::now();

    tracker.update_target(Target { index: 0, port: 0 }, 0, 1);
    tracker.propagate_all();
    tracker.pushed().drain();

    for round in 0 .. rounds {

        for index in 1 .. nodes {
            tracker.update_target(Target { index: index - 1, port: 0 }, round, -1);
            tracker.update_target(Target { index, port: 0 }, round, 1);
            tracker.propagate_all();
            tracker.pushed().drain();
        }

        tracker.update_target(Target { index: nodes - 1, port: 0 }, round, -1);
        tracker.update_target(Target { index: 0, port: 0 }, round + 1, 1);
        tracker.propagate_all();
        tracker.pushed().drain();
    }

    let elapsed = timer.elapsed();
    println!("Reachability (neu) elapsed for {} nodes; avg: {:?}, total: {:?}", nodes, elapsed / ((nodes * rounds) as u32), elapsed);

    let timer = ::std::time::Instant::now();
    for round in rounds .. 2 * rounds {

        tracker.update_target(Target { index: 0, port: 0 }, round, -1);
        tracker.update_target(Target { index: 0, port: 0 }, round + 1, 1);
        tracker.propagate_all();
        tracker.pushed().drain();
    }

    let elapsed = timer.elapsed();
    println!("Reachability (neu) elapsed for {} nodes; avg: {:?}, total: {:?}", nodes, elapsed / (rounds as u32), elapsed);
}
