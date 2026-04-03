//! Demonstrates cross-thread capture and replay of timely logging events.
//!
//! A source timely instance (2 workers) runs a simple dataflow and captures its
//! logging events using thread-safe `link_sync::EventLink`s. A sink timely instance
//! (1 worker) replays those events and counts them.

use std::sync::Arc;
use std::time::Duration;

use timely::dataflow::operators::{Exchange, Inspect, ToStream};
use timely::dataflow::operators::capture::event::link_sync::EventLink;
use timely::dataflow::operators::capture::Replay;
use timely::logging::{BatchLogger, TimelyEventBuilder, TimelyEvent};

fn main() {

    let source_workers = 2usize;
    let sink_workers = 1usize;

    // One EventLink per source worker, shared between source (writer) and sink (reader).
    let event_links: Vec<_> = (0..source_workers)
        .map(|_| Arc::new(EventLink::<Duration, Vec<(Duration, TimelyEvent)>>::new()))
        .collect();

    // Clone reader handles (they start at the head; the writer will advance past them).
    let readers: Vec<_> = event_links.iter().map(Arc::clone).collect();

    std::thread::scope(|scope| {

        // --- Source instance: 2 workers producing logging events ---
        let source = scope.spawn(move || {
            timely::execute(timely::Config::process(source_workers), move |worker| {

                // Install logging: capture timely events into our shared EventLink.
                let link = event_links[worker.index()].clone();
                let mut logger = BatchLogger::new(link);
                worker.log_register()
                    .unwrap()
                    .insert::<TimelyEventBuilder, _>("timely", move |time, data| {
                        logger.publish_batch(time, data);
                    });

                // A trivial dataflow to generate some logging activity.
                worker.dataflow::<u64,_,_>(|scope| {
                    (0..100u64)
                        .to_stream(scope)
                        .container::<Vec<_>>()
                        .exchange(|&x| x)
                        .inspect(|_x| { });
                });

            }).expect("source execution failed");
        });

        // --- Sink instance: 1 worker replaying the captured logs ---
        let sink = scope.spawn(move || {
            timely::execute(timely::Config::process(sink_workers), move |worker| {

                // Each sink worker replays a disjoint subset of the source streams.
                let replayers: Vec<_> = readers.iter().enumerate()
                    .filter(|(i, _)| i % worker.peers() == worker.index())
                    .map(|(_, r)| Arc::clone(r))
                    .collect();

                worker.dataflow::<Duration,_,_>(|scope| {
                    replayers
                        .replay_into(scope)
                        .inspect(|event| {
                            println!("  {:?}", event);
                        });
                });

            }).expect("sink execution failed");
        });

        source.join().expect("source panicked");
        sink.join().expect("sink panicked");
    });

    println!("Done.");
}
