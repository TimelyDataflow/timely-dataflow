use std::sync::{Arc, Mutex};
use std::time::Duration;

use timely::Config;
use timely::dataflow::InputHandle;
use timely::dataflow::operators::{Input, Exchange, Probe, Enter, Leave};
use timely::logging::{self, TimelyProgressEventBuilder, TimelyProgressEvent, TimelySummaryEventBuilder, OperatesSummaryEvent};
use timely::order::Product;

/// Collects log events into a shared vector during `timely::execute`.
fn collect_events<T: Send + 'static>(
) -> (Arc<Mutex<Vec<(Duration, T)>>>, impl Fn(&Duration, &mut Option<Vec<(Duration, T)>>) + Clone) {
    let events: Arc<Mutex<Vec<(Duration, T)>>> = Arc::new(Mutex::new(Vec::new()));
    let events2 = Arc::clone(&events);
    let callback = move |_time: &Duration, data: &mut Option<Vec<(Duration, T)>>| {
        if let Some(data) = data {
            events2.lock().unwrap().extend(data.drain(..));
        }
    };
    (events, callback)
}

// ------------------------------------------------------------------
// Flat dataflow: Input -> Exchange -> Probe
// ------------------------------------------------------------------

#[test]
fn progress_logging() {
    let (events, callback) = collect_events::<TimelyProgressEvent<usize>>();

    timely::execute(Config::thread(), move |worker| {
        worker.log_register().unwrap().insert::<TimelyProgressEventBuilder<usize>, _>(
            &logging::progress_log_name::<usize>(), callback.clone(),
        );

        let mut input = InputHandle::new();
        let probe = timely::dataflow::ProbeHandle::new();

        worker.dataflow::<usize, _, _>(|scope| {
            scope.input_from(&mut input)
                .container::<Vec<_>>()
                .exchange(|&x: &usize| x as u64)
                .probe_with(&probe);
        });

        input.send(0usize);
        input.advance_to(1);
        while probe.less_than(input.time()) {
            worker.step();
        }
    })
    .unwrap();

    let events = events.lock().unwrap();

    // Both send and receive progress events must be present.
    let sends: Vec<_> = events.iter().filter(|(_, e)| e.is_send).collect();
    let recvs: Vec<_> = events.iter().filter(|(_, e)| !e.is_send).collect();
    assert!(!sends.is_empty(), "Expected at least one progress send event");
    assert!(!recvs.is_empty(), "Expected at least one progress recv event");

    // Every send should be mirrored by a receive with the same seq_no.
    for (_, send) in &sends {
        let matched = recvs.iter().any(|(_, r)| r.seq_no == send.seq_no && r.channel == send.channel);
        assert!(matched, "Send event seq_no={} channel={} has no matching recv", send.seq_no, send.channel);
    }

    // The input advances from 0 to 1, so we expect internal capability changes
    // that mention timestamp 0 (release) and/or 1 (acquire).
    let has_internal = events.iter().any(|(_, e)| !e.internal.is_empty());
    assert!(has_internal, "Expected at least one event with internal capability changes");
}

#[test]
fn summary_logging() {
    let (events, callback) = collect_events::<OperatesSummaryEvent<usize>>();

    timely::execute(Config::thread(), move |worker| {
        worker.log_register().unwrap().insert::<TimelySummaryEventBuilder<usize>, _>(
            &logging::summary_log_name::<usize>(), callback.clone(),
        );

        let mut input = InputHandle::new();
        worker.dataflow::<usize, _, _>(|scope| {
            scope.input_from(&mut input)
                .container::<Vec<_>>()
                .exchange(|&x: &usize| x as u64)
                .probe_with(&timely::dataflow::ProbeHandle::new());
        });

        input.advance_to(1);
        worker.step();
    })
    .unwrap();

    let events = events.lock().unwrap();

    // The dataflow has Input (0 inputs), Exchange (1 input -> 1 output), and Probe
    // (1 input -> 1 output). We should see summary events for each.
    // Operators with inputs have non-empty summaries (one PortConnectivity per input).
    let nonempty: Vec<_> = events.iter().filter(|(_, e)| !e.summary.is_empty()).collect();
    assert!(nonempty.len() >= 2, "Expected at least 2 operators with non-empty summaries (Exchange, Probe), got {}", nonempty.len());

    // Each non-empty summary should have exactly 1 entry (one input port).
    for (_, e) in &nonempty {
        assert_eq!(e.summary.len(), 1, "Operator {} should have 1 input, got {}", e.id, e.summary.len());
    }

    // Every summary entry should map input -> output 0 with the identity summary.
    for (_, e) in &nonempty {
        for port_conn in &e.summary {
            let ports: Vec<_> = port_conn.iter_ports().collect();
            assert!(!ports.is_empty(), "Operator {} has a PortConnectivity with no output mapping", e.id);
            for (output, antichain) in &ports {
                assert_eq!(*output, 0, "Expected output port 0, got {}", output);
                // The default (identity) summary for usize is 0.
                assert!(antichain.elements().contains(&0usize),
                    "Expected identity summary (0) for operator {}, got {:?}", e.id, antichain);
            }
        }
    }

    // All operator ids should be distinct.
    let mut ids: Vec<_> = events.iter().map(|(_, e)| e.id).collect();
    ids.sort();
    ids.dedup();
    assert_eq!(ids.len(), events.len(), "Duplicate operator ids in summary events");
}

// ------------------------------------------------------------------
// Nested (iterative) dataflow: Input -> [Enter -> Exchange -> Leave] -> Probe
// ------------------------------------------------------------------

#[test]
fn progress_logging_iterative() {
    type Inner = Product<usize, usize>;

    let (outer_events, outer_cb) = collect_events::<TimelyProgressEvent<usize>>();
    let (inner_events, inner_cb) = collect_events::<TimelyProgressEvent<Inner>>();

    timely::execute(Config::thread(), move |worker| {
        worker.log_register().unwrap().insert::<TimelyProgressEventBuilder<usize>, _>(
            &logging::progress_log_name::<usize>(), outer_cb.clone(),
        );
        worker.log_register().unwrap().insert::<TimelyProgressEventBuilder<Inner>, _>(
            &logging::progress_log_name::<Inner>(), inner_cb.clone(),
        );

        let mut input = InputHandle::new();
        let probe = timely::dataflow::ProbeHandle::new();

        worker.dataflow::<usize, _, _>(|scope| {
            let stream = scope.input_from(&mut input).container::<Vec<_>>();
            scope.iterative::<usize, _, _>(|outer, inner| {
                stream.enter(inner)
                    .exchange(|&x: &usize| x as u64)
                    .leave(outer)
            })
            .probe_with(&probe);
        });

        input.send(0usize);
        input.advance_to(1);
        while probe.less_than(input.time()) {
            worker.step();
        }
    })
    .unwrap();

    let outer = outer_events.lock().unwrap();
    let inner = inner_events.lock().unwrap();
    assert!(!outer.is_empty(), "Expected outer scope progress events");
    assert!(!inner.is_empty(), "Expected inner scope progress events");

    // Inner progress events should carry Product timestamps.
    // The internal updates should reference Product<0,0> being released.
    let has_product_ts = inner.iter().any(|(_, e)|
        e.internal.iter().any(|(_, _, t, _)| *t == Product::new(0, 0))
    );
    assert!(has_product_ts, "Expected inner progress events with Product<0,0> timestamps");
}

#[test]
fn summary_logging_iterative() {
    type Inner = Product<usize, usize>;
    type InnerSummary = <Inner as timely::progress::Timestamp>::Summary;

    let (outer_events, outer_cb) = collect_events::<OperatesSummaryEvent<usize>>();
    let (inner_events, inner_cb) = collect_events::<OperatesSummaryEvent<InnerSummary>>();

    timely::execute(Config::thread(), move |worker| {
        worker.log_register().unwrap().insert::<TimelySummaryEventBuilder<usize>, _>(
            &logging::summary_log_name::<usize>(), outer_cb.clone(),
        );
        worker.log_register().unwrap().insert::<TimelySummaryEventBuilder<InnerSummary>, _>(
            &logging::summary_log_name::<Inner>(), inner_cb.clone(),
        );

        let mut input = InputHandle::new();
        worker.dataflow::<usize, _, _>(|scope| {
            let stream = scope.input_from(&mut input).container::<Vec<_>>();
            scope.iterative::<usize, _, _>(|outer, inner| {
                stream.enter(inner)
                    .exchange(|&x: &usize| x as u64)
                    .leave(outer)
            });
        });

        input.advance_to(1);
        worker.step();
    })
    .unwrap();

    let outer = outer_events.lock().unwrap();
    let inner = inner_events.lock().unwrap();
    assert!(!outer.is_empty(), "Expected outer scope summary events");
    assert!(!inner.is_empty(), "Expected inner scope summary events");

    // The outer scope should see the iterative subgraph as a single operator
    // with a non-empty summary (it has inputs and outputs).
    let outer_nonempty: Vec<_> = outer.iter().filter(|(_, e)| !e.summary.is_empty()).collect();
    assert!(!outer_nonempty.is_empty(), "Expected the subgraph operator to have a non-empty outer summary");

    // The inner scope should have summary events for the Exchange operator at minimum.
    let inner_nonempty: Vec<_> = inner.iter().filter(|(_, e)| !e.summary.is_empty()).collect();
    assert!(!inner_nonempty.is_empty(), "Expected inner operators with non-empty summaries");

    // All inner operator ids should be distinct.
    let mut ids: Vec<_> = inner.iter().map(|(_, e)| e.id).collect();
    ids.sort();
    ids.dedup();
    assert_eq!(ids.len(), inner.len(), "Duplicate operator ids in inner summary events");
}
