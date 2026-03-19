//! Streams timely events over WebSocket for live visualization.
//!
//! Usage:
//!     cargo run --example logging-live --features visualizer [-- timely args]
//!
//! Then open `visualizer/index.html` in a browser and connect to `ws://localhost:51371`.

use timely::dataflow::operators::{Input, Exchange, Enter, Leave, Inspect, Probe, Feedback, ConnectLoop, Concat};
use timely::dataflow::operators::vec::{Map, Filter};
use timely::dataflow::{InputHandle, Scope};
use timely::visualizer::Server;

fn main() {
    let port = std::env::var("TIMELY_VIS_PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(51371);

    let server = Server::start(port);

    timely::execute_from_args(std::env::args(), move |worker| {

        server.register(worker);

        // A richer dataflow to exercise the visualizer:
        //
        //   Input -> Map -> Exchange -> Filter ----> Region[ Inspect ] -> Probe
        //                                  \                 ^
        //                                   +-> Feedback ----+
        //
        // This gives us: multiple pipeline stages, an exchange, branching,
        // a nested scope (region), a feedback loop with a back-edge, and
        // real data flowing through channels.
        let mut input = InputHandle::new();
        let mut probe = timely::dataflow::ProbeHandle::new();
        worker.dataflow(|scope| {
            let (handle, cycle) = scope.feedback::<Vec<u64>>(1);

            let mapped = scope
                .input_from(&mut input)
                .container::<Vec<_>>()
                .map(|x: u64| x.wrapping_mul(17).wrapping_add(3))
                .exchange(|&x: &u64| x);

            let filtered = mapped.filter(|&x: &u64| x % 2 == 0);

            let looped = filtered.clone().concat(cycle);

            scope.region(|inner| {
                looped
                    .enter(inner)
                    .inspect(|_x| { })
                    .leave()
            })
            .probe_with(&mut probe);

            // Feed back values that haven't reached zero yet.
            filtered
                .map(|x: u64| x / 4)
                .filter(|&x: &u64| x > 0)
                .connect_loop(handle);
        });

        // Continuously feed data so events keep flowing.
        let mut round = 0u64;
        loop {
            for i in 0..100u64 {
                input.send(round * 100 + i);
            }
            round += 1;
            input.advance_to(round);
            while probe.less_than(input.time()) {
                worker.step();
            }
        }
    }).unwrap();
}
