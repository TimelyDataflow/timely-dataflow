//! Check visibility of updates dominated by an earlier frontier element.
//!
//! For example: `cargo run --release -p timely --example progress_visibility -- 16000 16000`.
//! A many-input operator's output can retain one implication per input at
//! distinct times, although its frontier has only one element. Lazy progress
//! reporting asks whether pending subtractions at this port are globally visible.
use std::hint::black_box;
use std::time::Instant;
use timely::progress::reachability::PortInformation;

fn main() {
    let mut args = std::env::args().skip(1);
    let retained: usize = args.next().expect("retained timestamps").parse().unwrap();
    let queries: usize = args.next().expect("queries").parse().unwrap();
    assert!(retained > 1);
    let mut port = PortInformation::<usize>::new();
    port.implications.update_iter((0..retained).map(|time| (time, 1)));

    let start = Instant::now();
    for i in 0..queries {
        assert!(!black_box(&port).is_global(&black_box(1 + i % (retained - 1))));
    }
    let elapsed = start.elapsed();
    assert!(port.is_global(&0));
    port.implications.update_iter([(0, 1)]);
    assert!(!port.is_global(&0));
    println!("{retained},{queries},{}", elapsed.as_nanos());
}
