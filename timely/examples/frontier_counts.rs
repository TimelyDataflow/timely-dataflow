//! Add counts at an existing frontier while retaining many later timestamps.
//!
//! For example: `cargo run --release -p timely --example frontier_counts -- 16000 16000`.
//! The first argument is the retained timestamp count; the second is the number
//! of separately applied additions. Setup and final validation are not timed.
use std::hint::black_box;
use std::time::Instant;
use timely::progress::frontier::MutableAntichain;

fn main() {
    let mut args = std::env::args().skip(1);
    let retained: usize = args.next().expect("retained timestamps").parse().unwrap();
    let additions: usize = args.next().expect("additions").parse().unwrap();
    assert!(retained > 1);
    let additions_i64 = i64::try_from(additions).unwrap();
    assert!(additions_i64 < i64::MAX);

    let mut frontier = MutableAntichain::<usize>::new();
    frontier.update_iter((0..retained).map(|time| (time, 1)));
    let start = Instant::now();
    for _ in 0..additions {
        assert_eq!(black_box(&mut frontier).update_iter([(0, 1)]).count(), 0);
    }
    let elapsed = start.elapsed();
    assert_eq!(frontier.count_for(&0), additions_i64 + 1);
    assert_eq!(frontier.update_iter([(0, -additions_i64 - 1)]).collect::<Vec<_>>(), vec![(0, -1), (1, 1)]);
    println!("{retained},{additions},{}", elapsed.as_nanos());
}
