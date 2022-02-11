//! Benchmarks for [Tee] and [Buffer].
//!
//! The benchmarks determine throughput and latency for a number of configurations.

use std::fmt::{Display, Formatter};

use criterion::black_box;
use criterion::*;

use timely::communication::Push;
use timely::dataflow::channels::pushers::buffer::Buffer;
use timely::dataflow::channels::pushers::Tee;
use timely::dataflow::channels::{Bundle, Message};

use experiments::{construct_data, DropPusher, ReturnPusher};

#[derive(Clone)]
struct ExperimentConfig {
    pushers: usize,
    batch: u64,
}

impl Display for ExperimentConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "pushers={},batch={}", self.pushers, self.batch)
    }
}

/// Benchmark the [Tee] pusher for both [DropPusher] and [ReturnPusher].
///
/// This does not spawn a Timely instance but only benchmarks the struct on a single thread.
/// The benchmark varies
/// * the number of pushers downstream of the [Tee],
/// * the amount of data pushed at the tee.
fn tee_micro(c: &mut Criterion) {
    let mut group = c.benchmark_group("tee_micro");
    for pushers in [1, 2, 512] {
        for shift in [0, 4, 8, 14, 16] {
            let params = ExperimentConfig {
                pushers,
                batch: 1u64 << shift,
            };
            group.throughput(Throughput::Bytes(
                std::mem::size_of_val(&params.batch) as u64 * params.batch,
            ));
            group.bench_with_input(
                BenchmarkId::new("DropPusher", params.clone()),
                &params,
                move |b, params| experiment_tee_micro::<DropPusher>(b, params),
            );
            group.bench_with_input(
                BenchmarkId::new("ReturnPusher", params.clone()),
                &params,
                move |b, params| experiment_tee_micro::<ReturnPusher>(b, params),
            );
        }
    }
}

fn experiment_tee_micro<P: 'static + Default + Push<Bundle<u32, u64>>>(
    b: &mut Bencher,
    params: &ExperimentConfig,
) {
    let (mut tee, helper) = Tee::new();
    for pusher in (0..params.pushers).map(|_| P::default()) {
        helper.add_pusher(pusher);
    }

    let buffer = construct_data(params.batch);
    let mut copy = Vec::new();

    b.iter(move || {
        for batch in black_box(&buffer) {
            copy.clone_from(&batch);
            Message::push_at(&mut copy, 0, &mut tee);
        }
    })
}

/// Benchmark the [Buffer] pusher for both [DropPusher] and [ReturnPusher].
///
/// The benchmark varies the amount of data pushed at the buffer.
fn buffer_micro(c: &mut Criterion) {
    let mut group = c.benchmark_group("buffer_micro");
    for shift in [0, 4, 8, 14, 16] {
        let params = ExperimentConfig {
            pushers: 1, // Buffer only accepts a single pusher
            batch: 1u64 << shift,
        };
        group.throughput(Throughput::Bytes(
            std::mem::size_of_val(&params.batch) as u64 * params.batch,
        ));
        group.bench_with_input(
            BenchmarkId::new("DropPusher", params.clone()),
            &params,
            move |b, params| experiment_buffer_micro::<DropPusher>(b, params),
        );
        group.bench_with_input(
            BenchmarkId::new("ReturnPusher", params.clone()),
            &params,
            move |b, params| experiment_buffer_micro::<ReturnPusher>(b, params),
        );
    }
}

fn experiment_buffer_micro<P: 'static + Default + Push<Bundle<u32, u64>>>(
    b: &mut Bencher,
    params: &ExperimentConfig,
) {
    let mut send_buffer = Buffer::new(P::default());

    let buffer = construct_data(params.batch);

    b.iter(move || {
        for (index, batch) in black_box(&buffer).iter().enumerate() {
            let mut session = send_buffer.session(&(index as u32));
            for i in batch {
                session.give(*i);
            }
        }
    })
}

criterion_group!(benches, tee_micro, buffer_micro);
criterion_main!(benches);
