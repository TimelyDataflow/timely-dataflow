extern crate timely;

use std::fmt::{Display, Formatter};
use std::time::{Duration, Instant};

use criterion::black_box;
use criterion::*;
use itertools::Itertools;

use timely::communication::Push;
use timely::dataflow::channels::pushers::Exchange;
use timely::dataflow::channels::{Bundle, Message};
use timely::dataflow::operators::{Exchange as ExchangeOperator, Input, Probe};
use timely::dataflow::InputHandle;
use timely::{CommunicationConfig, Config, WorkerConfig};

use experiments::{DropPusher, ReturnPusher};

#[derive(Clone)]
struct ExperimentConfig {
    threads: usize,
    batch: u64,
}

impl Display for ExperimentConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "threads={},batch={}", self.threads, self.batch)
    }
}

fn exchange_e2e(c: &mut Criterion) {
    let mut group = c.benchmark_group("exchange_e2e");
    for threads in [1, 2, 4, 8, 16] {
        for shift in [0, 4, 8, 14] {
            let params = ExperimentConfig {
                threads,
                batch: 1u64 << shift,
            };
            group.throughput(Throughput::Bytes(
                std::mem::size_of_val(&params.batch) as u64 * params.batch,
            ));
            group.bench_with_input(
                BenchmarkId::new("Default", params.clone()),
                &params,
                move |b, params| {
                    b.iter_custom(|iters| {
                        let config = Config::process(params.threads);
                        black_box(experiment_exchange_e2e(config, params.batch, iters))
                    })
                },
            );
            group.bench_with_input(
                BenchmarkId::new("DefaultZero", params.clone()),
                &params,
                move |b, params| {
                    b.iter_custom(|iters| {
                        let config = Config {
                            communication: CommunicationConfig::ProcessBinary(params.threads),
                            worker: WorkerConfig::default(),
                        };
                        black_box(experiment_exchange_e2e(config, params.batch, iters))
                    })
                },
            );
        }
    }
}

fn experiment_exchange_e2e(config: Config, batch: u64, rounds: u64) -> Duration {
    timely::execute(config, move |worker| {
        let mut input = InputHandle::new();
        let probe = worker.dataflow(|scope| scope.input_from(&mut input).exchange(|x| *x).probe());

        let mut time = 0;

        let buffer = (0..batch).collect();
        let mut copy = Vec::new();

        let timer = Instant::now();
        for _round in 0..rounds {
            copy.clone_from(&buffer);
            input.send_batch(&mut copy);
            copy.clear();
            time += 1;
            input.advance_to(time);
            while probe.less_than(input.time()) {
                worker.step();
            }
        }
        timer.elapsed()
    })
    .unwrap()
    .join()
    .into_iter()
    .next()
    .unwrap()
    .unwrap()
}

fn exchange_micro(c: &mut Criterion) {
    let mut group = c.benchmark_group("exchange_micro");
    for threads in [1, 2, 512] {
        for shift in [0, 4, 8, 14, 16] {
            let params = ExperimentConfig {
                threads,
                batch: 1u64 << shift,
            };
            group.throughput(Throughput::Bytes(
                std::mem::size_of_val(&params.batch) as u64 * params.batch,
            ));
            group.bench_with_input(
                BenchmarkId::new("DropPusher", params.clone()),
                &params,
                move |b, params| experiment_exchange_micro::<DropPusher>(b, params),
            );
            group.bench_with_input(
                BenchmarkId::new("ReturnPusher", params.clone()),
                &params,
                move |b, params| experiment_exchange_micro::<ReturnPusher>(b, params),
            );
        }
    }
}

fn experiment_exchange_micro<P: Default + Push<Bundle<u32, u64>>>(
    b: &mut Bencher,
    params: &ExperimentConfig,
) {
    let pushers = (0..params.threads).map(|_| P::default()).collect();
    let mut exchange = Exchange::new(pushers, |_, x| *x);
    let buffer: Vec<Vec<_>> = (0..params.batch)
        .map(|x| x)
        .chunks(Message::<usize, u64>::default_length())
        .into_iter()
        .map(|chunk| chunk.collect())
        .collect();
    let mut copy = Vec::new();

    b.iter(move || {
        for batch in black_box(&buffer) {
            copy.clone_from(&batch);
            Message::push_at_no_allocation(&mut copy, 0, &mut exchange);
        }
    })
}

criterion_group!(benches, exchange_e2e, exchange_micro);
criterion_main!(benches);
