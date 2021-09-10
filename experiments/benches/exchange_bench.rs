extern crate timely;

use std::fmt::{Display, Formatter};
use std::time::{Duration, Instant};

use criterion::black_box;
use criterion::*;

use timely::dataflow::channels::pact::{
    Exchange as ExchangePact, NonRetainingExchange, ParallelizationContract,
};
use timely::dataflow::operators::{Exchange, Input, Probe};
use timely::dataflow::InputHandle;
use timely::{CommunicationConfig, Config, WorkerConfig};

#[derive(Clone)]
struct ExperimentConfig {
    threads: usize,
    batch: u64,
}

impl Display for ExperimentConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "threads={:2},batch={:5}", self.threads, self.batch)
    }
}

fn bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("exchange");
    for threads in [1, 2, 4, 8, 16] {
        for shift in [0, 4, 8, 14] {
            let params = ExperimentConfig {
                threads,
                batch: 1u64 << shift,
            };
            group.bench_with_input(
                BenchmarkId::new("Default", params.clone()),
                &params,
                move |b, params| {
                    b.iter_custom(|iters| {
                        let config = Config::process(params.threads);
                        black_box(experiment_exchange(
                            config,
                            params.batch,
                            iters,
                            || ExchangePact::new(|&x| x as u64),
                        ))
                    })
                },
            );
            group.bench_with_input(
                BenchmarkId::new("NonRetaining", params.clone()),
                &params,
                move |b, params| {
                    b.iter_custom(|iters| {
                        let config = Config::process(params.threads);
                        black_box(experiment_exchange(
                            config,
                            params.batch,
                            iters,
                            || NonRetainingExchange::new(|&x| x as u64),
                        ))
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
                        black_box(experiment_exchange(
                            config,
                            params.batch,
                            iters,
                            ||ExchangePact::new(|&x| x as u64),
                        ))
                    })
                },
            );
            group.bench_with_input(
                BenchmarkId::new("NonRetainingZero", params.clone()),
                &params,
                move |b, params| {
                    b.iter_custom(|iters| {
                        let config = Config {
                            communication: CommunicationConfig::ProcessBinary(params.threads),
                            worker: WorkerConfig::default(),
                        };
                        black_box(experiment_exchange(
                            config,
                            params.batch,
                            iters,
                            ||NonRetainingExchange::new(|&x| x as u64),
                        ))
                    })
                },
            );
        }
    }
}

fn experiment_exchange<F: Fn() -> P + Sync + Send + 'static, P: ParallelizationContract<i32, u64>>(
    config: Config,
    batch: u64,
    rounds: u64,
    pact: F,
) -> Duration {
    timely::execute(config, move |worker| {
        let mut input = InputHandle::new();
        let probe = worker.dataflow(|scope| scope.input_from(&mut input).apply_pact(pact()).probe());

        let mut time = 0;
        let timer = Instant::now();

        for _round in 0..rounds {
            for i in 0..batch {
                input.send(i);
            }
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

criterion_group!(benches, bench);
criterion_main!(benches);
