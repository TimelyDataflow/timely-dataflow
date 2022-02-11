# Benchmarks for Timely

The `experiments` crate serves as a home to benchmarks that are used to evaluate changes within Timely.
It is currently not complete or stabilized, only containing benchmarks to validate changes to core structs, such as
buffers, tees, and data exchanges.
Primarily, it serves to raise confidence that a change does not introduce regressions, and in some cases that an
expected performance gain can actually be observed.

Over time, this might become more robust and could evaluate more parts of the system, but it'll only happen as we work
on the implementation and need to get a deeper insight into potential effects of changes.

## Coverage

The current benchmarks cover the following aspects within Timely:
* The buffer benchmark evaluates the performance of Timely's Buffer and Tee in a micro-benchmark setting.
* The exchange benchmark evaluates the performance of Timely's exchange infrastructure, both in an actual dataflow
  and within a micro-benchmark.

We care about both throughput and latency metrics.
Timely's goal is to provide elasticity for maintaining low latency while benefitting from pipelining effects as the
amount of data increases.
Hence, the benchmarks vary the amount of data, and if applicable, the parallelism of the system.

## Running the benchmarks

The benchmarks are based on [criterion](https://github.com/bheisler/criterion.rs), a benchmark library to detect
performance changes.
When developing a feature, one first needs to capture a performance baseline. After implementing the feature, another
benchmark run can compare the performance with the baseline.

1. Obtaine a performance baseline, preferrably on current `master`:
   ```shell
   cargo +nightly bench
   ```
2. Develop a feature and compare the performance against the baseline:
   ```shell
   cargo +nightly bench
   ```
   Criterion prints summarized statistics while running the benchmark, which serves as a good first glimps on how
   performance might change.
3. Open the benchmark results in a browser, which are located in `target/criterion/`.
   To view change reports, select a specific benchmark and configuration, and open its report.
   For example, to check how the `buffer_micro` benchmark with configuration `DropPusher` and `pushers=1,batch=1`
   changed, navigate to `target/criterion/buffer_micro/DropPusher/pushers=1,batch=1/report` and open the `index.html`.

When adding a new benchmark as part of a change, first create the benchmark as a separate commit, take the baseline, and
continue developing with additional commits. This way, a fair baseline can be established.
