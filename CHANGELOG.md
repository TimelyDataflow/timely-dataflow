# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.21.5](https://github.com/TimelyDataflow/timely-dataflow/compare/timely-v0.21.4...timely-v0.21.5) - 2025-08-05

### Other

- Update to columnar 0.10 and make flush public
- Upgrade to columnar 0.9
# Changelog

All notable changes to this project will be documented in this file.

## [0.21.4](https://github.com/TimelyDataflow/timely-dataflow/compare/timely-v0.21.3...timely-v0.21.4) - 2025-07-06

### Other

- Update for Columnar 0.7
- Disable notifications for some operators

## [0.21.3](https://github.com/TimelyDataflow/timely-dataflow/compare/timely-v0.21.2...timely-v0.21.3) - 2025-06-24

### Other

- Improve doc
- Always assert
- Guard against data loss
- Rename to relax
- Also flush session buffer
- Flush container builders in exchange

## [0.21.2](https://github.com/TimelyDataflow/timely-dataflow/compare/timely-v0.21.1...timely-v0.21.2) - 2025-06-24

### Other

- Merge pull request #671 from antiguru/log_channel_typ
- Log channel container type

## [0.21.1](https://github.com/TimelyDataflow/timely-dataflow/compare/timely-v0.21.0...timely-v0.21.1) - 2025-06-17

### Other

- Fix nightly compile warnings ([#670](https://github.com/TimelyDataflow/timely-dataflow/pull/670))
- Avoid compile warnings on nightly ([#669](https://github.com/TimelyDataflow/timely-dataflow/pull/669))
- Derive Copy

## [0.21.0](https://github.com/TimelyDataflow/timely-dataflow/compare/timely-v0.20.0...timely-v0.21.0) - 2025-05-09

### Other

- Remove crossbeam uses
- Tidy various logging uses
- Make std::time::Instant optional
- Merge pull request #662 from frankmcsherry/further_cleanup
- Switch to finish_non_exhaustive()
- Merge branch 'master' into docs

## [0.20.0](https://github.com/TimelyDataflow/timely-dataflow/compare/timely-v0.19.0...timely-v0.20.0) - 2025-03-28

### Other

- Update timely/src/progress/operate.rs
- Add scaling test
- Switch builder API to port-identified iterators
- Adjust reachability logic to be more linear
- Swap PortConnectivity implementation from Vec to BTreeMap
- Support optional path summaries for disconnected ports
- Make PortConnectivity API more explicit
- Make PortConnectivity a struct, with sufficient methods
- Introduce Connectivity and PortConnectivity type aliases
- Add clippy lints ([#659](https://github.com/TimelyDataflow/timely-dataflow/pull/659))
- Add miri test ([#655](https://github.com/TimelyDataflow/timely-dataflow/pull/655))

## [0.19.0](https://github.com/TimelyDataflow/timely-dataflow/compare/timely-v0.18.1...timely-v0.19.0) - 2025-02-28

### Other

- Use `dep` syntax with `getopts` ([#591](https://github.com/TimelyDataflow/timely-dataflow/pull/591))
- Make Buffer::push_into private ([#649](https://github.com/TimelyDataflow/timely-dataflow/pull/649))
- Remove columnation and flatcontainer deps ([#647](https://github.com/TimelyDataflow/timely-dataflow/pull/647))

## [0.18.1](https://github.com/TimelyDataflow/timely-dataflow/compare/timely-v0.18.0...timely-v0.18.1) - 2025-02-12

### Other

- Update columnar to 0.3, make workspace dependency ([#639](https://github.com/TimelyDataflow/timely-dataflow/pull/639))

## [0.18.0](https://github.com/TimelyDataflow/timely-dataflow/compare/timely-v0.17.1...timely-v0.18.0) - 2025-02-12

### Other

- Update columnar to 0.3, and columnar example ([#635](https://github.com/TimelyDataflow/timely-dataflow/pull/635))
- Convert Write::write to Write::write_all ([#636](https://github.com/TimelyDataflow/timely-dataflow/pull/636))
- Introduce foundation for broadcast channel ([#633](https://github.com/TimelyDataflow/timely-dataflow/pull/633))

## [0.17.1](https://github.com/TimelyDataflow/timely-dataflow/compare/timely-v0.17.0...timely-v0.17.1) - 2025-01-24

### Other

- Derive ord/eq traits for Product's columnar variant ([#630](https://github.com/TimelyDataflow/timely-dataflow/pull/630))

## [0.17.0](https://github.com/TimelyDataflow/timely-dataflow/compare/timely-v0.16.1...timely-v0.17.0) - 2025-01-23

### Other

- Move opinions about reachability logging into TrackerLogger ([#629](https://github.com/TimelyDataflow/timely-dataflow/pull/629))
- Align `Bytesable` messages to `u64` ([#614](https://github.com/TimelyDataflow/timely-dataflow/pull/614))
- Flatten reachability logging, log identifier ([#628](https://github.com/TimelyDataflow/timely-dataflow/pull/628))
- Log operator summaries using clever approach ([#626](https://github.com/TimelyDataflow/timely-dataflow/pull/626))
- Allow event iterators to surface owned data ([#627](https://github.com/TimelyDataflow/timely-dataflow/pull/627))
- Typed logging ([#624](https://github.com/TimelyDataflow/timely-dataflow/pull/624))

## [0.16.1](https://github.com/TimelyDataflow/timely-dataflow/compare/timely-v0.16.0...timely-v0.16.1) - 2025-01-16

### Other

- Avoid allocation in progcaster ([#622](https://github.com/TimelyDataflow/timely-dataflow/pull/622))
- Log action can distinguish data from flush ([#619](https://github.com/TimelyDataflow/timely-dataflow/pull/619))
- give_container for arbitrary container builders ([#621](https://github.com/TimelyDataflow/timely-dataflow/pull/621))
- Add an is_empty check before retrieving elapsed time. ([#620](https://github.com/TimelyDataflow/timely-dataflow/pull/620))
- Correct documentation for execute_from_args ([#617](https://github.com/TimelyDataflow/timely-dataflow/pull/617))

## [0.16.0](https://github.com/TimelyDataflow/timely-dataflow/compare/timely-v0.15.1...timely-v0.16.0) - 2025-01-09

### Other

- Define loggers in terms of container builders ([#615](https://github.com/TimelyDataflow/timely-dataflow/pull/615))
- Remove SizableContainer requirement from partition ([#612](https://github.com/TimelyDataflow/timely-dataflow/pull/612))

## [0.15.1](https://github.com/TimelyDataflow/timely-dataflow/compare/timely-v0.15.0...timely-v0.15.1) - 2024-12-18

### Other

- Remove worker identifier from logging ([#533](https://github.com/TimelyDataflow/timely-dataflow/pull/533))
- add `.partition()` for `StreamCore` (#610)
- Update columnar ([#611](https://github.com/TimelyDataflow/timely-dataflow/pull/611))
- Introduce columnar and derive extensively ([#608](https://github.com/TimelyDataflow/timely-dataflow/pull/608))

## [0.15.0](https://github.com/TimelyDataflow/timely-dataflow/compare/timely-v0.14.1...timely-v0.15.0) - 2024-12-05

### Other

- Prefer byteorder in place of bincode ([#607](https://github.com/TimelyDataflow/timely-dataflow/pull/607))
- Use help from columnar 0.1.1 ([#606](https://github.com/TimelyDataflow/timely-dataflow/pull/606))
- Reorganize `Container` traits ([#605](https://github.com/TimelyDataflow/timely-dataflow/pull/605))
- Robustify potential Bytes alignment
- Correct bincode call to use and update reader
- Demonstrate `columnar` stuff ([#586](https://github.com/TimelyDataflow/timely-dataflow/pull/586))
- Allow containers to specify their own serialization ([#604](https://github.com/TimelyDataflow/timely-dataflow/pull/604))
- Remove Container: Clone + 'static ([#540](https://github.com/TimelyDataflow/timely-dataflow/pull/540))
- Apply various Clippy recommendations ([#603](https://github.com/TimelyDataflow/timely-dataflow/pull/603))
- Several improvements around `Bytesable` and `Message`. ([#601](https://github.com/TimelyDataflow/timely-dataflow/pull/601))

## [0.14.1](https://github.com/TimelyDataflow/timely-dataflow/compare/timely-v0.14.0...timely-v0.14.1) - 2024-11-12

### Added

The type `timely::Message` is now publicly re-exported.

### Other

- Public Message type ([#599](https://github.com/TimelyDataflow/timely-dataflow/pull/599))

## [0.14.0](https://github.com/TimelyDataflow/timely-dataflow/compare/timely-v0.13.0...timely-v0.14.0) - 2024-11-11

### Added

The trait `communication::Bytesable`, for types that must be serialized into or from a `Bytes`, and stands in for "timely appropriate serialization".
The trait `communication::Exchangeable`, a composite trait bringing together the requirements on a type for it to be sent along a general purpose communication channel.

### Removed

The communication `Message` and `RefOrMut` types have been removed.
The `RefOrMut` type wrapped either a `&T` or a `&mut T`, but with the removal of `abomonation` it is always a `&mut T`.
The `Message` type was used to indicate the serialization / deserialization behavior, and these opinions (e.g. "use `bincode`") have been migrated to the core `timely` crate.

### Other

- Move opinions about encoding from `communication` to `timely`. ([#597](https://github.com/TimelyDataflow/timely-dataflow/pull/597))
- Rust updates, better doc testing ([#598](https://github.com/TimelyDataflow/timely-dataflow/pull/598))
- Simplify communication `Message` type ([#596](https://github.com/TimelyDataflow/timely-dataflow/pull/596))

## 0.13.0 - 2024-10-29

Changelog bankruptcy declared.

## 0.12.0

The `Timestamp` trait has a new method `minimim()` that replaces Timely's use of `Default::default()` for default capabilities. The most pressing reason for this is the use of signed integers for timestamps, where Timely would effectively prevent the use of negative numbers by providing the default value of zero for capabilities. This should not have reduced any functionality, but might provide surprising output for programs that use integer timestamps and do not first advance timestamps (the tidy `0` will be replaced with `_::min_value()`).

### Added

Timely configuration can now be done with the `worker::Config` type, which supports user-defined configuration options.
The `get` and `set` methods allow binding arbitrary strings to arbitrary types `T: Send + Sync + 'static`.
For example, differential dataflow uses this mechanism to configure its background compaction rates.

### Removed

Removed all deprecated methods and traits.

Timely no longer responds to the `DEFAULT_PROGRESS_MODE` environment variable. Instead, it uses the newly added `worker::Config`.

Removed the `sort` crate, whose sorting methods are interesting but not currently a core part of timely dataflow.

### Changed

The default progress mode changed from "eager" to "demand driven", which causes progress updates to be accumulated for longer before transmission. The eager default had the potential to produce catastrophically large volumes of progress update messages, for the benefit of a reduced critical path latency. The demand-driven default removes the potential for catastrophic failure at the expense of an increase minimal latency. This should give a better default experience as one scales up the to large numbers of workers.

## 0.11.0

Reduce the amount of log flushing, and increase the batching of log messages.

## 0.10.0

### Added

A `Worker` now has a `step_or_park(Option<Duration>)` method, which instructs the worker to take a step and gives it permission to park the worker thread for at most the supplied timeout if there is no work to perform. A value of `None` implies no timeout (unboundedly parked) whereas a value of `Some(0)` should return immediately. The communication layers are implemented to awaken workers if they receive new communications, and workers should hand out copies of their `Thread` if they want other threads to wake them for other reasons (e.g. queues from threads external to timely).

Communication `WorkerGuards` expose their underlying join handles to allow the main thread or others to unpark worker threads that may be parked (for example, after pushing new data into a queue shared with the worker).

## 0.9.0

### Added

The `OperatorInfo` struct now contains the full address of the operator as a `Vec<usize>`.

### Changed

The `source` operator requires a closure that accepts an `OperatorInfo` struct in addition to its initial capability. This brings it to parity with the other closure-based operators, and is required to provide address information to the operator.

The address associated with each operator, a `[usize]` used to start with the identifier of the worker hosting the operator, followed by the dataflow identifier and the path down the dataflow to the operator. The worker identifier has been removed.

The `Worker` and the `Subgraph` operator no longer schedules all of their child dataflows and scopes by default. Instead, they track "active" children and schedule only those. Operators become active by receiving a message, a progress update, or by explicit activation. Some operators, source as `source`, have no inputs and will require explicit activation to run more than once. Operators that yield before completing all of their work (good for you!) should explicitly re-activate themselves to ensure they are re-scheduled even if they receive no further messages or progress updates. Documentation examples for the `source` method demonstrate this.

The `dataflow_using` method has been generalized to support arbitrary dataflow names, loggers, and additional resources the dataflow should keep alive. Its name has been changed to `dataflow_core`.

You can now construct `feedback` operators with a `Default::default()` path summary, which has the ability to not increment timestamps. Instead of panicking, Timely's reachability module will inform you if a non-incrementing cycle is detected, at which point you should probably double check your code. It is not 100% known what the system will do in this case (e.g., the progress tracker may enter a non-terminating loop; this is on you, not us ;)).

## 0.8.0

This release made several breaking modifications to the types associated with scopes, and in particular the generic parameters for the `Child<'a, G: ScopeParent, T: Timestamp>` type. Where previously the `T` parameter would be the *new coordinate* to add to `G`'s timestamp, it is now the *new timestamp* including `G`'s timestamp as well. This was done to support a broader class of timestamps to be used, beyond always requiring product combinations with new timestamps.

Beneficial fallouts include our ability to remove `RootTimestamp`, as dataflows can now be timestamped by `usize` or other primitive timestamps. Yay!

### Added

- The communication crate now has a `bincode` feature flag which should swing serialization over to use serde's `Serialize` trait. While it seems to work the ergonomics are likely in flux, as the choice is crate-wide and doesn't allow you to pick and choose a la carte.

- Timestamps may now implement a new `Refines` trait which allows one to describe one timestamp as a refinement of another. This is mainly used to describe which timestamps may be used for subscopes of an outer scope. The trait describes how to move between the timestamps (informally: "adding a zero" and "removing the inner coordinate") and how to summarize path summaries for the refining timestamp as those of the refined timestamp.

### Changed

- Many logging events have been rationalized. Operators and Channels should all have a worker-unique identifier that can be used to connect their metadata with events involving them. Previously this was a bit of a shambles.

- The `Scope` method `scoped` now allows new scopes with non-`Product` timestamps. Instead, the new timestamp must implement `Refines<_>` of the parent timestamp. This is the case for `Product` timestamps, but each timestamp also refines itself (allowing logical regions w/o changing the timestamp), and other timestamp combinators (e.g. Lexicographic) can be used.

- Root dataflow timestamps no longer need to be `Product<RootTimestamp,_>`. Instead, the `_` can be used as the timestamp.

- The `loop_variable` operator now takes a timestamp summary for the timestamp of its scope, not just the timestamp extending its parent scope. The old behavior can be recovered with `Product::new(Default::default(), summary)`, but the change allows cycles in more general scopes and seemed worth it. The operator also no longer takes a `limit`, and if you need to impose a limit other than the summary returning `None` you should use the `branch_when` operator.

### Removed

- The `RootTimestamp` and `RootSummary` types have been excised. Where you previously used `Product<RootTimestamp,T>` you can now use `Product<(),T>`, or even better just `T`. The requirement of a worker's `dataflow()` method is that the timestamp type implement `Refines<()>`, which .. ideally would be true for all timestamps but we can't have a blanket implementation until specialization lands (I believe).

- Several race conditions were "removed" from the communication library. These mostly involved rapid construction of dataflows (data received before a channel was constructed would be dropped) and clean shutdown (a timely computation could drop and fail to ack clean shutdown messages).

## 0.7.0

### Added
- You can now construct your own vector of allocator builders and supply them directly to `timely::execute::execute_from`. Previously one was restricted to whatever a `Configuration` could provide for you. This should allow more pleasant construction of custom allocators, or custom construction of existing allocators.
- Each timely worker now has a log registry, `worker.log_registry()`, from which you can register and acquire typed loggers for named log streams. This supports user-level logging, as well as user-configurable timely logging. Timely logging is under the name `"timely"`.

### Changed
- The `Root` type has been renamed `Worker` and is found in the `::worker` module. The methods of the `ScopeParent` trait are now in the `::worker::AsWorker` trait.
- The communication `Allocate` trait's main method `allocate` now takes a worker-unique identifier to use for the channel. The allocator may or may not use the information (most often for logging), but they are allowed to be incorrect if one allocates two channels with the same identifier.
- A `CapabilityRef<T>` now supports `retain_for(usize)` which indicates a specific output port the capability should be retain for use with. The `retain()` method still exists for now and is equivalent to `retain(0)`. This change also comes with the *inability* to use an arbitrary `Capability<T>` with any output; using a capability bound to the wrong output will result in a run-time error.
- The `unary` and `binary` operators now provide `data` as a `RefOrMut`, which does not implement `DerefMut`. More information on how to port methods can be found [here](https://github.com/TimelyDataflow/timely-dataflow/pull/135#issuecomment-418355284).


### Removed
- The deprecated `Unary` and `Binary` operator extension traits have been removed in favor of the `Operator` trait that supports both of them, as well as their `_notify` variants.
