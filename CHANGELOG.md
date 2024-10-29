# Changelog

All notable changes to this project will be documented in this file.

## [0.13.0](https://github.com/TimelyDataflow/timely-dataflow/compare/timely-v0.12.0...timely-v0.13.0) - 2024-10-29

### Fixed

- fixes 390 ([#391](https://github.com/TimelyDataflow/timely-dataflow/pull/391))
- fix build warnings
- fixes [#176](https://github.com/TimelyDataflow/timely-dataflow/pull/176)
- fixes [#40](https://github.com/TimelyDataflow/timely-dataflow/pull/40)
- fix to parameters in example with cargo run

### Other

- Allow viewing of next dataflow-unique identifier ([#593](https://github.com/TimelyDataflow/timely-dataflow/pull/593))
- Improve docs. ([#590](https://github.com/TimelyDataflow/timely-dataflow/pull/590))
- Fix typos. ([#588](https://github.com/TimelyDataflow/timely-dataflow/pull/588))
- Switch to serde's derive feature, update Rust to 2021 ([#585](https://github.com/TimelyDataflow/timely-dataflow/pull/585))
- Remove `abomonation` to reduce unsoundness ([#575](https://github.com/TimelyDataflow/timely-dataflow/pull/575))
- Shared reference-counted operator path ([#582](https://github.com/TimelyDataflow/timely-dataflow/pull/582))
- Avoid temporary allocations by improved APIs ([#580](https://github.com/TimelyDataflow/timely-dataflow/pull/580))
- Introduce SmallVec for small allocations ([#581](https://github.com/TimelyDataflow/timely-dataflow/pull/581))
- s/if the contain/if they contain/ ([#576](https://github.com/TimelyDataflow/timely-dataflow/pull/576))
- Fix flatcontainer example ([#573](https://github.com/TimelyDataflow/timely-dataflow/pull/573))
- Bump flatcontainer to 0.5 ([#572](https://github.com/TimelyDataflow/timely-dataflow/pull/572))
- Clone for Config and other improvements ([#571](https://github.com/TimelyDataflow/timely-dataflow/pull/571))
- Rework container builder to use push into ([#569](https://github.com/TimelyDataflow/timely-dataflow/pull/569))
- Product with flatcontainer ([#570](https://github.com/TimelyDataflow/timely-dataflow/pull/570))
- PushInto targets container instead of item ([#566](https://github.com/TimelyDataflow/timely-dataflow/pull/566))
- PartialOrder with Rhs type parameter ([#565](https://github.com/TimelyDataflow/timely-dataflow/pull/565))
- Move OutputHandleCore::cease to more general impl ([#563](https://github.com/TimelyDataflow/timely-dataflow/pull/563))
- Container builder ([#562](https://github.com/TimelyDataflow/timely-dataflow/pull/562))
- Correct flatcontainer.rs glitch I introduced ([#558](https://github.com/TimelyDataflow/timely-dataflow/pull/558))
- Move `capture/` to `core::capture/` ([#557](https://github.com/TimelyDataflow/timely-dataflow/pull/557))
- Introduce FlatContainer, container function for hinting ([#556](https://github.com/TimelyDataflow/timely-dataflow/pull/556))
- Update more core operators ([#555](https://github.com/TimelyDataflow/timely-dataflow/pull/555))
- Remove EnterAt, migrate enterleave.rs ([#554](https://github.com/TimelyDataflow/timely-dataflow/pull/554))
- Operator movement (into `core`) ([#553](https://github.com/TimelyDataflow/timely-dataflow/pull/553))
- Change generic variable for containers from D to C ([#552](https://github.com/TimelyDataflow/timely-dataflow/pull/552))
- Rename internal `Core` variants ([#551](https://github.com/TimelyDataflow/timely-dataflow/pull/551))
- Container GATs, improve traits ([#541](https://github.com/TimelyDataflow/timely-dataflow/pull/541))
- Revert "async: relax requirements of supplied streams ([#401](https://github.com/TimelyDataflow/timely-dataflow/pull/401))" ([#546](https://github.com/TimelyDataflow/timely-dataflow/pull/546))
- Activate only by channel ID ([#526](https://github.com/TimelyDataflow/timely-dataflow/pull/526))
- Tidy warnings ([#542](https://github.com/TimelyDataflow/timely-dataflow/pull/542))
- Probe only retains weak handle to Rc ([#543](https://github.com/TimelyDataflow/timely-dataflow/pull/543))
- Manual Antichain::default to avoid bounds ([#537](https://github.com/TimelyDataflow/timely-dataflow/pull/537))
- Insert an element by reference into an antichain ([#536](https://github.com/TimelyDataflow/timely-dataflow/pull/536))
- Implement Columnation for Product ([#535](https://github.com/TimelyDataflow/timely-dataflow/pull/535))
- Drop implementation for Tracker ([#517](https://github.com/TimelyDataflow/timely-dataflow/pull/517))
- add missing Refines implementation for tuples ([#527](https://github.com/TimelyDataflow/timely-dataflow/pull/527))
- input handles: give empty containers to operators
- Only push if message non-empty
- Log messages over enter/leave channels ([#507](https://github.com/TimelyDataflow/timely-dataflow/pull/507))
- Inline give for Buffer ([#511](https://github.com/TimelyDataflow/timely-dataflow/pull/511))
- Fix typo ([#506](https://github.com/TimelyDataflow/timely-dataflow/pull/506))
- Back Mutableantichain by ChangeBatch ([#505](https://github.com/TimelyDataflow/timely-dataflow/pull/505))
- Validate timestamp summary before forming capability ([#497](https://github.com/TimelyDataflow/timely-dataflow/pull/497))
- unconstrained lifetime for `CapabilityRef` ([#491](https://github.com/TimelyDataflow/timely-dataflow/pull/491))
- Add cease to output handles ([#496](https://github.com/TimelyDataflow/timely-dataflow/pull/496))
- Activate operators that may want to shut down ([#488](https://github.com/TimelyDataflow/timely-dataflow/pull/488))
- updated consumed counts after capabilityrefs are dropped ([#429](https://github.com/TimelyDataflow/timely-dataflow/pull/429))
- Container-invariant Exchange ([#476](https://github.com/TimelyDataflow/timely-dataflow/pull/476))
- Container-invariant Reclock operator ([#474](https://github.com/TimelyDataflow/timely-dataflow/pull/474))
- Container-invariant BranchWhen operator ([#477](https://github.com/TimelyDataflow/timely-dataflow/pull/477))
- Derive Clone for activators ([#481](https://github.com/TimelyDataflow/timely-dataflow/pull/481))
- Turn a stream of data into a stream of shared data ([#471](https://github.com/TimelyDataflow/timely-dataflow/pull/471))
- Open the Inspect trait for more stream kinds ([#472](https://github.com/TimelyDataflow/timely-dataflow/pull/472))
- Avoid spinning when workers have no dataflows ([#463](https://github.com/TimelyDataflow/timely-dataflow/pull/463))
- Antichain `FromIterator` implementations ([#459](https://github.com/TimelyDataflow/timely-dataflow/pull/459))
- Add option conversions for totally ordered antichains ([#458](https://github.com/TimelyDataflow/timely-dataflow/pull/458))
- remove one layer of boxing ([#456](https://github.com/TimelyDataflow/timely-dataflow/pull/456))
- Add a tuple timestamp ([#455](https://github.com/TimelyDataflow/timely-dataflow/pull/455))
- implement Hash for Antichain<T> when T is Ord+Hash ([#454](https://github.com/TimelyDataflow/timely-dataflow/pull/454))
- Add From impls for MutableAntichain
- Derive (De)serialize for EventCore ([#451](https://github.com/TimelyDataflow/timely-dataflow/pull/451))
- enterleave should not depend on command line args ([#450](https://github.com/TimelyDataflow/timely-dataflow/pull/450))
- Correct references in doc comments ([#447](https://github.com/TimelyDataflow/timely-dataflow/pull/447))
- Implement TotalOrder for totally ordered things ([#449](https://github.com/TimelyDataflow/timely-dataflow/pull/449))
- Exchange operator can take FnMut ([#445](https://github.com/TimelyDataflow/timely-dataflow/pull/445))
- Container stream (without Allocation) ([#426](https://github.com/TimelyDataflow/timely-dataflow/pull/426))
- remove redundant `Error` impl ([#443](https://github.com/TimelyDataflow/timely-dataflow/pull/443))
- Update rand to 0.8 ([#436](https://github.com/TimelyDataflow/timely-dataflow/pull/436))
- De-duplicate partitioning logic ([#434](https://github.com/TimelyDataflow/timely-dataflow/pull/434))
- Improve comments
- Promote non-preallocating exchange to default
- Remove LazyExhange, rename eager to NonRetainingExchange
- Unify all exchange variants to generic impl
- Eagerly deallocating exchange pusher
- Add the LazyExchange type
- remove references to `limit`
- Explicitly implement Clone for Antichain ([#419](https://github.com/TimelyDataflow/timely-dataflow/pull/419))
- relax requirements of supplied streams ([#401](https://github.com/TimelyDataflow/timely-dataflow/pull/401))
- Introduce inspect_core operator ([#418](https://github.com/TimelyDataflow/timely-dataflow/pull/418))
- Fix error message on capability downgrade ([#417](https://github.com/TimelyDataflow/timely-dataflow/pull/417))
- Base message default length off element size ([#403](https://github.com/TimelyDataflow/timely-dataflow/pull/403))
- Fix [#395](https://github.com/TimelyDataflow/timely-dataflow/pull/395): Add next_count returning the notification count ([#396](https://github.com/TimelyDataflow/timely-dataflow/pull/396))
- Added debug implementations for a bunch of types ([#387](https://github.com/TimelyDataflow/timely-dataflow/pull/387))
- Antichain helper implementations ([#389](https://github.com/TimelyDataflow/timely-dataflow/pull/389))
- add Antichain::into_elements ([#388](https://github.com/TimelyDataflow/timely-dataflow/pull/388))
- Fix ability to eliminate getopts dep ([#385](https://github.com/TimelyDataflow/timely-dataflow/pull/385))
- Worked on Capability egronomics ([#384](https://github.com/TimelyDataflow/timely-dataflow/pull/384))
- Fixed bug in Subgraph::get_internal_summary() ([#383](https://github.com/TimelyDataflow/timely-dataflow/pull/383))
- Usability and performance tweaks ([#379](https://github.com/TimelyDataflow/timely-dataflow/pull/379))
- activate scopes when data enter ([#378](https://github.com/TimelyDataflow/timely-dataflow/pull/378))
- Reachability logging ([#375](https://github.com/TimelyDataflow/timely-dataflow/pull/375))
- small change, and test ci ([#364](https://github.com/TimelyDataflow/timely-dataflow/pull/364))
- do not rename operator on shutdown
- Reduce the amount of cloning in `reachability.rs` ([#374](https://github.com/TimelyDataflow/timely-dataflow/pull/374))
- 0.12 changes ([#372](https://github.com/TimelyDataflow/timely-dataflow/pull/372))
- Pre-allocate vectors ([#370](https://github.com/TimelyDataflow/timely-dataflow/pull/370))
- Correctly propagate worker config ([#371](https://github.com/TimelyDataflow/timely-dataflow/pull/371))
- Implemented Debug for Stream ([#361](https://github.com/TimelyDataflow/timely-dataflow/pull/361))
- Add support for native async stream sources ([#357](https://github.com/TimelyDataflow/timely-dataflow/pull/357))
- add Worker::step_or_park_while ([#363](https://github.com/TimelyDataflow/timely-dataflow/pull/363))
- Accept frontier changes in subgraph set-up ([#360](https://github.com/TimelyDataflow/timely-dataflow/pull/360))
- Actually log progress updates ([#352](https://github.com/TimelyDataflow/timely-dataflow/pull/352))
- Change default progress mode to demand ([#351](https://github.com/TimelyDataflow/timely-dataflow/pull/351))
- add ProgressMode documentation
- Provide for worker-configuration parameters ([#350](https://github.com/TimelyDataflow/timely-dataflow/pull/350))
- Bump to latest crossbeam-channel ([#349](https://github.com/TimelyDataflow/timely-dataflow/pull/349))
- Provide stream functions to handle Result streams ([#348](https://github.com/TimelyDataflow/timely-dataflow/pull/348))
- Documentation improvements ([#343](https://github.com/TimelyDataflow/timely-dataflow/pull/343))
- add a convenience function to make named dataflows with otherwise default arguments ([#342](https://github.com/TimelyDataflow/timely-dataflow/pull/342))
- correct spelling ([#339](https://github.com/TimelyDataflow/timely-dataflow/pull/339))
- Allow a Duration offset to logging times ([#337](https://github.com/TimelyDataflow/timely-dataflow/pull/337))
- Use crossbeam-channel instead of std::sync::mpsc ([#335](https://github.com/TimelyDataflow/timely-dataflow/pull/335))
- relax debug test ([#331](https://github.com/TimelyDataflow/timely-dataflow/pull/331))
- Update ok_err.rs ([#329](https://github.com/TimelyDataflow/timely-dataflow/pull/329))
- add OkErr for demux
- Merge branch 'master' of github.com:TimelyDataflow/timely-dataflow
- generalize build
- quick bugfix
- Add PartialOrder impls ([#322](https://github.com/TimelyDataflow/timely-dataflow/pull/322))
- add region_named region constructor
- Make crate paths absolute
- prepare for schedulers
- Derive Clone and Debug for CapabilitySet
- Add deref (with target &[Capability<T>]) and from_elem to CapabilitySet
- Implement CapabilityTrait for ActivateCapability
- Move ActivateCapability out of unordered_input module
- Fix grammar of main module docstring
- tidy
- delete deprecated files
- :minimum()
- Pre-allocate hash map
- Improve `is_acyclic`.
- bump minor version
- Merge pull request [#311](https://github.com/TimelyDataflow/timely-dataflow/pull/311) from TimelyDataflow/fix_293
- Document `connection` arguments.
- release 0.11
- change fn to fnmut
- Improve `timely::execute` documentation.
- address underflow
- introduce delayed activations
- Add interfaces for activating across thread boundaries
- Merge pull request [#285](https://github.com/TimelyDataflow/timely-dataflow/pull/285) from TimelyDataflow/progress_tidy
- tidying up progress
- add extend, bump abom
- add 128bit orders
- v0.10 release
- Merge pull request [#271](https://github.com/TimelyDataflow/timely-dataflow/pull/271) from TimelyDataflow/mutable_exchange
- Mutable Exchange
- simplify park
- log park and unpark
- don't park if no dataflows
- Merge pull request [#266](https://github.com/TimelyDataflow/timely-dataflow/pull/266) from TimelyDataflow/stream_concatenate
- add Concatenate for streams
- reorg to use Cargo workspaces
- update links based on ownership
- Fix second example in README.md
- Remove travis-cargo from .travis.yml, publish mdbook on gh-pages
- normalize final newline
- update contributions
- readme update
- Update README.md
- Updated intro example code in README to use 'dataflow' instead of 'scoped' to prevent a compile error when playing along at home
- corrected example
- corrected example
- updated with sassier introduction
- completely unclear what changed
- improved text
- typos
- updated example text to be more interesting
- use new index, input methods to make example smoother
- updated examples
- more examples and links
- more examples and links
- more examples and links
- updating examples with .unwrap()
- Fix example output
- updated open problems
- updated project ideas
- open problem updated
- various inline directives to support constant prop
- typo
- updated examples
- updates
- readme
- documentation edits
- readme
- updated readme
- more documentation
- documentation
- update example
- update example to reflect new interfaces
- tweaks
- tweaks
- updated examples, readme
- text
- updated example
- updated README
- updated readme
- updated readme
- updated readme
- updated readme
- updated readme
- Made Scope implementations take slices instead of vectors when applicable. Previously, the implementations were just expected to avoid deleting any inputs or changing the length of the vectors - this is now enforced by the types.
- Observer -> associated types; ICEs at the moment.
- typo
- updated intro text to be friendlier
- updated readme; better error in command.rs
- updated readme.md
- Removed Clone trait from Graph to help rustc out (false positive finding non-object-safe .clone() method)
- Added example execution
- Added starting out section
- reformatted README.md
- reformatted README.md
- Initial commit

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
