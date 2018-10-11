# Changelog
All notable changes to this project will be documented in this file.

## Unreleased
- Many logging events have been rationalized. Operators and Channels should all have a worker-unique identifier that can be used to connect their metadata with events involving them. Previously this was a bit of a shambles.

- The `Scope` method `scoped` now allows supports new scopes with non-`Product` timestamps. Instead, the new timestamp must implement `Refines<_>` of the parent timestamp. This is the case for `Product` timestamps, but each timestamp also refines itself (allowing logical regions w/o changing the timestamp), and other timestamp combinators (e.g. Lexicographic) can be used.

- Timestamps no longer need to be `Product<RootTimestamp,_>`. Instead, the `_` can be used as the timestamp. The exception here is that the creation of loop variables requires a `Product<_,_>` structured timestamp, and so as much as you might like to have a scope with a `usize` timestamp and a loop, and you *should* be able to, you cannot yet without making the type `Product<(), usize>`.

- The `RootTimestamp` and `RootSummary` types have been excised. Where you previously used `Product<RootTimestamp,T>` you can now use `Product<(),T>`, or even better just `T`. The requirement of a worker's `dataflow()` method is that the timestamp type implement `Refines<()>`, which .. ideally would be true for all timestamps but we can't have a blanket implementation until specialization lands (I believe).

- The communication crate now has a `bincode` feature flag which should swing serialization over to use serde's `Serialize` trait. While it seems to work the ergonomics are likely in flux, as the choice is crate-wide and doesn't allow you to pick and choose a la carte.


## 0.7.0

### Added
- You can now construct your own vector of allocator builders and supply them directly to `timely::execute::execute_from`. Previously one was restricted to whatever a `Configuration` could provide for you. This should allow more pleasant construction of custom allocators, or custom construction of existing allocators.
- Each timely worker now has a log registry, `worker.log_registry()`, from which you can register and acquire typed loggers for named log streams. This supports user-level logging, as well as user-configurable timely logging. Timely logging is under the name `"timely"`.

### Changed
- The `Root` type has been renamed `Worker` and is found in the `::worker` module. The methods of the `ScopeParent` trait are now in the `::worker::AsWorker` trait.
- The communication `Allocate` trait's main method `allocate` now takes a worker-unique identifier to use for the channel. The allocator may or may not use the information (most often for logging), but they are allowed to be incorrect if one allocates two channels with the same identifier.
- A `CapabilityRef<T>` now supports `retain_for(usize)` which indicates a specific output port the capability should be retain for use with. The `retain()` method still exists for now and is equivalent to `retain(0)`. This change also comes with the *inability* to use an arbitrary `Capability<T>` with any output; using a capability bound to the wrong output will result in a run-time error.
- The `unary` and `binary` operators now provide `data` as a `RefOrMut`, which does not implement `DerefMut`. More information on how to port methods can be found [here](https://github.com/frankmcsherry/timely-dataflow/pull/135#issuecomment-418355284).


### Removed
- The deprecated `Unary` and `Binary` operator extension traits have been removed in favor of the `Operator` trait that supports both of them, as well as their `_notify` variants.