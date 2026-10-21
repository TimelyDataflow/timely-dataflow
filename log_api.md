# API consistency audit log

## `core::Input::Handle` — `epoch()` and `time()` return the identical value

`Handle<T, CB>` at `timely/src/dataflow/operators/core/input.rs:473-480` defines two methods:
```rust
pub fn epoch(&self) -> &T { &self.now_at }
pub fn time(&self) -> &T { &self.now_at }
```

Both return a reference to the same field.
Users have no way to know which to call, and the existence of both suggests they might differ.

Suggestion: deprecate `epoch()` in favor of `time()`, which is the name used everywhere else (e.g. `Capability::time()`, `probe::Handle::less_than(time)`).

## `Antichain::from_elem` vs `MutableAntichain::new_bottom` — inconsistent singleton constructors

`Antichain::from_elem(element)` at `frontier.rs:222` and `MutableAntichain::new_bottom(bottom)` at `frontier.rs:447` both create a singleton antichain, but use different naming conventions.

`from_elem` follows Rust standard library conventions.
`new_bottom` uses domain-specific naming.

Suggestion: add `MutableAntichain::from_elem` as an alias, or rename `new_bottom` to `from_elem` for consistency.

## `core` vs `vec` — operators with no `core` equivalent

Several operators exist only in `vec` with no `core`-level generalization:

* `vec::Delay` (`delay`, `delay_total`, `delay_batch`)
* `vec::Broadcast`
* `vec::Branch` (data-dependent branching; note: `BranchWhen` works on generic `Stream<S, C>` but lives in `vec::branch`)
* `vec::count::Accumulate` (`accumulate`, `count`)
* `vec::ResultStream` (`ok`, `err`, `map_ok`, `map_err`, `and_then`, `unwrap_or_else`)
* `vec::flow_controlled::iterator_source`

These operators are only available for `StreamVec<G, D>`, not for arbitrary container types.
Users who switch from `Vec` to a custom container must reimplement this functionality.

The most impactful gaps are `Delay` and `Branch`, which are fundamental dataflow operations.

## `vec::BranchWhen` — lives in `vec` module but operates on generic `Stream<S, C>`

`BranchWhen` at `vec/branch.rs:102` is implemented for `Stream<S, C>` (generic containers), not `StreamVec`:
```rust
impl<S: Scope, C: Container> BranchWhen<S::Timestamp> for Stream<S, C> { ... }
```

It is defined in the `vec` module but does not depend on `Vec` containers.
It should be in `core` alongside `OkErr`, `Partition`, etc.

## `vec::Partition` — closure type as trait parameter

`vec::Partition` at `vec/partition.rs:8` puts the closure type in the trait generics:
```rust
pub trait Partition<G: Scope, D: 'static, D2: 'static, F: Fn(D) -> (u64, D2)> {
    fn partition(self, parts: u64, route: F) -> Vec<StreamVec<G, D2>>;
}
```

`core::Partition` at `core/partition.rs:11` keeps the closure as a method-level generic:
```rust
pub trait Partition<G: Scope, C: DrainContainer> {
    fn partition<CB, D2, F>(self, parts: u64, route: F) -> Vec<Stream<G, CB::Container>>
    where ...;
}
```

The `vec` style makes the trait harder to import and use, because the user must specify all type parameters.
Every other operator trait in both `core` and `vec` uses method-level generics for closures.

## `Antichain` has `with_capacity` but `MutableAntichain` does not

`Antichain::with_capacity(capacity)` at `frontier.rs:207` pre-allocates space.
`MutableAntichain` has no `with_capacity` constructor, despite wrapping internal collections that support it.

This is a minor gap, but inconsistent across the two related types.

## `Antichain::extend` shadows `std::iter::Extend`

`Antichain::extend` at `frontier.rs:118` has signature:
```rust
pub fn extend<I: IntoIterator<Item=T>>(&mut self, iterator: I) -> bool
```

This returns `bool` (whether any element was inserted), which conflicts with the `std::iter::Extend` trait (which returns `()`).
As a result, `Antichain` cannot implement `std::iter::Extend`, though it does implement `FromIterator`.

Users expecting the standard `Extend` trait to work will be surprised.

## `core::Map` has `flat_map_builder`, `vec::Map` has `map_in_place` — non-overlapping extensions

`core::Map` provides `map`, `flat_map`, and `flat_map_builder`.
`vec::Map` provides `map`, `flat_map`, and `map_in_place`.

`flat_map_builder` (a zero-cost iterator combinator pattern) has no vec counterpart.
`map_in_place` (mutation without allocation) has no core counterpart.

These are both useful optimizations that are only available in one of the two module hierarchies.

## `core::OkErr` has no `vec` counterpart

`core::OkErr` splits a stream by a closure returning `Result<D1, D2>`.
The `vec` module has no `OkErr` trait — instead it has `ResultStream` which operates on streams of `Result` values.
These serve different purposes:
* `OkErr` splits any stream into two streams (general routing)
* `ResultStream` processes streams whose data is already `Result<T, E>`

A `vec::OkErr` wrapper (delegating to `core::OkErr`) would be consistent with how `vec::Filter`, `vec::Partition`, etc. wrap their core counterparts.

## `Bytes` and `BytesMut` — asymmetric `try_merge`

`Bytes` has `try_merge(&mut self, other: Bytes) -> Result<(), Bytes>` at `bytes/src/lib.rs:238`.
`BytesMut` has no `try_merge` method.

To merge `BytesMut` slices, one must first `freeze()` them into `Bytes`, merge, then work with the result.
The module-level doc example demonstrates this pattern, but it is an API asymmetry.

The crate docs note this: "The crate is currently minimalist rather than maximalist."
Still, the asymmetry means `BytesMut` users must navigate a more complex workflow.
