# Unsafe code audit log

## Overview

The codebase has a very small unsafe surface: 7 total `unsafe` usages across 3 files, all concentrated in low-level byte handling.
No `unsafe` exists in the `container`, `logging`, or `communication` library crates, nor in most of `timely`.

## Findings

### 1. `BytesMut` raw pointer aliasing relies on `Arc` exclusivity invariant

**Severity: medium — soundness depends on informal invariant**

* `bytes/src/lib.rs:47-58` — `BytesMut` stores a raw `*mut u8` pointer alongside an `Arc<dyn Any>` that owns the backing allocation.
  The safety argument is that the `Arc` prevents the allocation from being freed while any `BytesMut`/`Bytes` slice exists.
* `bytes/src/lib.rs:157` — `Deref` for `BytesMut` does `from_raw_parts(self.ptr, self.len)`.
* `bytes/src/lib.rs:164` — `DerefMut` for `BytesMut` does `from_raw_parts_mut(self.ptr, self.len)`.
* The struct comment at line 55-56 acknowledges uncertainty: "I'm not sure I understand Rust's rules enough to make a stronger statement about this."

**Soundness analysis:**
The key invariant is that no two `BytesMut` instances share overlapping byte ranges.
`extract_to` (line 90) splits ranges using `assert!(index <= self.len)` and pointer arithmetic, which maintains non-overlap.
However, `BytesMut::from` (line 63) takes `self` by value, so there's no path to create overlapping `BytesMut` slices through the public API.
`Bytes` (the frozen variant) only provides `&[u8]`, so multiple `Bytes` over the same range are fine.
**Verdict: sound**, assuming the backing allocation (`B: DerefMut<Target=[u8]>`) keeps its buffer stable after being moved into an `Arc`.
This is guaranteed for `Vec<u8>` and `Box<[u8]>`, which are the only types used in practice.

### 2. `unsafe impl Send for Bytes` — missing `Sync`

**Severity: low — correct but incomplete**

* `bytes/src/lib.rs:190` — `Bytes` manually implements `Send` but not `Sync`.
  `Bytes` contains `*const u8` (not `Send`/`Sync` by default) and `Arc<dyn Any>` (which is `Send + Sync`).
* Since `Bytes` only provides `&[u8]` access (no interior mutability), it would also be safe to implement `Sync`.
  The omission means `Bytes` cannot be shared by reference across threads, only moved — which is a minor API limitation, not a soundness issue.

**Verdict: sound.** `Send` is correct because moving a `Bytes` to another thread is safe (the `Arc` keeps the allocation alive, and the `*const u8` points into it).
`Sync` could be added but its absence is conservative, not unsound.

### 3. `BytesMut` is not `Send` — intentional but undocumented

**Severity: low — API limitation**

* `BytesMut` contains `*mut u8`, which opts out of auto-`Send`.
  No manual `unsafe impl Send` is provided for `BytesMut`.
* The module doc at line 5 notes: "`BytesMut` should be able to implement `Send`" — this is correct, the same safety argument as `Bytes` applies.
  The `*mut u8` does not create aliasing across threads because `BytesMut` enforces exclusive access.

**Verdict: sound** (conservative — not unsound, just a missing capability).

### 4. `ProductRegion::copy` delegates to inner regions

**Severity: none — trait obligation**

* `timely/src/order.rs:174` — `unsafe fn copy(&mut self, item: &Self::Item) -> Self::Item` implements the `columnation::Region` trait.
  The implementation simply delegates to `self.outer_region.copy()` and `self.inner_region.copy()`.
* The unsafety contract is defined by the `columnation` crate (external dependency, version 0.1).
  The `Product` implementation adds no new unsafe behavior beyond what the inner regions do.

**Verdict: sound** (assuming the `columnation` crate's `Region` contract is sound).

### 5. lgalloc example uses raw pointers from allocator

**Severity: none — example code**

* `communication/examples/lgalloc.rs:47,54` — `Deref`/`DerefMut` for `LgallocHandle` use `from_raw_parts`/`from_raw_parts_mut` with a `NonNull<u8>` pointer and `capacity` from `lgalloc::allocate`.
* Soundness depends on `lgalloc::allocate` returning a valid pointer and capacity, and `lgalloc::deallocate` being called exactly once (ensured by `Drop`).

**Verdict: sound** for an example (not library code, not reusable).

## Summary

| # | Location | Type | Verdict |
|---|----------|------|---------|
| 1 | `bytes/src/lib.rs:157,164` | `from_raw_parts[_mut]` | Sound (invariant maintained by API) |
| 2 | `bytes/src/lib.rs:190` | `unsafe impl Send` | Sound (conservative, could also be `Sync`) |
| 3 | `bytes/src/lib.rs:253` | `from_raw_parts` | Sound (same as #1, read-only variant) |
| 4 | `timely/src/order.rs:174` | `unsafe fn copy` | Sound (delegates to inner regions) |
| 5 | `communication/examples/lgalloc.rs:47,54` | `from_raw_parts[_mut]` | Sound (example code) |

No unsoundness found.
The most notable observation is the `bytes` crate's comment expressing uncertainty about its own safety argument (line 55-56), though analysis confirms the invariants hold.
