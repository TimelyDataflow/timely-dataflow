# Unsafe code audit tracker

Look for: all `unsafe` blocks, `unsafe impl`, and `unsafe fn` across the codebase.
Validate soundness, check for UB, aliasing violations, incorrect Send/Sync impls, and unsound abstractions.

## Crates and areas

### `bytes/` (1 file) — DONE
* [x] `bytes/src/lib.rs` — 4 unsafe usages (3 `from_raw_parts`/`from_raw_parts_mut`, 1 `unsafe impl Send`)

### `container/` (1 file) — DONE
* [x] `container/src/lib.rs` — clean (no unsafe)

### `logging/` (1 file) — DONE
* [x] `logging/src/lib.rs` — clean (no unsafe)

### `communication/` (20 files) — DONE
* [x] All files — clean (no unsafe in library code)
* [x] `communication/examples/lgalloc.rs` — 2 usages (example code, `from_raw_parts`/`from_raw_parts_mut` from lgalloc pointer)

### `timely/` — DONE
* [x] `timely/src/order.rs` — 1 usage (`unsafe fn copy` implementing `columnation::Region` trait)
* [x] All other files — clean (no unsafe)
