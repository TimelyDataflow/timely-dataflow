# mdbook documentation audit log

## Overview

All 47 compilable code examples (`rust` blocks without `ignore`) pass.
The problems are concentrated in chapters 4-5, where `rust,ignore` blocks show outdated API signatures.
Chapters 0-3 are largely accurate, with only cosmetic issues.

## Findings

### 1. Broken GitHub links to examples directory

**Severity: low**

* `chapter_0/chapter_0_1.md:5` — Links to `examples/hello.rs` at the repo root.
  The actual path is `timely/examples/hello.rs`.
  The link to the `examples/` tree is similarly wrong.

### 2. Outdated build output with removed dependencies

**Severity: low**

* `chapter_0/chapter_0_1.md:62-70` — Shows `timely v0.2.0`, `timely_sort v0.1.6`, `abomonation v0.4.5`, `time v0.1.38`.
  Current version is `v0.27.0`.
  Dependencies `timely_sort`, `abomonation`, `time`, and `libc` no longer exist.
  Build output also exposes a developer's local path (`/Users/mcsherry/Projects/temporary/timely-dataflow`).

### 3. Outdated Rayon repository URL

**Severity: low**

* `chapter_0/chapter_0_3.md:13` — Links to `https://github.com/nikomatsakis/rayon`.
  Rayon moved to `https://github.com/rayon-rs/rayon`.

### 4. Missing dereference in delay example

**Severity: medium — code won't compile**

* `chapter_4/chapter_4_3.md:78` — `.delay(|number, time| number / 100)` where `number` is `&u64`.
  Cannot divide a reference by an integer.
  Should be `*number / 100`.

### 5. Stale `capture_into` implementation

**Severity: high — completely wrong API**

* `chapter_4/chapter_4_4.md:12-42` — Shows a two-closure `OperatorBuilder::build` API:
  ```rust
  builder.build(
      move |frontier| { ... },   // frontier closure
      move |input, output| { ... } // data closure
  );
  ```
  The actual API takes a single closure with a `&mut SharedProgress` parameter.
  The generic parameter `D` is now `C` (container-generic).

### 6. Stale `replay_into` implementation

**Severity: high — completely wrong API**

* `chapter_4/chapter_4_4.md:55-96` — Same two-closure API issue.
  Also references `Event::Start` (does not exist) and calls `output.cease()` (does not exist, now `output.done()`).
  Return type shown as `StreamVec<S, D>`, actual is `Stream<S, C>`.

### 7. Dead abomonation reference

**Severity: medium**

* `chapter_4/chapter_4_4.md:115` — Claims serialization uses abomonation.
  The crate now uses `serde`/`bincode` via the `Bytesable` trait.
  Abomonation is no longer a dependency.

### 8. Dead kafkaesque reference

**Severity: low**

* `chapter_4/chapter_4_4.md:212` — References an "in-progress Kafka adapter" at `kafkaesque/` in the repo.
  This directory does not exist.

### 9. Chapter 4.5 self-acknowledged as incorrect

**Severity: high — entire chapter**

* `chapter_4/chapter_4_5.md:4` — Contains `**THIS TEXT IS LARGELY INCORRECT AND NEEDS IMPROVEMENT**`.

### 10. Wrong `Data` trait definition

**Severity: medium**

* `chapter_4/chapter_4_5.md:11-12` — Describes `Data` as "essentially a synonym for `Clone+'static`".
  There is no separate `Data` trait in the timely crate.
  Types used within a worker need `'static`; cross-thread exchange uses `ExchangeData` which requires `Send+Any+Serialize+Deserialize`.

### 11. Wrong `ExchangeData` trait definition

**Severity: medium**

* `chapter_4/chapter_4_5.md:22-24` — Describes `ExchangeData` with a `Sync` bound.
  The actual trait does not require `Sync`.

### 12. Missing import for `Map` operator

**Severity: medium — code won't compile**

* `chapter_4/chapter_4_5.md:48-53,110-117` — Uses `use timely::dataflow::operators::*` which does not bring `Map` into scope.
  The `map` method requires `use timely::dataflow::operators::vec::Map`.

### 13. Wrong `Config` enum name and variants

**Severity: medium**

* `chapter_5/chapter_5_1.md:84-94` — Shows `Configuration` enum with variants `Thread`, `Process(usize)`, `Cluster(usize, usize, Vec<String>, bool)`.
  Actual name is `Config`.
  Missing `ProcessBinary(usize)` variant.
  `Cluster` is now a struct variant (`Cluster { threads, process, addresses, report, log_fn }`), not a tuple variant.

### 14. Wrong `Allocate::allocate` signature

**Severity: medium**

* `chapter_5/chapter_5_1.md:104-109` — Shows `fn allocate<T: Data>(&mut self) -> (Vec<Box<Push<T>>>, Box<Pull<T>>)`.
  Actual: `fn allocate<T: Exchangeable>(&mut self, identifier: usize) -> (Vec<Box<dyn Push<T>>>, Box<dyn Pull<T>>)`.
  Wrong trait bound (`Data` vs `Exchangeable`), missing `identifier` parameter, missing `dyn` keywords.

### 15. Wrong `Bytesable` trait signature

**Severity: high — completely different API**

* `chapter_5/chapter_5_1.md:129-134` — Shows `fn into_bytes(&mut self, &mut Vec<u8>)` and `fn from_bytes(&mut Vec<u8>) -> Self`.
  Actual: `fn from_bytes(bytes: Bytes) -> Self`, `fn length_in_bytes(&self) -> usize`, `fn into_bytes<W: Write>(&self, writer: &mut W)`.
  Every method signature is different; `length_in_bytes` is missing entirely.

### 16. Wrong `Source`/`Target` field name

**Severity: medium**

* `chapter_5/chapter_5_2.md:23-37` — Shows field name `pub index: usize` in both `Source` and `Target`.
  Actual field name is `pub node: usize`.

### 17. Incomplete `Timestamp` trait definition

**Severity: medium**

* `chapter_5/chapter_5_2.md:73-77` — Shows `pub trait Timestamp: PartialOrder { type Summary: PathSummary<Self>; }`.
  Actual: `pub trait Timestamp: Clone+Eq+PartialOrder+Ord+Debug+Any+ExchangeData { type Summary: PathSummary<Self> + 'static; fn minimum() -> Self; }`.
  Missing supertraits, `'static` bound, and `minimum()` method.

### 18. Incomplete `PathSummary` trait definition

**Severity: medium**

* `chapter_5/chapter_5_2.md:81-85` — Shows `pub trait PathSummary<T>: PartialOrder { ... }`.
  Actual: `pub trait PathSummary<T>: Clone+Eq+PartialOrder+Debug+Default { ... }`.
  Missing `Clone+Eq+Debug+Default` supertraits.

### 19. Stale `Operate` trait description

**Severity: low-medium**

* `chapter_5/chapter_5_2.md:115` — Describes `Operate` as a trait operators implement for scheduling.
  Per commit `39ba5a74`, `Operate` is now a builder trait that consumes itself via `initialize()`.
  The description is not entirely wrong but omits the builder pattern.

### 20. Placeholder TODO in containers chapter

**Severity: low**

* `chapter_5/chapter_5_3.md:27` — Contains a TODO placeholder ("Explain why it's hard to build container-generic operators...") instead of actual content.
