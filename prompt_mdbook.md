# mdbook documentation audit tracker

Compare documentation against actual implementation.
Look for: broken code examples, incorrect API signatures, outdated behavior descriptions, dead links, stale references.

## Chapters

### Chapter 0 — Motivation — DONE
* [x] `chapter_0.md` — clean
* [x] `chapter_0_0.md` — clean
* [x] `chapter_0_1.md` — 3 findings (broken example links, outdated build output, version mismatch)
* [x] `chapter_0_2.md` — clean
* [x] `chapter_0_3.md` — 1 finding (outdated Rayon URL)

### Chapter 1 — Core concepts — DONE
* [x] `chapter_1.md` — clean
* [x] `chapter_1_1.md` — clean
* [x] `chapter_1_2.md` — clean
* [x] `chapter_1_3.md` — clean

### Chapter 2 — Building dataflows — DONE
* [x] `chapter_2.md` — clean
* [x] `chapter_2_1.md` — clean
* [x] `chapter_2_2.md` — clean
* [x] `chapter_2_3.md` — clean
* [x] `chapter_2_4.md` — clean
* [x] `chapter_2_5.md` — clean

### Chapter 3 — Running dataflows — DONE
* [x] `chapter_3.md` — clean
* [x] `chapter_3_1.md` — clean
* [x] `chapter_3_2.md` — clean
* [x] `chapter_3_3.md` — clean
* [x] `chapter_3_4.md` — clean

### Chapter 4 — Advanced — DONE
* [x] `chapter_4.md` — clean
* [x] `chapter_4_1.md` — clean
* [x] `chapter_4_2.md` — clean
* [x] `chapter_4_3.md` — 1 finding (deref bug in delay example)
* [x] `chapter_4_4.md` — 4 findings (stale capture/replay implementations, abomonation reference, dead kafkaesque link)
* [x] `chapter_4_5.md` — 3 findings (self-acknowledged "LARGELY INCORRECT", wrong Data/ExchangeData trait definitions, missing import)

### Chapter 5 — Internals — DONE
* [x] `chapter_5.md` — clean
* [x] `chapter_5_1.md` — 3 findings (wrong Config enum, wrong Allocate signature, wrong Bytesable signature)
* [x] `chapter_5_2.md` — 4 findings (Source/Target field name, Timestamp/PathSummary trait signatures, stale Operate description)
* [x] `chapter_5_3.md` — 1 finding (TODO placeholder instead of content)

### Build infrastructure — DONE
* [x] `build.rs` — works correctly (generates doc tests from markdown)
* [x] `lib.rs` — clean
* [x] All 47 compilable code examples pass
