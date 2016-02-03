# timely-sort
An cache-aware implementation of radix sort in Rust.

Timely sort is a least-significant bit radix sort written in Rust. It is designed to be cache aware, and in particular to minimize the amount of data moved from the L1 cache up the memory hierarchy. Conventional wisdom is that these exchanges are what limit the scaling of sorting on modern multiprocessors.

There are currently two flavors of radix sorter: A vanilla `LSBRadixSorter` and a version which performs software write-combining, `LSBSWCRadixSorter`. Each maintain internal buffers of the elements `push`ed so far, initially partitioned by the least significant byte, and finish the sorting when you call `finish`. For examples, check out `benches/benches.rs`. At the moment, the documentation is not stellar.

For performance, consider sorting increasing numbers of random `u32` data. Here we go from `2 << 20` to `2 << 25` elements, using Rust's default `sort()`. You should see numbers like these when you run `cargo bench`:

	test msort_u32_20    ... bench:  19,182,135 ns/iter (+/- 1,022,566) = 218 MB/s
	test msort_u32_21    ... bench:  38,633,682 ns/iter (+/- 1,299,863) = 217 MB/s
	test msort_u32_22    ... bench:  78,986,400 ns/iter (+/- 1,691,552) = 212 MB/s
	test msort_u32_23    ... bench: 161,851,450 ns/iter (+/- 3,195,896) = 207 MB/s
	test msort_u32_24    ... bench: 329,526,599 ns/iter (+/- 5,655,355) = 203 MB/s
	test msort_u32_25    ... bench: 672,753,001 ns/iter (+/- 11,634,847) = 199 MB/s

The throughput drops, which makes sense because `msort` is an n log n algorithm.

Radix sort doesn't have the same issue, and apparently actually accelerates (don't ask; probably some buffer re-use I'm not getting right):

	test rsort_u32_20    ... bench:  19,704,006 ns/iter (+/- 1,643,605) = 212 MB/s
	test rsort_u32_21    ... bench:  34,972,698 ns/iter (+/- 1,925,898) = 239 MB/s
	test rsort_u32_22    ... bench:  63,746,111 ns/iter (+/- 3,033,849) = 263 MB/s
	test rsort_u32_23    ... bench: 120,175,182 ns/iter (+/- 7,227,761) = 279 MB/s
	test rsort_u32_24    ... bench: 232,857,170 ns/iter (+/- 12,383,304) = 288 MB/s
	test rsort_u32_25    ... bench: 458,974,307 ns/iter (+/- 17,849,213) = 292 MB/s

When we go to software write-combining, which keeps the heads of each of the buffers on their own cache line, spread across just a few pages to avoid eviction, we get even better numbers:

	test rsortswc_u32_20 ... bench:  11,996,260 ns/iter (+/- 1,615,053) = 349 MB/s
	test rsortswc_u32_21 ... bench:  24,176,384 ns/iter (+/- 1,989,330) = 346 MB/s
	test rsortswc_u32_22 ... bench:  48,412,808 ns/iter (+/- 3,983,659) = 346 MB/s
	test rsortswc_u32_23 ... bench:  98,133,997 ns/iter (+/- 7,162,924) = 341 MB/s
	test rsortswc_u32_24 ... bench: 197,561,440 ns/iter (+/- 13,321,125) = 339 MB/s
	test rsortswc_u32_25 ... bench: 395,547,731 ns/iter (+/- 19,570,709) = 339 MB/s

The code appears to work for me, but please be warned before using before your special purpose. There is logic to suss out the number of elements that fit on a 64 byte cache line, and the number that fit in a 4k page, but this logic may be horribly broken for untested cases.