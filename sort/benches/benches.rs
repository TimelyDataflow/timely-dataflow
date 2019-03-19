extern crate timely_sort as haeoua;

use bencher::{Bencher, benchmark_main, benchmark_group};
use rand::{Rng, SeedableRng};
use rand::distributions::{Distribution, Standard};
use rand::rngs::StdRng;
use haeoua::*;

use haeoua::{RadixSorter, RadixSorterBase};

fn rsort_lsb_u32_10(bencher: &mut Bencher) { radix_sort::<u32,_,_>(bencher, 1 << 10, &|&x| x); }
fn rsort_lsb_u32_11(bencher: &mut Bencher) { radix_sort::<u32,_,_>(bencher, 1 << 11, &|&x| x); }
fn rsort_lsb_u32_12(bencher: &mut Bencher) { radix_sort::<u32,_,_>(bencher, 1 << 12, &|&x| x); }
fn rsort_lsb_u32_13(bencher: &mut Bencher) { radix_sort::<u32,_,_>(bencher, 1 << 13, &|&x| x); }
fn rsort_lsb_u32_14(bencher: &mut Bencher) { radix_sort::<u32,_,_>(bencher, 1 << 14, &|&x| x); }
fn rsort_lsb_u32_15(bencher: &mut Bencher) { radix_sort::<u32,_,_>(bencher, 1 << 15, &|&x| x); }
fn rsort_lsb_u32_16(bencher: &mut Bencher) { radix_sort::<u32,_,_>(bencher, 1 << 16, &|&x| x); }
fn rsort_lsb_u32_17(bencher: &mut Bencher) { radix_sort::<u32,_,_>(bencher, 1 << 17, &|&x| x); }
fn rsort_lsb_u32_18(bencher: &mut Bencher) { radix_sort::<u32,_,_>(bencher, 1 << 18, &|&x| x); }
fn rsort_lsb_u32_19(bencher: &mut Bencher) { radix_sort::<u32,_,_>(bencher, 1 << 19, &|&x| x); }
fn rsort_lsb_u32_20(bencher: &mut Bencher) { radix_sort::<u32,_,_>(bencher, 1 << 20, &|&x| x); }
// fn rsort_lsb_u32_21(bencher: &mut Bencher) { radix_sort::<u32,_,_>(bencher, 1 << 21, &|&x| x); }
// fn rsort_lsb_u32_22(bencher: &mut Bencher) { radix_sort::<u32,_,_>(bencher, 1 << 22, &|&x| x); }
// fn rsort_lsb_u32_23(bencher: &mut Bencher) { radix_sort::<u32,_,_>(bencher, 1 << 23, &|&x| x); }
// fn rsort_lsb_u32_24(bencher: &mut Bencher) { radix_sort::<u32,_,_>(bencher, 1 << 24, &|&x| x); }
// fn rsort_lsb_u32_25(bencher: &mut Bencher) { radix_sort::<u32,_,_>(bencher, 1 << 25, &|&x| x); }

fn rsort_msb_u32_10(bencher: &mut Bencher) { radix_sort_msb::<u32,_,_>(bencher, 1 << 10, &|&x| x); }
fn rsort_msb_u32_11(bencher: &mut Bencher) { radix_sort_msb::<u32,_,_>(bencher, 1 << 11, &|&x| x); }
fn rsort_msb_u32_12(bencher: &mut Bencher) { radix_sort_msb::<u32,_,_>(bencher, 1 << 12, &|&x| x); }
fn rsort_msb_u32_13(bencher: &mut Bencher) { radix_sort_msb::<u32,_,_>(bencher, 1 << 13, &|&x| x); }
fn rsort_msb_u32_14(bencher: &mut Bencher) { radix_sort_msb::<u32,_,_>(bencher, 1 << 14, &|&x| x); }
fn rsort_msb_u32_15(bencher: &mut Bencher) { radix_sort_msb::<u32,_,_>(bencher, 1 << 15, &|&x| x); }
fn rsort_msb_u32_16(bencher: &mut Bencher) { radix_sort_msb::<u32,_,_>(bencher, 1 << 16, &|&x| x); }
fn rsort_msb_u32_17(bencher: &mut Bencher) { radix_sort_msb::<u32,_,_>(bencher, 1 << 17, &|&x| x); }
fn rsort_msb_u32_18(bencher: &mut Bencher) { radix_sort_msb::<u32,_,_>(bencher, 1 << 18, &|&x| x); }
fn rsort_msb_u32_19(bencher: &mut Bencher) { radix_sort_msb::<u32,_,_>(bencher, 1 << 19, &|&x| x); }
fn rsort_msb_u32_20(bencher: &mut Bencher) { radix_sort_msb::<u32,_,_>(bencher, 1 << 20, &|&x| x); }
// fn rsort_msb_u32_21(bencher: &mut Bencher) { radix_sort_msb::<u32,_,_>(bencher, 1 << 21, &|&x| x); }
// fn rsort_msb_u32_22(bencher: &mut Bencher) { radix_sort_msb::<u32,_,_>(bencher, 1 << 22, &|&x| x); }
// fn rsort_msb_u32_23(bencher: &mut Bencher) { radix_sort_msb::<u32,_,_>(bencher, 1 << 23, &|&x| x); }
// fn rsort_msb_u32_24(bencher: &mut Bencher) { radix_sort_msb::<u32,_,_>(bencher, 1 << 24, &|&x| x); }
// fn rsort_msb_u32_25(bencher: &mut Bencher) { radix_sort_msb::<u32,_,_>(bencher, 1 << 25, &|&x| x); }

// fn rsort_lsb_u32x2_20(bencher: &mut Bencher) { radix_sort::<(u32, u32),_,_>(bencher, 1 << 20, &|&x| x.0); }
// fn rsort_lsb_u32x2_21(bencher: &mut Bencher) { radix_sort::<(u32, u32),_,_>(bencher, 1 << 21, &|&x| x.0); }
// fn rsort_lsb_u32x2_22(bencher: &mut Bencher) { radix_sort::<(u32, u32),_,_>(bencher, 1 << 22, &|&x| x.0); }
// fn rsort_lsb_u32x2_23(bencher: &mut Bencher) { radix_sort::<(u32, u32),_,_>(bencher, 1 << 23, &|&x| x.0); }
// fn rsort_lsb_u32x2_24(bencher: &mut Bencher) { radix_sort::<(u32, u32),_,_>(bencher, 1 << 24, &|&x| x.0); }
// fn rsort_lsb_u32x2_25(bencher: &mut Bencher) { radix_sort::<(u32, u32),_,_>(bencher, 1 << 25, &|&x| x.0); }

fn rsort_lsb_swc_u32_10(bencher: &mut Bencher) { radix_sort_swc::<u32,_,_>(bencher, 1 << 10, &|&x| x); }
fn rsort_lsb_swc_u32_11(bencher: &mut Bencher) { radix_sort_swc::<u32,_,_>(bencher, 1 << 11, &|&x| x); }
fn rsort_lsb_swc_u32_12(bencher: &mut Bencher) { radix_sort_swc::<u32,_,_>(bencher, 1 << 12, &|&x| x); }
fn rsort_lsb_swc_u32_13(bencher: &mut Bencher) { radix_sort_swc::<u32,_,_>(bencher, 1 << 13, &|&x| x); }
fn rsort_lsb_swc_u32_14(bencher: &mut Bencher) { radix_sort_swc::<u32,_,_>(bencher, 1 << 14, &|&x| x); }
fn rsort_lsb_swc_u32_15(bencher: &mut Bencher) { radix_sort_swc::<u32,_,_>(bencher, 1 << 15, &|&x| x); }
fn rsort_lsb_swc_u32_16(bencher: &mut Bencher) { radix_sort_swc::<u32,_,_>(bencher, 1 << 16, &|&x| x); }
fn rsort_lsb_swc_u32_17(bencher: &mut Bencher) { radix_sort_swc::<u32,_,_>(bencher, 1 << 17, &|&x| x); }
fn rsort_lsb_swc_u32_18(bencher: &mut Bencher) { radix_sort_swc::<u32,_,_>(bencher, 1 << 18, &|&x| x); }
fn rsort_lsb_swc_u32_19(bencher: &mut Bencher) { radix_sort_swc::<u32,_,_>(bencher, 1 << 19, &|&x| x); }
fn rsort_lsb_swc_u32_20(bencher: &mut Bencher) { radix_sort_swc::<u32,_,_>(bencher, 1 << 20, &|&x| x); }
// fn rsort_lsb_swc_u32_21(bencher: &mut Bencher) { radix_sort_swc::<u32,_,_>(bencher, 1 << 21, &|&x| x); }
// fn rsort_lsb_swc_u32_22(bencher: &mut Bencher) { radix_sort_swc::<u32,_,_>(bencher, 1 << 22, &|&x| x); }
// fn rsort_lsb_swc_u32_23(bencher: &mut Bencher) { radix_sort_swc::<u32,_,_>(bencher, 1 << 23, &|&x| x); }
// fn rsort_lsb_swc_u32_24(bencher: &mut Bencher) { radix_sort_swc::<u32,_,_>(bencher, 1 << 24, &|&x| x); }
// fn rsort_lsb_swc_u32_25(bencher: &mut Bencher) { radix_sort_swc::<u32,_,_>(bencher, 1 << 25, &|&x| x); }

fn rsort_msb_swc_u32_10(bencher: &mut Bencher) { radix_sort_msb_swc::<u32,_,_>(bencher, 1 << 10, &|&x| x); }
fn rsort_msb_swc_u32_11(bencher: &mut Bencher) { radix_sort_msb_swc::<u32,_,_>(bencher, 1 << 11, &|&x| x); }
fn rsort_msb_swc_u32_12(bencher: &mut Bencher) { radix_sort_msb_swc::<u32,_,_>(bencher, 1 << 12, &|&x| x); }
fn rsort_msb_swc_u32_13(bencher: &mut Bencher) { radix_sort_msb_swc::<u32,_,_>(bencher, 1 << 13, &|&x| x); }
fn rsort_msb_swc_u32_14(bencher: &mut Bencher) { radix_sort_msb_swc::<u32,_,_>(bencher, 1 << 14, &|&x| x); }
fn rsort_msb_swc_u32_15(bencher: &mut Bencher) { radix_sort_msb_swc::<u32,_,_>(bencher, 1 << 15, &|&x| x); }
fn rsort_msb_swc_u32_16(bencher: &mut Bencher) { radix_sort_msb_swc::<u32,_,_>(bencher, 1 << 16, &|&x| x); }
fn rsort_msb_swc_u32_17(bencher: &mut Bencher) { radix_sort_msb_swc::<u32,_,_>(bencher, 1 << 17, &|&x| x); }
fn rsort_msb_swc_u32_18(bencher: &mut Bencher) { radix_sort_msb_swc::<u32,_,_>(bencher, 1 << 18, &|&x| x); }
fn rsort_msb_swc_u32_19(bencher: &mut Bencher) { radix_sort_msb_swc::<u32,_,_>(bencher, 1 << 19, &|&x| x); }
fn rsort_msb_swc_u32_20(bencher: &mut Bencher) { radix_sort_msb_swc::<u32,_,_>(bencher, 1 << 20, &|&x| x); }
// fn rsort_msb_u32_21(bencher: &mut Bencher) { radix_sort_msb_swc::<u32,_,_>(bencher, 1 << 21, &|&x| x); }
// fn rsort_msb_u32_22(bencher: &mut Bencher) { radix_sort_msb_swc::<u32,_,_>(bencher, 1 << 22, &|&x| x); }
// fn rsort_msb_u32_23(bencher: &mut Bencher) { radix_sort_msb_swc::<u32,_,_>(bencher, 1 << 23, &|&x| x); }
// fn rsort_msb_u32_24(bencher: &mut Bencher) { radix_sort_msb_swc::<u32,_,_>(bencher, 1 << 24, &|&x| x); }
// fn rsort_msb_u32_25(bencher: &mut Bencher) { radix_sort_msb_swc::<u32,_,_>(bencher, 1 << 25, &|&x| x); }

// fn pdq_u32_10(bencher: &mut Bencher) { pdq_sort::<u32>(bencher, 1 << 10); }
// fn pdq_u32_11(bencher: &mut Bencher) { pdq_sort::<u32>(bencher, 1 << 11); }
// fn pdq_u32_12(bencher: &mut Bencher) { pdq_sort::<u32>(bencher, 1 << 12); }
// fn pdq_u32_13(bencher: &mut Bencher) { pdq_sort::<u32>(bencher, 1 << 13); }
// fn pdq_u32_14(bencher: &mut Bencher) { pdq_sort::<u32>(bencher, 1 << 14); }
// fn pdq_u32_15(bencher: &mut Bencher) { pdq_sort::<u32>(bencher, 1 << 15); }
// fn pdq_u32_16(bencher: &mut Bencher) { pdq_sort::<u32>(bencher, 1 << 16); }
// fn pdq_u32_17(bencher: &mut Bencher) { pdq_sort::<u32>(bencher, 1 << 17); }
// fn pdq_u32_18(bencher: &mut Bencher) { pdq_sort::<u32>(bencher, 1 << 18); }
// fn pdq_u32_19(bencher: &mut Bencher) { pdq_sort::<u32>(bencher, 1 << 19); }
// fn pdq_u32_20(bencher: &mut Bencher) { pdq_sort::<u32>(bencher, 1 << 20); }
// fn pdq_u32_21(bencher: &mut Bencher) { pdq_sort::<u32>(bencher, 1<<21); }
// fn pdq_u32_22(bencher: &mut Bencher) { pdq_sort::<u32>(bencher, 1<<22); }
// fn pdq_u32_23(bencher: &mut Bencher) { pdq_sort::<u32>(bencher, 1<<23); }
// fn pdq_u32_24(bencher: &mut Bencher) { pdq_sort::<u32>(bencher, 1<<24); }
// fn pdq_u32_25(bencher: &mut Bencher) { pdq_sort::<u32>(bencher, 1<<25); }

// fn msort_u32_10(bencher: &mut Bencher) { merge_sort::<u32>(bencher, 1 << 10); }
// fn msort_u32_11(bencher: &mut Bencher) { merge_sort::<u32>(bencher, 1 << 11); }
// fn msort_u32_12(bencher: &mut Bencher) { merge_sort::<u32>(bencher, 1 << 12); }
// fn msort_u32_13(bencher: &mut Bencher) { merge_sort::<u32>(bencher, 1 << 13); }
// fn msort_u32_14(bencher: &mut Bencher) { merge_sort::<u32>(bencher, 1 << 14); }
// fn msort_u32_15(bencher: &mut Bencher) { merge_sort::<u32>(bencher, 1 << 15); }
// fn msort_u32_16(bencher: &mut Bencher) { merge_sort::<u32>(bencher, 1 << 16); }
// fn msort_u32_17(bencher: &mut Bencher) { merge_sort::<u32>(bencher, 1 << 17); }
// fn msort_u32_18(bencher: &mut Bencher) { merge_sort::<u32>(bencher, 1 << 18); }
// fn msort_u32_19(bencher: &mut Bencher) { merge_sort::<u32>(bencher, 1 << 19); }
// fn msort_u32_20(bencher: &mut Bencher) { merge_sort::<u32>(bencher, 1 << 20); }
// fn msort_u32_21(bencher: &mut Bencher) { merge_sort::<u32>(bencher, 1<<21); }
// fn msort_u32_22(bencher: &mut Bencher) { merge_sort::<u32>(bencher, 1<<22); }
// fn msort_u32_23(bencher: &mut Bencher) { merge_sort::<u32>(bencher, 1<<23); }
// fn msort_u32_24(bencher: &mut Bencher) { merge_sort::<u32>(bencher, 1<<24); }
// fn msort_u32_25(bencher: &mut Bencher) { merge_sort::<u32>(bencher, 1<<25); }

fn radix_sort<T: Copy, U: Unsigned, F: Fn(&T)->U>(bencher: &mut Bencher, size: usize, function: &F)
    where Standard: Distribution<T>
{

    let seed = 1 << 24 + 2 << 16 + 3 << 8 + 4;
    let mut rng: StdRng = SeedableRng::seed_from_u64(seed);

    let mut vector = Vec::<T>::with_capacity(size);
    for _ in 0..size {
        vector.push(rng.gen());
    }
    let mut output = Vec::new();
    let mut sorter = LSBRadixSorter::new();
    bencher.bytes = (size * ::std::mem::size_of::<T>()) as u64;
    bencher.iter(|| {
        for &element in &vector {
            sorter.push(element, &function);
        }
        sorter.finish_into(&mut output, &function);
        sorter.recycle(&mut output);
    });
}


fn radix_sort_swc<T: Copy, U: Unsigned, F: Fn(&T)->U>(bencher: &mut Bencher, size: usize, function: &F)
    where Standard: Distribution<T>
{

    let seed = 1 << 24 + 2 << 16 + 3 << 8 + 4;
    let mut rng: StdRng = SeedableRng::seed_from_u64(seed);

    let mut vector = Vec::<T>::with_capacity(size);
    for _ in 0..size {
        vector.push(rng.gen());
    }
    let mut output = Vec::new();
    let mut sorter = LSBSWCRadixSorter::new();
    bencher.bytes = (size * ::std::mem::size_of::<T>()) as u64;
    bencher.iter(|| {
        for &element in &vector {
            sorter.push(element, &function);
        }
        sorter.finish_into(&mut output, &function);
        sorter.recycle(&mut output);
    });
}


fn radix_sort_msb<T: Ord+Copy, U: Unsigned, F: Fn(&T)->U>(bencher: &mut Bencher, size: usize, function: &F)
    where Standard: Distribution<T>
{

    let seed = 1 << 24 + 2 << 16 + 3 << 8 + 4;
    let mut rng: StdRng = SeedableRng::seed_from_u64(seed);

    let mut vector = Vec::<T>::with_capacity(size);
    for _ in 0..size {
        vector.push(rng.gen());
    }

    let mut output = Vec::new();
    let mut sorter = MSBRadixSorter::new();
    bencher.bytes = (size * ::std::mem::size_of::<T>()) as u64;
    bencher.iter(|| {
        for &element in &vector {
            sorter.push(element, &function);
        }
        sorter.finish_into(&mut output, &function);
        sorter.recycle(&mut output);
    });
}

fn radix_sort_msb_swc<T: Ord+Copy, U: Unsigned, F: Fn(&T)->U>(bencher: &mut Bencher, size: usize, function: &F)
    where Standard: Distribution<T>
{

    let seed = 1 << 24 + 2 << 16 + 3 << 8 + 4;
    let mut rng: StdRng = SeedableRng::seed_from_u64(seed);

    let mut vector = Vec::<T>::with_capacity(size);
    for _ in 0..size {
        vector.push(rng.gen());
    }

    let mut output = Vec::new();
    let mut sorter = MSBSWCRadixSorter::new();
    bencher.bytes = (size * ::std::mem::size_of::<T>()) as u64;
    bencher.iter(|| {
        for &element in &vector {
            sorter.push(element, &function);
        }
        sorter.finish_into(&mut output, &function);
        sorter.recycle(&mut output);
    });
}

#[allow(dead_code)]
fn merge_sort<T: Ord+Copy>(bencher: &mut Bencher, size: usize)
    where Standard: Distribution<T>
{

    let seed = 1 << 24 + 2 << 16 + 3 << 8 + 4;
    let mut rng: StdRng = SeedableRng::seed_from_u64(seed);

    let mut vector = Vec::<T>::with_capacity(size);
    for _ in 0..size {
        vector.push(rng.gen());
    }

    bencher.bytes = (vector.len() * ::std::mem::size_of::<T>()) as u64;
    bencher.iter(|| {
        let mut data = vector.clone();
        data.sort();
    });
}

#[allow(dead_code)]
fn pdq_sort<T: Ord+Copy>(bencher: &mut Bencher, size: usize)
    where Standard: Distribution<T>
{

    let seed = 1 << 24 + 2 << 16 + 3 << 8 + 4;
    let mut rng: StdRng = SeedableRng::seed_from_u64(seed);

    let mut vector = Vec::<T>::with_capacity(size);
    for _ in 0..size {
        vector.push(rng.gen());
    }

    bencher.bytes = (vector.len() * ::std::mem::size_of::<T>()) as u64;
    bencher.iter(|| {
        let mut data = vector.clone();
        data.sort_unstable();
    });
}

benchmark_group!(benches,
    rsort_lsb_u32_10,
    rsort_lsb_u32_11,
    rsort_lsb_u32_12,
    rsort_lsb_u32_13,
    rsort_lsb_u32_14,
    rsort_lsb_u32_15,
    rsort_lsb_u32_16,
    rsort_lsb_u32_17,
    rsort_lsb_u32_18,
    rsort_lsb_u32_19,
    rsort_lsb_u32_20,
    // rsort_lsb_u32_21,
    // rsort_lsb_u32_22,
    // rsort_lsb_u32_23,
    // rsort_lsb_u32_24,
    // rsort_lsb_u32_25,
    rsort_msb_u32_10,
    rsort_msb_u32_11,
    rsort_msb_u32_12,
    rsort_msb_u32_13,
    rsort_msb_u32_14,
    rsort_msb_u32_15,
    rsort_msb_u32_16,
    rsort_msb_u32_17,
    rsort_msb_u32_18,
    rsort_msb_u32_19,
    rsort_msb_u32_20,
    // rsort_msb_u32_21,
    // rsort_msb_u32_22,
    // rsort_msb_u32_23,
    // rsort_msb_u32_24,
    // rsort_msb_u32_25,
    // rsort_lsb_u32x2_20,
    // rsort_lsb_u32x2_21,
    // rsort_lsb_u32x2_22,
    // rsort_lsb_u32x2_23,
    // rsort_lsb_u32x2_24,
    // rsort_lsb_u32x2_25,
    rsort_lsb_swc_u32_10
,    rsort_lsb_swc_u32_11
,    rsort_lsb_swc_u32_12
,    rsort_lsb_swc_u32_13
,    rsort_lsb_swc_u32_14
,    rsort_lsb_swc_u32_15
,    rsort_lsb_swc_u32_16
,    rsort_lsb_swc_u32_17
,    rsort_lsb_swc_u32_18,
    rsort_lsb_swc_u32_19,
    rsort_lsb_swc_u32_20,
    // rsort_lsb_swc_u32_21,
    // rsort_lsb_swc_u32_22,
    // rsort_lsb_swc_u32_23,
    // rsort_lsb_swc_u32_24,
    // rsort_lsb_swc_u32_25,
    rsort_msb_swc_u32_10,
    rsort_msb_swc_u32_11,
    rsort_msb_swc_u32_12,
    rsort_msb_swc_u32_13,
    rsort_msb_swc_u32_14,
    rsort_msb_swc_u32_15,
    rsort_msb_swc_u32_16,
    rsort_msb_swc_u32_17,
    rsort_msb_swc_u32_18,
    rsort_msb_swc_u32_19,
    rsort_msb_swc_u32_20,
    // rsort_msb_u32_21,
    // rsort_msb_u32_22,
    // rsort_msb_u32_23,
    // rsort_msb_u32_24,
    // rsort_msb_u32_25,
    // pdq_u32_10,
    // pdq_u32_11,
    // pdq_u32_12,
    // pdq_u32_13,
    // pdq_u32_14,
    // pdq_u32_15,
    // pdq_u32_16,
    // pdq_u32_17,
    // pdq_u32_18,
    // pdq_u32_19,
    // pdq_u32_20,
    // pdq_u32_21,
    // pdq_u32_22,
    // pdq_u32_23,
    // pdq_u32_24,
    // pdq_u32_25,
    // msort_u32_10,
    // msort_u32_11,
    // msort_u32_12,
    // msort_u32_13,
    // msort_u32_14,
    // msort_u32_15,
    // msort_u32_16,
    // msort_u32_17,
    // msort_u32_18,
    // msort_u32_19,
    // msort_u32_20,
    // msort_u32_21,
    // msort_u32_22,
    // msort_u32_23,
    // msort_u32_24,
    // msort_u32_25,
);

benchmark_main!(benches);