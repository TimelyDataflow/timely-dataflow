#![feature(test)]
#![feature(sort_unstable)]
extern crate rand;
extern crate test;
extern crate timely_sort as haeoua;

use rand::{SeedableRng, distributions::Distribution, distributions::Standard, prng::Hc128Rng};
use test::Bencher;
use haeoua::*;

use haeoua::{RadixSorter, RadixSorterBase};

#[bench] fn rsort_lsb_u32_10(bencher: &mut Bencher) { radix_sort::<u32,_,_>(bencher, 1 << 10, &|&x| x); }
#[bench] fn rsort_lsb_u32_11(bencher: &mut Bencher) { radix_sort::<u32,_,_>(bencher, 1 << 11, &|&x| x); }
#[bench] fn rsort_lsb_u32_12(bencher: &mut Bencher) { radix_sort::<u32,_,_>(bencher, 1 << 12, &|&x| x); }
#[bench] fn rsort_lsb_u32_13(bencher: &mut Bencher) { radix_sort::<u32,_,_>(bencher, 1 << 13, &|&x| x); }
#[bench] fn rsort_lsb_u32_14(bencher: &mut Bencher) { radix_sort::<u32,_,_>(bencher, 1 << 14, &|&x| x); }
#[bench] fn rsort_lsb_u32_15(bencher: &mut Bencher) { radix_sort::<u32,_,_>(bencher, 1 << 15, &|&x| x); }
#[bench] fn rsort_lsb_u32_16(bencher: &mut Bencher) { radix_sort::<u32,_,_>(bencher, 1 << 16, &|&x| x); }
#[bench] fn rsort_lsb_u32_17(bencher: &mut Bencher) { radix_sort::<u32,_,_>(bencher, 1 << 17, &|&x| x); }
#[bench] fn rsort_lsb_u32_18(bencher: &mut Bencher) { radix_sort::<u32,_,_>(bencher, 1 << 18, &|&x| x); }
#[bench] fn rsort_lsb_u32_19(bencher: &mut Bencher) { radix_sort::<u32,_,_>(bencher, 1 << 19, &|&x| x); }
#[bench] fn rsort_lsb_u32_20(bencher: &mut Bencher) { radix_sort::<u32,_,_>(bencher, 1 << 20, &|&x| x); }
// #[bench] fn rsort_lsb_u32_21(bencher: &mut Bencher) { radix_sort::<u32,_,_>(bencher, 1 << 21, &|&x| x); }
// #[bench] fn rsort_lsb_u32_22(bencher: &mut Bencher) { radix_sort::<u32,_,_>(bencher, 1 << 22, &|&x| x); }
// #[bench] fn rsort_lsb_u32_23(bencher: &mut Bencher) { radix_sort::<u32,_,_>(bencher, 1 << 23, &|&x| x); }
// #[bench] fn rsort_lsb_u32_24(bencher: &mut Bencher) { radix_sort::<u32,_,_>(bencher, 1 << 24, &|&x| x); }
// #[bench] fn rsort_lsb_u32_25(bencher: &mut Bencher) { radix_sort::<u32,_,_>(bencher, 1 << 25, &|&x| x); }

#[bench] fn rsort_msb_u32_10(bencher: &mut Bencher) { radix_sort_msb::<u32,_,_>(bencher, 1 << 10, &|&x| x); }
#[bench] fn rsort_msb_u32_11(bencher: &mut Bencher) { radix_sort_msb::<u32,_,_>(bencher, 1 << 11, &|&x| x); }
#[bench] fn rsort_msb_u32_12(bencher: &mut Bencher) { radix_sort_msb::<u32,_,_>(bencher, 1 << 12, &|&x| x); }
#[bench] fn rsort_msb_u32_13(bencher: &mut Bencher) { radix_sort_msb::<u32,_,_>(bencher, 1 << 13, &|&x| x); }
#[bench] fn rsort_msb_u32_14(bencher: &mut Bencher) { radix_sort_msb::<u32,_,_>(bencher, 1 << 14, &|&x| x); }
#[bench] fn rsort_msb_u32_15(bencher: &mut Bencher) { radix_sort_msb::<u32,_,_>(bencher, 1 << 15, &|&x| x); }
#[bench] fn rsort_msb_u32_16(bencher: &mut Bencher) { radix_sort_msb::<u32,_,_>(bencher, 1 << 16, &|&x| x); }
#[bench] fn rsort_msb_u32_17(bencher: &mut Bencher) { radix_sort_msb::<u32,_,_>(bencher, 1 << 17, &|&x| x); }
#[bench] fn rsort_msb_u32_18(bencher: &mut Bencher) { radix_sort_msb::<u32,_,_>(bencher, 1 << 18, &|&x| x); }
#[bench] fn rsort_msb_u32_19(bencher: &mut Bencher) { radix_sort_msb::<u32,_,_>(bencher, 1 << 19, &|&x| x); }
#[bench] fn rsort_msb_u32_20(bencher: &mut Bencher) { radix_sort_msb::<u32,_,_>(bencher, 1 << 20, &|&x| x); }
// #[bench] fn rsort_msb_u32_21(bencher: &mut Bencher) { radix_sort_msb::<u32,_,_>(bencher, 1 << 21, &|&x| x); }
// #[bench] fn rsort_msb_u32_22(bencher: &mut Bencher) { radix_sort_msb::<u32,_,_>(bencher, 1 << 22, &|&x| x); }
// #[bench] fn rsort_msb_u32_23(bencher: &mut Bencher) { radix_sort_msb::<u32,_,_>(bencher, 1 << 23, &|&x| x); }
// #[bench] fn rsort_msb_u32_24(bencher: &mut Bencher) { radix_sort_msb::<u32,_,_>(bencher, 1 << 24, &|&x| x); }
// #[bench] fn rsort_msb_u32_25(bencher: &mut Bencher) { radix_sort_msb::<u32,_,_>(bencher, 1 << 25, &|&x| x); }

// #[bench] fn rsort_lsb_u32x2_20(bencher: &mut Bencher) { radix_sort::<(u32, u32),_,_>(bencher, 1 << 20, &|&x| x.0); }
// #[bench] fn rsort_lsb_u32x2_21(bencher: &mut Bencher) { radix_sort::<(u32, u32),_,_>(bencher, 1 << 21, &|&x| x.0); }
// #[bench] fn rsort_lsb_u32x2_22(bencher: &mut Bencher) { radix_sort::<(u32, u32),_,_>(bencher, 1 << 22, &|&x| x.0); }
// #[bench] fn rsort_lsb_u32x2_23(bencher: &mut Bencher) { radix_sort::<(u32, u32),_,_>(bencher, 1 << 23, &|&x| x.0); }
// #[bench] fn rsort_lsb_u32x2_24(bencher: &mut Bencher) { radix_sort::<(u32, u32),_,_>(bencher, 1 << 24, &|&x| x.0); }
// #[bench] fn rsort_lsb_u32x2_25(bencher: &mut Bencher) { radix_sort::<(u32, u32),_,_>(bencher, 1 << 25, &|&x| x.0); }

#[bench] fn rsort_lsb_swc_u32_10(bencher: &mut Bencher) { radix_sort_swc::<u32,_,_>(bencher, 1 << 10, &|&x| x); }
#[bench] fn rsort_lsb_swc_u32_11(bencher: &mut Bencher) { radix_sort_swc::<u32,_,_>(bencher, 1 << 11, &|&x| x); }
#[bench] fn rsort_lsb_swc_u32_12(bencher: &mut Bencher) { radix_sort_swc::<u32,_,_>(bencher, 1 << 12, &|&x| x); }
#[bench] fn rsort_lsb_swc_u32_13(bencher: &mut Bencher) { radix_sort_swc::<u32,_,_>(bencher, 1 << 13, &|&x| x); }
#[bench] fn rsort_lsb_swc_u32_14(bencher: &mut Bencher) { radix_sort_swc::<u32,_,_>(bencher, 1 << 14, &|&x| x); }
#[bench] fn rsort_lsb_swc_u32_15(bencher: &mut Bencher) { radix_sort_swc::<u32,_,_>(bencher, 1 << 15, &|&x| x); }
#[bench] fn rsort_lsb_swc_u32_16(bencher: &mut Bencher) { radix_sort_swc::<u32,_,_>(bencher, 1 << 16, &|&x| x); }
#[bench] fn rsort_lsb_swc_u32_17(bencher: &mut Bencher) { radix_sort_swc::<u32,_,_>(bencher, 1 << 17, &|&x| x); }
#[bench] fn rsort_lsb_swc_u32_18(bencher: &mut Bencher) { radix_sort_swc::<u32,_,_>(bencher, 1 << 18, &|&x| x); }
#[bench] fn rsort_lsb_swc_u32_19(bencher: &mut Bencher) { radix_sort_swc::<u32,_,_>(bencher, 1 << 19, &|&x| x); }
#[bench] fn rsort_lsb_swc_u32_20(bencher: &mut Bencher) { radix_sort_swc::<u32,_,_>(bencher, 1 << 20, &|&x| x); }
// #[bench] fn rsort_lsb_swc_u32_21(bencher: &mut Bencher) { radix_sort_swc::<u32,_,_>(bencher, 1 << 21, &|&x| x); }
// #[bench] fn rsort_lsb_swc_u32_22(bencher: &mut Bencher) { radix_sort_swc::<u32,_,_>(bencher, 1 << 22, &|&x| x); }
// #[bench] fn rsort_lsb_swc_u32_23(bencher: &mut Bencher) { radix_sort_swc::<u32,_,_>(bencher, 1 << 23, &|&x| x); }
// #[bench] fn rsort_lsb_swc_u32_24(bencher: &mut Bencher) { radix_sort_swc::<u32,_,_>(bencher, 1 << 24, &|&x| x); }
// #[bench] fn rsort_lsb_swc_u32_25(bencher: &mut Bencher) { radix_sort_swc::<u32,_,_>(bencher, 1 << 25, &|&x| x); }

#[bench] fn rsort_msb_swc_u32_10(bencher: &mut Bencher) { radix_sort_msb_swc::<u32,_,_>(bencher, 1 << 10, &|&x| x); }
#[bench] fn rsort_msb_swc_u32_11(bencher: &mut Bencher) { radix_sort_msb_swc::<u32,_,_>(bencher, 1 << 11, &|&x| x); }
#[bench] fn rsort_msb_swc_u32_12(bencher: &mut Bencher) { radix_sort_msb_swc::<u32,_,_>(bencher, 1 << 12, &|&x| x); }
#[bench] fn rsort_msb_swc_u32_13(bencher: &mut Bencher) { radix_sort_msb_swc::<u32,_,_>(bencher, 1 << 13, &|&x| x); }
#[bench] fn rsort_msb_swc_u32_14(bencher: &mut Bencher) { radix_sort_msb_swc::<u32,_,_>(bencher, 1 << 14, &|&x| x); }
#[bench] fn rsort_msb_swc_u32_15(bencher: &mut Bencher) { radix_sort_msb_swc::<u32,_,_>(bencher, 1 << 15, &|&x| x); }
#[bench] fn rsort_msb_swc_u32_16(bencher: &mut Bencher) { radix_sort_msb_swc::<u32,_,_>(bencher, 1 << 16, &|&x| x); }
#[bench] fn rsort_msb_swc_u32_17(bencher: &mut Bencher) { radix_sort_msb_swc::<u32,_,_>(bencher, 1 << 17, &|&x| x); }
#[bench] fn rsort_msb_swc_u32_18(bencher: &mut Bencher) { radix_sort_msb_swc::<u32,_,_>(bencher, 1 << 18, &|&x| x); }
#[bench] fn rsort_msb_swc_u32_19(bencher: &mut Bencher) { radix_sort_msb_swc::<u32,_,_>(bencher, 1 << 19, &|&x| x); }
#[bench] fn rsort_msb_swc_u32_20(bencher: &mut Bencher) { radix_sort_msb_swc::<u32,_,_>(bencher, 1 << 20, &|&x| x); }
// #[bench] fn rsort_msb_u32_21(bencher: &mut Bencher) { radix_sort_msb_swc::<u32,_,_>(bencher, 1 << 21, &|&x| x); }
// #[bench] fn rsort_msb_u32_22(bencher: &mut Bencher) { radix_sort_msb_swc::<u32,_,_>(bencher, 1 << 22, &|&x| x); }
// #[bench] fn rsort_msb_u32_23(bencher: &mut Bencher) { radix_sort_msb_swc::<u32,_,_>(bencher, 1 << 23, &|&x| x); }
// #[bench] fn rsort_msb_u32_24(bencher: &mut Bencher) { radix_sort_msb_swc::<u32,_,_>(bencher, 1 << 24, &|&x| x); }
// #[bench] fn rsort_msb_u32_25(bencher: &mut Bencher) { radix_sort_msb_swc::<u32,_,_>(bencher, 1 << 25, &|&x| x); }

// #[bench] fn pdq_u32_10(bencher: &mut Bencher) { pdq_sort::<u32>(bencher, 1 << 10); }
// #[bench] fn pdq_u32_11(bencher: &mut Bencher) { pdq_sort::<u32>(bencher, 1 << 11); }
// #[bench] fn pdq_u32_12(bencher: &mut Bencher) { pdq_sort::<u32>(bencher, 1 << 12); }
// #[bench] fn pdq_u32_13(bencher: &mut Bencher) { pdq_sort::<u32>(bencher, 1 << 13); }
// #[bench] fn pdq_u32_14(bencher: &mut Bencher) { pdq_sort::<u32>(bencher, 1 << 14); }
// #[bench] fn pdq_u32_15(bencher: &mut Bencher) { pdq_sort::<u32>(bencher, 1 << 15); }
// #[bench] fn pdq_u32_16(bencher: &mut Bencher) { pdq_sort::<u32>(bencher, 1 << 16); }
// #[bench] fn pdq_u32_17(bencher: &mut Bencher) { pdq_sort::<u32>(bencher, 1 << 17); }
// #[bench] fn pdq_u32_18(bencher: &mut Bencher) { pdq_sort::<u32>(bencher, 1 << 18); }
// #[bench] fn pdq_u32_19(bencher: &mut Bencher) { pdq_sort::<u32>(bencher, 1 << 19); }
// #[bench] fn pdq_u32_20(bencher: &mut Bencher) { pdq_sort::<u32>(bencher, 1 << 20); }
// #[bench] fn pdq_u32_21(bencher: &mut Bencher) { pdq_sort::<u32>(bencher, 1<<21); }
// #[bench] fn pdq_u32_22(bencher: &mut Bencher) { pdq_sort::<u32>(bencher, 1<<22); }
// #[bench] fn pdq_u32_23(bencher: &mut Bencher) { pdq_sort::<u32>(bencher, 1<<23); }
// #[bench] fn pdq_u32_24(bencher: &mut Bencher) { pdq_sort::<u32>(bencher, 1<<24); }
// #[bench] fn pdq_u32_25(bencher: &mut Bencher) { pdq_sort::<u32>(bencher, 1<<25); }

// #[bench] fn msort_u32_10(bencher: &mut Bencher) { merge_sort::<u32>(bencher, 1 << 10); }
// #[bench] fn msort_u32_11(bencher: &mut Bencher) { merge_sort::<u32>(bencher, 1 << 11); }
// #[bench] fn msort_u32_12(bencher: &mut Bencher) { merge_sort::<u32>(bencher, 1 << 12); }
// #[bench] fn msort_u32_13(bencher: &mut Bencher) { merge_sort::<u32>(bencher, 1 << 13); }
// #[bench] fn msort_u32_14(bencher: &mut Bencher) { merge_sort::<u32>(bencher, 1 << 14); }
// #[bench] fn msort_u32_15(bencher: &mut Bencher) { merge_sort::<u32>(bencher, 1 << 15); }
// #[bench] fn msort_u32_16(bencher: &mut Bencher) { merge_sort::<u32>(bencher, 1 << 16); }
// #[bench] fn msort_u32_17(bencher: &mut Bencher) { merge_sort::<u32>(bencher, 1 << 17); }
// #[bench] fn msort_u32_18(bencher: &mut Bencher) { merge_sort::<u32>(bencher, 1 << 18); }
// #[bench] fn msort_u32_19(bencher: &mut Bencher) { merge_sort::<u32>(bencher, 1 << 19); }
// #[bench] fn msort_u32_20(bencher: &mut Bencher) { merge_sort::<u32>(bencher, 1 << 20); }
// #[bench] fn msort_u32_21(bencher: &mut Bencher) { merge_sort::<u32>(bencher, 1<<21); }
// #[bench] fn msort_u32_22(bencher: &mut Bencher) { merge_sort::<u32>(bencher, 1<<22); }
// #[bench] fn msort_u32_23(bencher: &mut Bencher) { merge_sort::<u32>(bencher, 1<<23); }
// #[bench] fn msort_u32_24(bencher: &mut Bencher) { merge_sort::<u32>(bencher, 1<<24); }
// #[bench] fn msort_u32_25(bencher: &mut Bencher) { merge_sort::<u32>(bencher, 1<<25); }

fn radix_sort<T, U, F>(bencher: &mut Bencher, size: usize, function: &F)
  where
    T: Copy,
    U: Unsigned,
    F: Fn(&T)->U,
    Standard: Distribution<T>,
{

    let seed = [3u8; 32];
    let mut rng: Hc128Rng = SeedableRng::from_seed(seed);
    let vector: Vec<T> = Standard.sample_iter(&mut rng).take(size).collect();

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


fn radix_sort_swc<T,U, F>(bencher: &mut Bencher, size: usize, function: &F)
  where
    T: Copy,
    U: Unsigned,
    F: Fn(&T)->U,
    Standard: Distribution<T>,
{

    let seed = [3u8; 32];
    let mut rng: Hc128Rng = SeedableRng::from_seed(seed);
    let vector: Vec<T> = Standard.sample_iter(&mut rng).take(size).collect();

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


fn radix_sort_msb<T, U, F>(bencher: &mut Bencher, size: usize, function: &F)
  where
    T: Copy,
    U: Unsigned,
    F: Fn(&T)->U,
    Standard: Distribution<T>,
{

    let seed = [3u8; 32];
    let mut rng: Hc128Rng = SeedableRng::from_seed(seed);
    let vector: Vec<T> = Standard.sample_iter(&mut rng).take(size).collect();

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

fn radix_sort_msb_swc<T, U, F>(bencher: &mut Bencher, size: usize, function: &F)
  where
    T: Copy,
    U: Unsigned,
    F: Fn(&T)->U,
    Standard: Distribution<T>,
{

    let seed = [3u8; 32];
    let mut rng: Hc128Rng = SeedableRng::from_seed(seed);
    let vector: Vec<T> = Standard.sample_iter(&mut rng).take(size).collect();

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

fn merge_sort<T>(bencher: &mut Bencher, size: usize)
  where
    T: Ord+Copy,
    Standard: Distribution<T>,
{

    let seed = [3u8; 32];
    let mut rng: Hc128Rng = SeedableRng::from_seed(seed);
    let vector: Vec<T> = Standard.sample_iter(&mut rng).take(size).collect();

    bencher.bytes = (vector.len() * ::std::mem::size_of::<T>()) as u64;
    bencher.iter(|| {
        let mut data = vector.clone();
        data.sort();
    });
}

fn pdq_sort<T>(bencher: &mut Bencher, size: usize)
  where
    T: Ord+Copy,
    Standard: Distribution<T>,
{

    let seed = [3u8; 32];
    let mut rng: Hc128Rng = SeedableRng::from_seed(seed);
    let vector: Vec<T> = Standard.sample_iter(&mut rng).take(size).collect();

    bencher.bytes = (vector.len() * ::std::mem::size_of::<T>()) as u64;
    bencher.iter(|| {
        let mut data = vector.clone();
        data.sort_unstable();
    });
}
