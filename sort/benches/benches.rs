#![feature(test)]
extern crate test;
extern crate rand;
extern crate radix_sort as haeoua;

use rand::{Rng, SeedableRng, StdRng, Rand};
use test::Bencher;
use haeoua::*;

// #[bench] fn rsort_u16_16(bencher: &mut Bencher) { radix_sort(bencher, 0u32..(1<<16), &|&x| x); }

#[bench] fn rsort_u32_20(bencher: &mut Bencher) { radix_sort::<u32,_,_>(bencher, 1 << 20, &|&x| x); }
#[bench] fn rsort_u32_21(bencher: &mut Bencher) { radix_sort::<u32,_,_>(bencher, 1 << 21, &|&x| x); }
#[bench] fn rsort_u32_22(bencher: &mut Bencher) { radix_sort::<u32,_,_>(bencher, 1 << 22, &|&x| x); }
// #[bench] fn rsort_u32_23(bencher: &mut Bencher) { radix_sort::<u32,_,_>(bencher, 1 << 23, &|&x| x); }
// #[bench] fn rsort_u32_24(bencher: &mut Bencher) { radix_sort::<u32,_,_>(bencher, 1 << 24, &|&x| x); }
// #[bench] fn rsort_u32_25(bencher: &mut Bencher) { radix_sort::<u32,_,_>(bencher, 1 << 25, &|&x| x); }

// #[bench] fn rsort_u32x2_20(bencher: &mut Bencher) { radix_sort::<(u32, u32),_,_>(bencher, 1 << 20, &|&x| x.0); }
// #[bench] fn rsort_u32x2_21(bencher: &mut Bencher) { radix_sort::<(u32, u32),_,_>(bencher, 1 << 21, &|&x| x.0); }
// #[bench] fn rsort_u32x2_22(bencher: &mut Bencher) { radix_sort::<(u32, u32),_,_>(bencher, 1 << 22, &|&x| x.0); }
// #[bench] fn rsort_u32x2_23(bencher: &mut Bencher) { radix_sort::<(u32, u32),_,_>(bencher, 1 << 23, &|&x| x.0); }
// #[bench] fn rsort_u32x2_24(bencher: &mut Bencher) { radix_sort::<(u32, u32),_,_>(bencher, 1 << 24, &|&x| x.0); }
// #[bench] fn rsort_u32x2_25(bencher: &mut Bencher) { radix_sort::<(u32, u32),_,_>(bencher, 1 << 25, &|&x| x.0); }

#[bench] fn rsortswc_u32_20(bencher: &mut Bencher) { radix_sort_swc::<u32,_,_>(bencher, 1 << 20, &|&x| x); }
#[bench] fn rsortswc_u32_21(bencher: &mut Bencher) { radix_sort_swc::<u32,_,_>(bencher, 1 << 21, &|&x| x); }
#[bench] fn rsortswc_u32_22(bencher: &mut Bencher) { radix_sort_swc::<u32,_,_>(bencher, 1 << 22, &|&x| x); }
// #[bench] fn rsortswc_u32_23(bencher: &mut Bencher) { radix_sort_swc::<u32,_,_>(bencher, 1 << 23, &|&x| x); }
// #[bench] fn rsortswc_u32_24(bencher: &mut Bencher) { radix_sort_swc::<u32,_,_>(bencher, 1 << 24, &|&x| x); }
// #[bench] fn rsortswc_u32_25(bencher: &mut Bencher) { radix_sort_swc::<u32,_,_>(bencher, 1 << 25, &|&x| x); }

// #[bench] fn rsortswc_u32x2_20(bencher: &mut Bencher) { radix_sort_swc::<(u32, u32),_,_>(bencher, 1 << 20, &|&x| x.0); }
// #[bench] fn rsortswc_u32x2_21(bencher: &mut Bencher) { radix_sort_swc::<(u32, u32),_,_>(bencher, 1 << 21, &|&x| x.0); }
// #[bench] fn rsortswc_u32x2_22(bencher: &mut Bencher) { radix_sort_swc::<(u32, u32),_,_>(bencher, 1 << 22, &|&x| x.0); }
// #[bench] fn rsortswc_u32x2_23(bencher: &mut Bencher) { radix_sort_swc::<(u32, u32),_,_>(bencher, 1 << 23, &|&x| x.0); }
// #[bench] fn rsortswc_u32x2_24(bencher: &mut Bencher) { radix_sort_swc::<(u32, u32),_,_>(bencher, 1 << 24, &|&x| x.0); }
// #[bench] fn rsortswc_u32x2_25(bencher: &mut Bencher) { radix_sort_swc::<(u32, u32),_,_>(bencher, 1 << 25, &|&x| x.0); }

// #[bench] fn rsort_u64_20(bencher: &mut Bencher) { radix_sort(bencher, 0u64..(1<<20), &|&x| x); }
// #[bench] fn rsort_u64_21(bencher: &mut Bencher) { radix_sort(bencher, 0u64..(1<<21), &|&x| x); }
// #[bench] fn rsort_u64_22(bencher: &mut Bencher) { radix_sort(bencher, 0u64..(1<<22), &|&x| x); }
// #[bench] fn rsort_u64_23(bencher: &mut Bencher) { radix_sort(bencher, 0u64..(1<<23), &|&x| x); }

#[bench] fn msort_u32_20(bencher: &mut Bencher) { merge_sort(bencher, 0u32..(1<<20)); }
#[bench] fn msort_u32_21(bencher: &mut Bencher) { merge_sort(bencher, 0u32..(1<<21)); }
#[bench] fn msort_u32_22(bencher: &mut Bencher) { merge_sort(bencher, 0u32..(1<<22)); }
// #[bench] fn msort_u64_23(bencher: &mut Bencher) { merge_sort(bencher, 0u32..(1<<23)); }
// #[bench] fn msort_u64_24(bencher: &mut Bencher) { merge_sort(bencher, 0u32..(1<<24)); }
// #[bench] fn msort_u64_25(bencher: &mut Bencher) { merge_sort(bencher, 0u32..(1<<25)); }

fn radix_sort<T: Copy+Rand, U: Unsigned, F: Fn(&T)->U>(bencher: &mut Bencher, size: usize, function: &F) {

    let seed: &[_] = &[1, 2, 3, 4];
    let mut rng: StdRng = SeedableRng::from_seed(seed);

    let mut vector = Vec::<T>::with_capacity(size);
    for _ in 0..size {
        vector.push(<T as Rand>::rand(&mut rng));
    }

    let mut sorter = LSBRadixSorter::new();
    bencher.bytes = (size * ::std::mem::size_of::<T>()) as u64;
    bencher.iter(|| {
        for &element in &vector {
            sorter.push(element, &function);
        }
        let output = sorter.finish(&function);
        sorter.recycle(output);
    });
}


fn radix_sort_swc<T: Copy+Rand, U: Unsigned, F: Fn(&T)->U>(bencher: &mut Bencher, size: usize, function: &F) {

    let seed: &[_] = &[1, 2, 3, 4];
    let mut rng: StdRng = SeedableRng::from_seed(seed);

    let mut vector = Vec::<T>::with_capacity(size);
    for _ in 0..size {
        vector.push(<T as Rand>::rand(&mut rng));
    }

    let mut sorter = LSBSWCRadixSorter::new();
    bencher.bytes = (size * ::std::mem::size_of::<T>()) as u64;
    bencher.iter(|| {
        for &element in &vector {
            sorter.push(element, &function);
        }
        let output = sorter.finish(&function);
        sorter.recycle(output);
    });
}

fn merge_sort<I: Iterator+Clone>(bencher: &mut Bencher, data: I) where I::Item: Clone+Ord {
    let mut vector = data.clone().collect::<Vec<_>>();
    bencher.bytes = (vector.len() * ::std::mem::size_of::<I::Item>()) as u64;
    bencher.iter(|| {
        // let mut data = vector.clone();
        vector.sort();
    });
}




// #[bench] fn rsortmsb_u32_20(bencher: &mut Bencher) { radix_sort_msb::<u32,_,_>(bencher, 1 << 20, &|&x| x); }
// #[bench] fn rsortmsb_u32_21(bencher: &mut Bencher) { radix_sort_msb::<u32,_,_>(bencher, 1 << 21, &|&x| x); }
// #[bench] fn rsortmsb_u32_22(bencher: &mut Bencher) { radix_sort_msb::<u32,_,_>(bencher, 1 << 22, &|&x| x); }
// #[bench] fn rsortmsb_u32_23(bencher: &mut Bencher) { radix_sort_msb::<u32,_,_>(bencher, 1 << 23, &|&x| x); }
// #[bench] fn rsortmsb_u32_24(bencher: &mut Bencher) { radix_sort_msb::<u32,_,_>(bencher, 1 << 24, &|&x| x); }
// #[bench] fn rsortmsb_u32_25(bencher: &mut Bencher) { radix_sort_msb::<u32,_,_>(bencher, 1 << 25, &|&x| x); }


fn radix_sort_msb<T: Copy+Rand, U: Unsigned, F: Fn(&T)->U>(bencher: &mut Bencher, size: usize, function: &F) {

    let seed: &[_] = &[1, 2, 3, 4];
    let mut rng: StdRng = SeedableRng::from_seed(seed);

    let mut vector = Vec::<T>::with_capacity(size);
    for _ in 0..size {
        vector.push(<T as Rand>::rand(&mut rng));
    }

    bencher.bytes = (size * ::std::mem::size_of::<T>()) as u64;
    bencher.iter(|| {
        let mut tmp = vector.clone();
        top_down(&mut tmp, function);
    });
}


pub fn top_down<T, U: Unsigned, F: Fn(&T)->U>(mut input: &mut [T], function: &F) {

    let mut upper = [0u32; 256];
    let mut lower = [0u32; 256];

    let mut offset = 0;
    while offset < input.len() {

        let split_pos = ::std::cmp::min(input.len(), offset + (16 * 4096 / ::std::mem::size_of::<T>()));

        let slice = &mut input[offset..split_pos];
        offset = split_pos;

        // count number of elts with each radix
        for i in 0..upper.len() { upper[i] = 0; }
        for elem in slice.iter() { unsafe { *upper.get_unchecked_mut(((function(elem).as_u64()) & 0xFF) as usize) += 1; } }
        lower[0] = 0; for i in 1..lower.len() { lower[i] = upper[i-1]; upper[i] += lower[i]; }

        for i in 0..256 {
            while lower[i] < upper[i] {
                unsafe {
                for j in *lower.get_unchecked(i)..*upper.get_unchecked(i) {
                    let dst = ((function(slice.get_unchecked_mut(j as usize)).as_u64()) & 0xFF) as usize;
                    let pa: *mut T = slice.get_unchecked_mut(j as usize);
                    let pb: *mut T = slice.get_unchecked_mut(*lower.get_unchecked(dst) as usize);
                    ::std::ptr::swap(pa, pb);
                    *lower.get_unchecked_mut(dst) += 1;
                    }
                }
                // let dst = ((function(unsafe { slice.get_unchecked_mut(*lower.get_unchecked(i) as usize) }).as_u64()) & 0xFF) as usize;
                // unsafe {
                //     let pa: *mut T = slice.get_unchecked_mut(*lower.get_unchecked(i) as usize);
                //     let pb: *mut T = slice.get_unchecked_mut(*lower.get_unchecked(dst) as usize);
                //     ::std::ptr::swap(pa, pb);
                //     *lower.get_unchecked_mut(dst) += 1;
                // }
            }
        }
    }
}
