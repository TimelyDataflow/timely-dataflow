#![feature(test)]
extern crate test;
extern crate rand;
extern crate timely_sort as haeoua;

use rand::{SeedableRng, StdRng, Rand};
use test::Bencher;
use haeoua::*;

#[bench] fn rsort_u32_20(bencher: &mut Bencher) { radix_sort::<u32,_,_>(bencher, 1 << 20, &|&x| x); }
#[bench] fn rsort_u32_21(bencher: &mut Bencher) { radix_sort::<u32,_,_>(bencher, 1 << 21, &|&x| x); }
#[bench] fn rsort_u32_22(bencher: &mut Bencher) { radix_sort::<u32,_,_>(bencher, 1 << 22, &|&x| x); }
#[bench] fn rsort_u32_23(bencher: &mut Bencher) { radix_sort::<u32,_,_>(bencher, 1 << 23, &|&x| x); }
#[bench] fn rsort_u32_24(bencher: &mut Bencher) { radix_sort::<u32,_,_>(bencher, 1 << 24, &|&x| x); }
#[bench] fn rsort_u32_25(bencher: &mut Bencher) { radix_sort::<u32,_,_>(bencher, 1 << 25, &|&x| x); }

// #[bench] fn rsort_u32x2_20(bencher: &mut Bencher) { radix_sort::<(u32, u32),_,_>(bencher, 1 << 20, &|&x| x.0); }
// #[bench] fn rsort_u32x2_21(bencher: &mut Bencher) { radix_sort::<(u32, u32),_,_>(bencher, 1 << 21, &|&x| x.0); }
// #[bench] fn rsort_u32x2_22(bencher: &mut Bencher) { radix_sort::<(u32, u32),_,_>(bencher, 1 << 22, &|&x| x.0); }
// #[bench] fn rsort_u32x2_23(bencher: &mut Bencher) { radix_sort::<(u32, u32),_,_>(bencher, 1 << 23, &|&x| x.0); }
// #[bench] fn rsort_u32x2_24(bencher: &mut Bencher) { radix_sort::<(u32, u32),_,_>(bencher, 1 << 24, &|&x| x.0); }
// #[bench] fn rsort_u32x2_25(bencher: &mut Bencher) { radix_sort::<(u32, u32),_,_>(bencher, 1 << 25, &|&x| x.0); }

#[bench] fn rsortswc_u32_20(bencher: &mut Bencher) { radix_sort_swc::<u32,_,_>(bencher, 1 << 20, &|&x| x); }
#[bench] fn rsortswc_u32_21(bencher: &mut Bencher) { radix_sort_swc::<u32,_,_>(bencher, 1 << 21, &|&x| x); }
#[bench] fn rsortswc_u32_22(bencher: &mut Bencher) { radix_sort_swc::<u32,_,_>(bencher, 1 << 22, &|&x| x); }
#[bench] fn rsortswc_u32_23(bencher: &mut Bencher) { radix_sort_swc::<u32,_,_>(bencher, 1 << 23, &|&x| x); }
#[bench] fn rsortswc_u32_24(bencher: &mut Bencher) { radix_sort_swc::<u32,_,_>(bencher, 1 << 24, &|&x| x); }
#[bench] fn rsortswc_u32_25(bencher: &mut Bencher) { radix_sort_swc::<u32,_,_>(bencher, 1 << 25, &|&x| x); }

#[bench] fn msort_u32_20(bencher: &mut Bencher) { merge_sort(bencher, 0u32..(1<<20)); }
#[bench] fn msort_u32_21(bencher: &mut Bencher) { merge_sort(bencher, 0u32..(1<<21)); }
#[bench] fn msort_u32_22(bencher: &mut Bencher) { merge_sort(bencher, 0u32..(1<<22)); }
#[bench] fn msort_u32_23(bencher: &mut Bencher) { merge_sort(bencher, 0u32..(1<<23)); }
#[bench] fn msort_u32_24(bencher: &mut Bencher) { merge_sort(bencher, 0u32..(1<<24)); }
#[bench] fn msort_u32_25(bencher: &mut Bencher) { merge_sort(bencher, 0u32..(1<<25)); }

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
    let vector = data.clone().collect::<Vec<_>>();
    bencher.bytes = (vector.len() * ::std::mem::size_of::<I::Item>()) as u64;
    bencher.iter(|| {
        let mut data = vector.clone();
        data.sort();
    });
}