extern crate rand;
extern crate timely_sort as haeoua;

use rand::{SeedableRng, distributions::Distribution, distributions::Standard, prng::Hc128Rng};
use haeoua::*;

fn main() {

    if ::std::env::args().len() == 4 {
        let size: usize = ::std::env::args().nth(1).unwrap().parse().unwrap();
        let iters: usize = ::std::env::args().nth(2).unwrap().parse().unwrap();
        let threads: usize = ::std::env::args().nth(3).unwrap().parse().unwrap();

        println!("merge_sort:\t{:?}s", test_threads(threads, move || test_sort_by::<u64>(size, iters)));
        println!("pdq_sort:\t{:?}s", test_threads(threads, move || test_sort_unstable_by::<u64>(size, iters)));
        println!("lsb_sort:\t{:?}s", test_threads(threads, move || test_radix::<u64, LSBRadixSorter<_>>(size, iters)));
        println!("lsb_swc_sort:\t{:?}s", test_threads(threads, move || test_radix::<u64, LSBSWCRadixSorter<_>>(size, iters)));
        println!("msb_sort:\t{:?}s", test_threads(threads, move || test_radix::<u64, MSBRadixSorter<_>>(size, iters)));
        println!("msb_swc_sort:\t{:?}s", test_threads(threads, move || test_radix::<u64, MSBSWCRadixSorter<_>>(size, iters)));
    }
    else {
        println!("usage: profile <size> <iterations> <threads>");
    }
}

fn test_threads<L: Fn()+Send+Sync+'static>(threads: usize, logic: L) -> f64 {

    let timer = ::std::time::Instant::now();

    let logic = ::std::sync::Arc::new(logic);
    let mut handles = Vec::new();
    for _thread in 0 .. threads {
        let logic = logic.clone();
        handles.push(::std::thread::spawn(move || (*logic)()));
    }
    for handle in handles {
        handle.join().unwrap();
    }

    let elapsed = timer.elapsed();
    elapsed.as_secs() as f64 + (elapsed.subsec_nanos() as f64) / 1000000000.0
}

fn test_radix<T, R>(size: usize, iters: usize)
  where
    T: Ord+Copy+Unsigned,
    R: RadixSorter<(T,T),T>,
    Standard: Distribution<T>,
{

    let seed = [3u8; 32];
    let mut rng: Hc128Rng = SeedableRng::from_seed(seed);

    let mut sorter = R::new();
    for _ in 0..size {
        let tup: (T, T) = (Standard.sample(&mut rng), Standard.sample(&mut rng));
        sorter.push(tup, &|x| x.0);
    }
    let mut vector = sorter.finish(&|x| x.0);
    sorter.sort(&mut vector, &|x| x.1);

    for _ in 0 .. (iters - 1) {
        sorter.sort(&mut vector, &|x| x.0);
        sorter.sort(&mut vector, &|x| x.1);
    }
}

fn test_sort_by<T>(size: usize, iters: usize)
  where
    T: Ord,
    Standard: Distribution<T>,
{

    let seed = [3u8; 32];
    let mut rng: Hc128Rng = SeedableRng::from_seed(seed);

    let mut vector = Vec::<(T, T)>::with_capacity(size);
    for _ in 0..size {
        let tup: (T, T) = (Standard.sample(&mut rng), Standard.sample(&mut rng));
        vector.push(tup);
    }

    for _ in 0 .. iters {
        vector.sort_by(|x,y| x.0.cmp(&y.0));
        vector.sort_by(|x,y| x.1.cmp(&y.1));
    }
}

fn test_sort_unstable_by<T>(size: usize, iters: usize)
  where
    T: Ord,
    Standard: Distribution<T>,
{

    let seed = [3u8; 32];
    let mut rng: Hc128Rng = SeedableRng::from_seed(seed);

    let mut vector = Vec::<(T, T)>::with_capacity(size);
    for _ in 0..size {
        let tup: (T, T) = (Standard.sample(&mut rng), Standard.sample(&mut rng));
        vector.push(tup);
    }

    for _ in 0 .. iters {
        vector.sort_unstable_by(|x,y| x.0.cmp(&y.0));
        vector.sort_unstable_by(|x,y| x.1.cmp(&y.1));
    }
}
