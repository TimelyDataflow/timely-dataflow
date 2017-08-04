//! Types and traits for radix sorting.
//!
//! The current offerings are
//!
//! * LSBRadixSorter: A least-significant byte radix sorter.
//! * MSBRadixSorter: A most-significant byte radix sorter.
//! * LSBSWCRadixSorter: A least-significant byte radix sorter with software write combining.
//! 
//! There should probably be a `MSBSWCRadixSorter` in the future, because I like both of those things.

mod lsb;
mod lsb_swc;
mod msb;
mod msb_swc;
mod stash;
mod batched_vec;
mod swc_buffer;

pub use lsb::Sorter as LSBRadixSorter;
pub use lsb_swc::Sorter as LSBSWCRadixSorter;
pub use msb::Sorter as MSBRadixSorter;
pub use msb_swc::Sorter as MSBSWCRadixSorter;
pub use swc_buffer::SWCBuffer;

/// An unsigned integer fit for use as a radix key.
pub trait Unsigned : Ord {
    fn bytes() -> usize;
    fn as_u64(&self) -> u64;
}

impl Unsigned for  u8 { #[inline]fn bytes() -> usize { 1 } #[inline] fn as_u64(&self) -> u64 { *self as u64 } }
impl Unsigned for u16 { #[inline]fn bytes() -> usize { 2 } #[inline] fn as_u64(&self) -> u64 { *self as u64 } }
impl Unsigned for u32 { #[inline]fn bytes() -> usize { 4 } #[inline] fn as_u64(&self) -> u64 { *self as u64 } }
impl Unsigned for u64 { #[inline]fn bytes() -> usize { 8 } #[inline] fn as_u64(&self) -> u64 { *self as u64 } }
impl Unsigned for usize { #[inline]fn bytes() -> usize { ::std::mem::size_of::<usize>() } #[inline]fn as_u64(&self) -> u64 { *self as u64 } }

/// Functionality provided by a radix sorter.
///
/// These radix sorters allow you to specify a radix key function with each method call, so that 
/// the sorter does not need to reflect this (hard to name) type in its own type. This means you
/// need to take some care and not abuse this.
///
/// Internally the sorters manage data using blocks `Vec<T>`, and are delighted to consume data 
/// in block form (they will re-use the blocks), and will return results as a sequence of blocks
/// such that if they are traversed in-order they are appropriately sorted.
///
/// Most of the sorters manage a stash of buffers (ideally: 256) that they use when sorting the 
/// data. If this stash goes dry the sorters will allocate, but they much prefer to re-use buffers
/// whenever possible, and if having consulted your sorted results you would like to return the
/// buffers (using either `recycle` or `rebalance`) you should! The `rebalance` method allows you
/// to control just how many buffers the sorter is sitting on. 
pub trait RadixSorter<T, U: Unsigned> : RadixSorterBase<T> {

    /// Pushes a single element using the supplied radix key function.
    fn push<F: Fn(&T)->U>(&mut self, element: T, key: &F);
    /// Pushes a batch of elements using the supplied radix key function.
    fn push_batch<F: Fn(&T)->U>(&mut self, batch: Vec<T>, key: &F);
    /// Pushes a sequence of elements using the supplied radix key function.
    fn extend<F: Fn(&T)->U, I: Iterator<Item=T>>(&mut self, iterator: I, key: &F) {
    	for element in iterator {
    		self.push(element, key)
    	}
    }
    /// Completes the sorting session and returns the sorted results.
    fn finish<F: Fn(&T)->U>(&mut self, key: &F) -> Vec<Vec<T>> {
        let mut result = Vec::new();
        self.finish_into(&mut result, key);
        result    	
    }
    /// Completes the sorting session and puts the sorted results into `target`.
    fn finish_into<F: Fn(&T)->U>(&mut self, target: &mut Vec<Vec<T>>, key: &F);
    /// Sorts batched data using the supplied radix key function.
    fn sort<F: Fn(&T)->U>(&mut self, batches: &mut Vec<Vec<T>>, key: &F) {
        for batch in batches.drain(..) { 
            self.push_batch(batch, key); 
        }
        self.finish_into(batches, key);    	
    }
}

/// Functionality independent of the type `U` used to sort.
pub trait RadixSorterBase<T> {
	/// Allocates a new instance of the radix sorter.
    fn new() -> Self;
    /// Provides empty buffers for the radix sorter to use.
    fn recycle(&mut self, buffers: &mut Vec<Vec<T>>) {
        self.rebalance(buffers, usize::max_value());
	}
	/// Provides empty buffers for the radix sorter to use, with the intent that it should own at most `intended`.
    fn rebalance(&mut self, buffers: &mut Vec<Vec<T>>, intended: usize);	
}