//! A partially ordered measure of progress at each timely dataflow location.

use std::fmt::Debug;
use std::any::Any;
use std::default::Default;
use std::fmt::Formatter;
use std::fmt::Error;

use progress::nested::product::Product;

use abomonation::Abomonation;

// TODO : Change Copy requirement to Clone;
/// A composite trait for types that serve as timestamps in timely dataflow.
pub trait Timestamp: Copy+Eq+PartialOrd+Default+Debug+Send+Any+Abomonation {
    /// A type summarizing action on a timestamp along a dataflow path.
    type Summary : PathSummary<Self> + 'static;
}

// TODO : Change Copy requirement to Clone
// TODO : Change `results_in` and perhaps `followed_by` to return an `Option`, indicating no path.
// TODO : This can be important when a summary would "overflow", as we want neither to overflow,
// TODO : nor wrap around, nor saturate.
/// A summary of how a timestamp advances along a timely dataflow path.
pub trait PathSummary<T> : 'static+Copy+Eq+PartialOrd+Debug+Default {
    /// Advances a timestamp according to the timestamp actions on the path.
    fn results_in(&self, src: &T) -> T;
    /// Composes this path summary with another path summary.
    fn followed_by(&self, other: &Self) -> Self;
}

/// An empty timestamp used by the root scope.
#[derive(Copy, Clone, Hash, Eq, Ord, PartialOrd, PartialEq, Default)]
pub struct RootTimestamp;
impl Timestamp for RootTimestamp { type Summary = RootSummary; }
impl Debug for RootTimestamp {
    #[inline]
    fn fmt(&self, f: &mut Formatter) -> Result<(), Error> {
        f.write_str("Root")
    }
}

impl Abomonation for RootTimestamp { }
impl RootTimestamp {
    /// Constructs a new `Product<RootTimestamp,T>`.
    #[inline]
    pub fn new<T: Timestamp>(t: T) -> Product<RootTimestamp, T> {
        Product::new(RootTimestamp, t)
    }
}

/// An empty path summary for root timestamps.
#[derive(Copy, Clone, Eq, Ord, PartialOrd, PartialEq, Debug, Default)]
pub struct RootSummary;
impl PathSummary<RootTimestamp> for RootSummary {
    #[inline]
    fn results_in(&self, _: &RootTimestamp) -> RootTimestamp { RootTimestamp }
    #[inline]
    fn followed_by(&self, _: &RootSummary) -> RootSummary { RootSummary }
}

impl Timestamp for () { type Summary = (); }
impl PathSummary<()> for () {
    fn results_in(&self, _src: &()) -> () { () }
    fn followed_by(&self, _other: &()) -> () { () }
}

impl Timestamp for usize { type Summary = usize; }
impl PathSummary<usize> for usize {
    #[inline]
    fn results_in(&self, src: &usize) -> usize { *self + *src }
    #[inline]
    fn followed_by(&self, other: &usize) -> usize { *self + *other }
}

impl Timestamp for u64 { type Summary = u64; }
impl PathSummary<u64> for u64 {
    #[inline]
    fn results_in(&self, src: &u64) -> u64 { *self + *src }
    #[inline]
    fn followed_by(&self, other: &u64) -> u64 { *self + *other }
}

impl Timestamp for u32 { type Summary = u32; }
impl PathSummary<u32> for u32 {
    #[inline]
    fn results_in(&self, src: &u32) -> u32 { *self + *src }
    #[inline]
    fn followed_by(&self, other: &u32) -> u32 { *self + *other }
}

impl Timestamp for i32 { type Summary = i32; }
impl PathSummary<i32> for i32 {
    #[inline]
    fn results_in(&self, src: &i32) -> i32 { *self + *src }
    #[inline]
    fn followed_by(&self, other: &i32) -> i32 { *self + *other }
}
