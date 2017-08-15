//! A partially ordered measure of progress at each timely dataflow location.

use std::fmt::Debug;
use std::any::Any;
use std::default::Default;
use std::fmt::Formatter;
use std::fmt::Error;
use std::hash::Hash;

use order::PartialOrder;
use progress::nested::product::Product;

use abomonation::Abomonation;

// TODO : Change Copy requirement to Clone;
/// A composite trait for types that serve as timestamps in timely dataflow.
pub trait Timestamp: Clone+Eq+PartialOrder+Default+Debug+Send+Any+Abomonation+Hash+Ord {
    /// A type summarizing action on a timestamp along a dataflow path.
    type Summary : PathSummary<Self> + 'static;
}

// TODO : Change Copy requirement to Clone
// TODO : Change `results_in` and perhaps `followed_by` to return an `Option`, indicating no path.
// TODO : This can be important when a summary would "overflow", as we want neither to overflow,
// TODO : nor wrap around, nor saturate.
/// A summary of how a timestamp advances along a timely dataflow path.
pub trait PathSummary<T> : Clone+'static+Eq+PartialOrder+Debug+Default {
    /// Advances a timestamp according to the timestamp actions on the path.
    ///
    /// The path may advance the timestamp sufficiently that it is no longer valid, for example if
    /// incrementing fields would result in integer overflow. In this case, `results_in` should 
    /// return `None`.
    ///
    /// The `feedback` operator, apparently the only point where timestamps are actually incremented
    /// in computation, uses this method and will drop messages with timestamps that when advanced
    /// result in `None`. Ideally, all other timestamp manipulation should behave similarly.
    ///
    /// #Examples
    /// ```
    /// use timely::progress::timestamp::PathSummary;
    ///
    /// let timestamp = 3;
    ///
    /// let summary1 = 5;
    /// let summary2 = usize::max_value() - 2;
    /// 
    /// assert_eq!(summary1.results_in(&timestamp), Some(8));
    /// assert_eq!(summary2.results_in(&timestamp), None);
    /// ```
    fn results_in(&self, src: &T) -> Option<T>;
    /// Composes this path summary with another path summary.
    ///
    /// It is possible that the two composed paths result in an invalid summary, for example when 
    /// integer additions overflow. If it is correct that all timestamps moved along these paths 
    /// would also result in overflow and be discarded, `followed_by` can return `None. It is very
    /// important that this not be used casually, as this does not prevent the actual movement of 
    /// data. 
    ///
    /// #Examples
    /// ```
    /// use timely::progress::timestamp::PathSummary;
    ///
    /// let summary1 = 5;
    /// let summary2 = usize::max_value() - 3;
    /// 
    /// assert_eq!(summary1.followed_by(&summary2), None);
    /// ```    
    fn followed_by(&self, other: &Self) -> Option<Self>;
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

impl PartialOrder for RootTimestamp { #[inline(always)] fn less_equal(&self, _other: &Self) -> bool { true } }

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
    fn results_in(&self, _: &RootTimestamp) -> Option<RootTimestamp> { Some(RootTimestamp) }
    #[inline]
    fn followed_by(&self, _: &RootSummary) -> Option<RootSummary> { Some(RootSummary) }
}

impl PartialOrder for RootSummary { #[inline(always)] fn less_equal(&self, _other: &Self) -> bool { true } }


impl Timestamp for () { type Summary = (); }
impl PathSummary<()> for () {
    fn results_in(&self, _src: &()) -> Option<()> { Some(()) }
    fn followed_by(&self, _other: &()) -> Option<()> { Some(()) }
}

impl Timestamp for usize { type Summary = usize; }
impl PathSummary<usize> for usize {
    #[inline]
    fn results_in(&self, src: &usize) -> Option<usize> { self.checked_add(*src) }
    #[inline]
    fn followed_by(&self, other: &usize) -> Option<usize> { self.checked_add(*other) }
}

impl Timestamp for u64 { type Summary = u64; }
impl PathSummary<u64> for u64 {
    #[inline]
    fn results_in(&self, src: &u64) -> Option<u64> { self.checked_add(*src) }
    #[inline]
    fn followed_by(&self, other: &u64) -> Option<u64> { self.checked_add(*other) }
}

impl Timestamp for u32 { type Summary = u32; }
impl PathSummary<u32> for u32 {
    #[inline]
    fn results_in(&self, src: &u32) -> Option<u32> { self.checked_add(*src) }
    #[inline]
    fn followed_by(&self, other: &u32) -> Option<u32> { self.checked_add(*other) }
}

impl Timestamp for i32 { type Summary = i32; }
impl PathSummary<i32> for i32 {
    #[inline]
    fn results_in(&self, src: &i32) -> Option<i32> { self.checked_add(*src) }
    #[inline]
    fn followed_by(&self, other: &i32) -> Option<i32> { self.checked_add(*other) }
}
