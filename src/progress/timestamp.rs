use core::fmt::Debug;
use std::hash::Hash;
use std::any::Any;
use std::default::Default;
use columnar::Columnar;

// TODO : Change Copy requirement to Clone; understand Columnar requirement (for serialization at the moment)
pub trait Timestamp: Copy+Hash+Eq+PartialOrd+Default+Debug+Send+Columnar+Any {
    type Summary : PathSummary<Self> + 'static;   // summarizes cumulative action of Timestamp along a path
}

impl Timestamp for () { type Summary = (); }
impl Timestamp for u64 { type Summary = u64; }

// summarized reachability from one location to another.
// TODO : Change Copy requirement to Clone
pub trait PathSummary<T> : 'static+Copy+Eq+PartialOrd+Debug+Default {
    fn results_in(&self, src: &T) -> T;             // advances a timestamp
    fn followed_by(&self, other: &Self) -> Self;    // composes two summaries
}

impl PathSummary<()> for () {
    fn results_in(&self, _: &()) -> () { () }
    fn followed_by(&self, _: &()) -> () { () }
}

impl PathSummary<u64> for u64 {
    fn results_in(&self, src: &u64) -> u64 { *self + *src }
    fn followed_by(&self, other: &u64) -> u64 { *self + *other }
}
