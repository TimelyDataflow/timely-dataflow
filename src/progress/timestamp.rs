use std::fmt::Debug;
use std::any::Any;
use std::default::Default;
use std::fmt::Formatter;
use std::fmt::Error;

use progress::nested::product::Product;

use abomonation::Abomonation;

// TODO : Change Copy requirement to Clone; understand Columnar requirement (for serialization at the moment)
pub trait Timestamp: Copy+Eq+PartialOrd+Default+Debug+Send+Any+Abomonation {
    type Summary : PathSummary<Self> + 'static;   // summarizes cumulative action of Timestamp along a path
}

// summarized reachability from one location to another.
// TODO : Change Copy requirement to Clone
pub trait PathSummary<T> : 'static+Copy+Eq+PartialOrd+Debug+Default {
    fn results_in(&self, src: &T) -> T;             // advances a timestamp
    fn followed_by(&self, other: &Self) -> Self;    // composes two summaries
}

#[derive(Copy, Clone, Hash, Eq, PartialOrd, PartialEq, Default)]
pub struct RootTimestamp;
impl Timestamp for RootTimestamp { type Summary = RootSummary; }
impl Debug for RootTimestamp {
    fn fmt(&self, f: &mut Formatter) -> Result<(), Error> {
        f.write_str(&format!("Root"))
    }
}

impl Abomonation for RootTimestamp { }
impl RootTimestamp {
    pub fn new<T: Timestamp>(t: T) -> Product<RootTimestamp, T> {
        Product::new(RootTimestamp, t)
    }
}


#[derive(Copy, Clone, Eq, PartialOrd, PartialEq, Debug, Default)]
pub struct RootSummary;
impl PathSummary<RootTimestamp> for RootSummary {
    fn results_in(&self, _: &RootTimestamp) -> RootTimestamp { RootTimestamp }
    fn followed_by(&self, _: &RootSummary) -> RootSummary { RootSummary }
}


impl Timestamp for u64 { type Summary = u64; }
impl PathSummary<u64> for u64 {
    fn results_in(&self, src: &u64) -> u64 { *self + *src }
    fn followed_by(&self, other: &u64) -> u64 { *self + *other }
}

impl Timestamp for u32 { type Summary = u32; }
impl PathSummary<u32> for u32 {
    fn results_in(&self, src: &u32) -> u32 { *self + *src }
    fn followed_by(&self, other: &u32) -> u32 { *self + *other }
}

impl Timestamp for i32 { type Summary = i32; }
impl PathSummary<i32> for i32 {
    fn results_in(&self, src: &i32) -> i32 { *self + *src }
    fn followed_by(&self, other: &i32) -> i32 { *self + *other }
}
