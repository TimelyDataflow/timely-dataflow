use core::fmt::Show;
use std::default::Default;

// summarized reachability from one location to another.
pub trait PathSummary<T> : 'static+Copy+Clone+Eq+PartialOrd+Show+Default
{
    fn results_in(&self, src: &T) -> T;             // advances a timestamp
    fn followed_by(&self, other: &Self) -> Self;    // composes two summaries
}

impl PathSummary<()> for ()
{
    fn results_in(&self, _: &()) -> () { () }
    fn followed_by(&self, _: &()) -> () { () }
}

impl PathSummary<uint> for uint
{
    fn results_in(&self, src: &uint) -> uint { *self + *src }
    fn followed_by(&self, other: &uint) -> uint { *self + *other }
}
