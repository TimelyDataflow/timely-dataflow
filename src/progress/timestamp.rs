use core::fmt::Debug;
use std::hash::Hash;
use std::collections::hash_map::Hasher;
use std::default::Default;
use progress::PathSummary;


pub trait Timestamp: Copy+Eq+PartialOrd+PartialEq+Default+Hash<Hasher>+Debug+Send+Clone+'static {
    type Summary : PathSummary<Self>;
}

impl Timestamp for () {
    type Summary = ();
}
impl Timestamp for u64 {
    type Summary = u64;
}
