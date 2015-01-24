use core::fmt::Debug;
use std::hash::Hash;
use std::collections::hash_map::Hasher;
use std::default::Default;


pub trait Timestamp: Copy+Eq+PartialOrd+PartialEq+Default+Hash<Hasher>+Debug+Send+Clone+'static { }

impl Timestamp for () { }
impl Timestamp for u64 { }
