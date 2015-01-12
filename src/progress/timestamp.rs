use core::fmt::Show;
use std::hash::Hash;
use std::collections::hash_map::Hasher;
use std::default::Default;


pub trait Timestamp: Copy+Eq+PartialOrd+PartialEq+Default+Hash<Hasher>+Show+Send+Clone+'static { }

impl Timestamp for () { }
impl Timestamp for u64 { }
