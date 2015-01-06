use core::fmt::Show;
use std::hash::Hash;
use std::default::Default;


pub trait Timestamp: Eq+PartialOrd+PartialEq+Copy+Default+Hash+Show+Send+Clone+'static { }

impl Timestamp for () { }
impl Timestamp for uint { }
