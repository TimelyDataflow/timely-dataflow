use core::fmt::Show;
use std::hash::Hash;
use std::default::Default;


pub trait Time: Ord+Eq+PartialOrd+PartialEq+Copy+Default+Hash+Show+'static { }

impl Time for () { }
impl Time for uint { }
