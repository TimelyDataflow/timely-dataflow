extern crate getopts;
extern crate byteorder;
extern crate abomonation;

pub mod allocator;
pub mod networking;
pub mod initialize;
mod drain;

use abomonation::{Abomonation, encode, decode};

pub use allocator::{Allocate, Push, Pull, Data};
pub use initialize::{initialize, Configuration};

/// Describes types that can be converted to and from `Vec<u8>`. A default implementation is
/// provided for any `T: Abomonation`, but types may specify their own conversion as well.
pub trait Serialize {
    /// Append the binary representation of `self` to a vector of bytes.
    fn into_bytes(&mut self, &mut Vec<u8>);
    /// Recover an instance of Self from its binary representation.
    fn from_bytes(&mut Vec<u8>) -> Self;
}

impl<T: Abomonation+Clone> Serialize for T {
    fn into_bytes(&mut self, bytes: &mut Vec<u8>) {
        encode(self, bytes);
    }
    fn from_bytes(bytes: &mut Vec<u8>) -> Self {
        (*decode::<T>(bytes).unwrap().0).clone()
    }
}
