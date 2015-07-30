//! A trait for serializable data types. Currently a proxy for `Abomonation`.


pub mod abomonation;

use abomonation::Abomonation;

pub trait Serializable : Abomonation {
    fn encode(typed: &Self, bytes: &mut Vec<u8>);
    fn decode(bytes: &mut [u8]) -> Option<(&Self, &mut [u8])>;
    fn verify(bytes: &[u8]) -> Option<(&Self, &[u8])>;
}
