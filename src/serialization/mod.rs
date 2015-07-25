pub mod abomonation;

use abomonation::Abomonation;

pub trait Serializable : Abomonation {
    fn encode(typed: &Self, bytes: &mut Vec<u8>);
    fn decode(bytes: &mut [u8]) -> Option<(&Self, &mut [u8])>;
}
