pub mod abomonation;
// pub mod columnar;

use abomonation::Abomonation;
// use columnar::Columnar;

pub trait Serializable : Abomonation {
    fn encode(typed: Self, bytes: &mut Vec<u8>);
    fn decode(bytes: &mut [u8]) -> Result<Self, &mut [u8]>;
}
