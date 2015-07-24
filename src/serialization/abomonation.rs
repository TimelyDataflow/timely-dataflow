use serialization::Serializable;
use abomonation::{Abomonation, encode, decode};
// use columnar::Columnar;

impl<T: Abomonation+Clone> Serializable for T {
    fn encode(typed: &mut Self, bytes: &mut Vec<u8>) {
        encode(typed, bytes);
    }
    fn decode(bytes: &mut [u8]) -> Option<&Self> {
        decode::<T>(bytes).ok().map(|x| x.0)
    }
}
