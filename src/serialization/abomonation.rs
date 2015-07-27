use serialization::Serializable;
use abomonation::{Abomonation, encode, decode, verify};
// use columnar::Columnar;

impl<T: Abomonation> Serializable for T {
    fn encode(typed: &Self, bytes: &mut Vec<u8>) {
        encode(typed, bytes);
    }
    fn decode(bytes: &mut [u8]) -> Option<(&Self, &mut [u8])> {
        decode::<T>(bytes)
    }
    fn verify(bytes: &[u8]) -> Option<(&Self, &[u8])> {
        verify::<T>(bytes)
    }
}
