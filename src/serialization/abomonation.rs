use serialization::Serializable;
use abomonation::{Abomonation, encode, decode, decode_unchecked};
// use columnar::Columnar;

impl<T: Abomonation> Serializable for T {
    fn encode(typed: &Self, bytes: &mut Vec<u8>) {
        encode(typed, bytes);
    }
    fn decode(bytes: &mut [u8]) -> Option<(&Self, &mut [u8])> {
        match decode::<T>(bytes) {
            Ok(pair) => Some(pair),
            Err(hrm) => { panic!("found this: {}", hrm.len()); }
        }
    }
    unsafe fn assume(bytes: &[u8]) -> &Self {
        decode_unchecked(bytes)
    }
}
