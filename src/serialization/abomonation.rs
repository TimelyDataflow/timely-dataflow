use serialization::Serializable;
use abomonation::{Abomonation, encode, decode};

impl<T: Abomonation+Clone> Serializable for T {
    fn encode(typed: Self, bytes: &mut Vec<u8>) {
        let mut slice = Vec::new();
        slice.push(typed);
        encode(&slice, bytes);
    }
    fn decode(bytes: &mut [u8]) -> Result<Self, &mut [u8]> {
        if let Ok(result) = decode::<T>(bytes) {
            Ok(result[0].clone())
        }
        else { Err (bytes) }
    }
}
