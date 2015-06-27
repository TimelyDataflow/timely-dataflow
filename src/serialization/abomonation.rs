use serialization::Serializable;
use abomonation::{Abomonation, encode, decode};
use columnar::{Columnar, ColumnarStack};

impl<T: Abomonation+Columnar+Clone> Serializable for T {
    fn encode(typed: Self, bytes: &mut Vec<u8>) {
        let mut slice = Vec::new();
        slice.push(typed);
        encode(&slice, bytes);
    }
    fn decode(bytes: &mut [u8]) -> Result<Self, &mut [u8]> {
        let result = if let Ok(result) = decode::<T>(bytes) {
            Ok(result[0].clone())
        } else { Err(()) };
        if let Ok(result) = result { Ok(result) } else { Err (bytes) }
    }
}
