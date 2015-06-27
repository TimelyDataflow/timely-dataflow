use serialization::Serializable;
use columnar::{Columnar, ColumnarStack};

impl<T: Abomonation+Columnar> Serializable for T {
    fn encode(typed: Self, bytes: &mut Vec<u8>) {
        let mut stack: <T as Columnar>::Stack = Default::default();
        stack.push(typed);
        stack.encode(bytes).unwrap();
    }
    fn decode(bytes: &mut [u8]) -> Result<Self, &mut [u8]> {
        let mut stack: <T as Columnar>::Stack = Default::default();
        stack.decode(&mut &bytes[..]).unwrap();
        if let Some(result) = stack.pop() { Ok(result) } else { Err(bytes) }
    }
}
