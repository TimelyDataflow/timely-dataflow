pub mod columnar;

pub trait Serializable {
    fn encode(typed: Self, bytes: &mut Vec<u8>);
    fn decode(bytes: &mut [u8]) -> Result<Self, &mut [u8]>;
}
