// I think the idea here is that one can wrap any `T: Abomonation` type with something that can
// Deref and DerefMut. This can lead to less work with deserialization, and that may be interesting.
// However, it will just implement `Serialize` differently, and should be compatible with the API
// as it currently stands. 

pub struct Wrapper<D> {
    contents: Contents<D>,
}

enum Contents<D> {
    Bytes(Vec<u8>),
    Typed(D),
}

/// ALLOC : This Drop implementation gets *very* angry if we drop allocated data.
/// ALLOC : It probably shouldn't be used in practice, but should help track down who is being
/// ALLOC : bad about respecting allocated memory.
// impl<D> Drop for Wrapper<D> {
//     match self.contents {
//         Contents::Bytes(bytes, _, _) => { assert!(bytes.capacity() == 0); }
//         Contents::Typed(typed) => { assert!(typed.capacity() == 0); }
//     }
// }

impl<D> Wrapper<D> {
    /// Constructs a `Wrapper` from typed data, replacing its argument with `Vec::new()`.
    pub fn from_typed(typed: &mut Option<D>) -> Option<Wrapper<D>> {
        typed.take().map(|element| Wrapper { contents: Contents::Typed(element) })
    }
    /// ALLOC : dropping of binary data.
    pub fn into_typed(self) -> Option<D> {
        match self.contents {
            Contents::Bytes(_,_,_) => None,
            Contents::Typed(data)  => Some(data),
        }
    }
}

impl<D: Serializable> Wrapper<D> {
    /// Constructs a `Wrapper` from binary data, starting at `offset` in `bytes`.
    ///
    /// This method calls `decode` on the binary data, ensuring that pointers are properly
    /// adjusted, and that future calls to `verify` should succeed.
    pub fn from_bytes(mut bytes: Vec<u8>) -> Wrapper<D> {
        let length = <Vec<D> as Serializable>::decode(&mut bytes[offset..]).unwrap().0.len();
        Wrapper { contents: Contents::Bytes(bytes, offset, length) }
    }
}

impl<D: Serializable> Deref for Wrapper<D> {
    type Target = D;
    fn deref(&self) -> &D {
        match self.contents {
            // NOTE : decoded in Contents::Byte constructor. verification should pass.
            // NOTE : privacy of Contents should mean cannot construct w/o validation.
            Contents::Bytes(ref bytes, offset, _length) => {
                <D as Serializable>::verify(&bytes[offset..]).unwrap().0
            },
            Contents::Typed(ref data) => data,
        }
    }
}

// TODO : Rather than .clone() the decoded data, we should try and re-rig serialization so that the
// TODO : underlying byte array can just be handed to Vec::from_raw_parts, cloning any owned data.
// TODO : I think we would need to make sure that the byte array had the right alignment, so that
// TODO : when the Vec is eventually dropped we don't de-allocate the wrong number of bytes.
// TODO : This require mucking with the Abomonation code, as it doesn't currently let you step in
// TODO : and skip copying the 24 byte Vec struct first. We'd also have to bake in the typed length
// TODO : somewhere outside of this serialized hunk of data.
impl<D: Clone+Serializable> DerefMut for Wrapper<D> {
    fn deref_mut(&mut self) -> &mut Vec<D> {
        let value = if let Contents::Bytes(ref mut bytes, offset, _length) = self.contents {
            let (data, _) = <Vec<D> as Serializable>::verify(&bytes[offset..]).unwrap();
            // ALLOC : clone() will allocate a Vec<D> and maybe more.
            Some(Contents::Typed((*data).clone()))
        }
        else { None };

        if let Some(contents) = value {
            self.contents = contents;
        }

        if let Contents::Typed(ref mut data) = self.contents {
            data
        }
        else { unreachable!() }
    }
}
