use serialization::Serializable;

pub enum Message<D> {
    Bytes(Vec<u8>, usize, usize),  // bytes[offset..] decodes to length elements
    Typed(Vec<D>),
}

impl<D> Message<D> {
    pub fn len(&self) -> usize {
        match *self {
            Message::Bytes(_, _, length) => length,
            Message::Typed(ref data) => data.len(),
        }
    }
}
impl<D: Serializable> Message<D> {
    pub fn look(&mut self) -> &Vec<D> {
        match *self {
            Message::Bytes(ref mut bytes, offset, _length) => {
                <Vec<D> as Serializable>::decode(&mut bytes[offset..]).unwrap().0
            },
            Message::Typed(ref data) => data,
        }
    }
}
impl<D: Clone+Serializable> Message<D> {
    pub fn take(&mut self) -> &mut Vec<D> {
        let value = if let Message::Bytes(ref mut bytes, offset, _length) = *self {
            let (data, _) = <Vec<D> as Serializable>::decode(&mut bytes[offset..]).unwrap();
            Some(Message::Typed((*data).clone()))
        }
        else { None };

        if let Some(message) = value {
            *self = message;
        }

        if let Message::Typed(ref mut data) = *self {
            data
        }
        else { panic!{} }
    }
}
