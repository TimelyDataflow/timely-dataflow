use progress::count_map::CountMap;
use serialization::Serializable;

pub trait Pullable<T, D> {
    fn pull(&mut self) -> Option<(&T, &mut Message<D>)>;
}

impl<T, D, P: ?Sized + Pullable<T, D>> Pullable<T, D> for Box<P> {
    fn pull(&mut self) -> Option<(&T, &mut Message<D>)> { (**self).pull() }
}

pub enum Message<D> {
    Bytes(Vec<u8>, usize, usize),  // bytes[offset..] are valid, and decode to length elements
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

pub struct Counter<T: Eq+Clone+'static, D, P: Pullable<T, D>> {
    pullable:   P,
    consumed:   CountMap<T>,
    phantom: ::std::marker::PhantomData<D>,
}

// NOTE : not implementing pullable, as never composed and users don't
// NOTE : want to have to `use communicator::Pullable;` to use this.
impl<T:Eq+Clone+'static, D, P: Pullable<T, D>> Counter<T, D, P> {
    #[inline]
    pub fn pull(&mut self) -> Option<(&T, &mut Message<D>)> {
        if let Some(pair) = self.pullable.pull() {
            if pair.1.len() > 0 {
                self.consumed.update(&pair.0, pair.1.len() as i64);
                Some(pair)
            }
            else { None }
        }
        else { None }
    }
}

impl<T:Eq+Clone+'static, D, P: Pullable<T, D>> Counter<T, D, P> {
    pub fn new(pullable: P) -> Counter<T, D, P> {
        Counter {
            phantom: ::std::marker::PhantomData,
            pullable: pullable,
            consumed: CountMap::new(),
        }
    }
    pub fn pull_progress(&mut self, consumed: &mut CountMap<T>) {
        while let Some((ref time, value)) = self.consumed.pop() {
            consumed.update(time, value);
        }
    }
}
