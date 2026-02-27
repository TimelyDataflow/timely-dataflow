//! Push and Pull implementations wrapping serialized data.

use std::rc::Rc;
use std::cell::RefCell;
use std::collections::VecDeque;

use timely_bytes::arc::Bytes;

use crate::allocator::canary::Canary;
use crate::networking::MessageHeader;
use crate::{Bytesable, Push, Pull};

use super::bytes_exchange::{BytesPush, SendEndpoint};

/// An adapter into which one may push elements of type `T`.
///
/// This pusher has a fixed MessageHeader, and access to a SharedByteBuffer which it uses to
/// acquire buffers for serialization.
pub struct Pusher<T, P: BytesPush> {
    header:     MessageHeader,
    sender:     Rc<RefCell<SendEndpoint<P>>>,
    phantom:    ::std::marker::PhantomData<T>,
}

impl<T, P: BytesPush> Pusher<T, P> {
    /// Creates a new `Pusher` from a header and shared byte buffer.
    pub fn new(header: MessageHeader, sender: Rc<RefCell<SendEndpoint<P>>>) -> Pusher<T, P> {
        Pusher {
            header,
            sender,
            phantom:    ::std::marker::PhantomData,
        }
    }
}

impl<T: Bytesable, P: BytesPush> Push<T> for Pusher<T, P> {
    #[inline]
    fn push(&mut self, element: &mut Option<T>) {
        if let Some(ref element) = *element {

            // determine byte lengths and build header.
            let mut header = self.header;
            self.header.seqno += 1;
            header.length = element.length_in_bytes();
            assert!(header.length > 0);

            // acquire byte buffer and write header, element.
            let mut borrow = self.sender.borrow_mut();
            {
                let mut bytes = borrow.reserve(header.required_bytes());
                assert!(bytes.len() >= header.required_bytes());
                let writer = &mut bytes;
                header.write_to(writer).expect("failed to write header!");
                element.into_bytes(writer);
            }
            borrow.make_valid(header.required_bytes());
        }
    }
}

/// An adapter from which one can pull elements of type `T`.
///
/// This type is very simple, and just consumes owned `Vec<u8>` allocations. It is
/// not the most efficient thing possible, which would probably instead be something
/// like the `bytes` crate (../bytes/) which provides an exclusive view of a shared
/// allocation.
pub struct Puller<T> {
    _canary: Canary,
    current: Option<T>,
    receiver: Rc<RefCell<VecDeque<Bytes>>>,    // source of serialized buffers
}

impl<T: Bytesable> Puller<T> {
    /// Creates a new `Puller` instance from a shared queue.
    pub fn new(receiver: Rc<RefCell<VecDeque<Bytes>>>, _canary: Canary) -> Puller<T> {
        Puller {
            _canary,
            current: None,
            receiver,
        }
    }
}

impl<T: Bytesable> Pull<T> for Puller<T> {
    #[inline]
    fn pull(&mut self) -> &mut Option<T> {
        self.current =
        self.receiver
            .borrow_mut()
            .pop_front()
            .map(T::from_bytes);

        &mut self.current
    }
}

/// An adapter from which one can pull elements of type `T`.
///
/// This type is very simple, and just consumes owned `Vec<u8>` allocations. It is
/// not the most efficient thing possible, which would probably instead be something
/// like the `bytes` crate (../bytes/) which provides an exclusive view of a shared
/// allocation.
pub struct PullerInner<T> {
    inner: Box<dyn Pull<T>>,               // inner pullable (e.g. intra-process typed queue)
    _canary: Canary,
    current: Option<T>,
    receiver: Rc<RefCell<VecDeque<Bytes>>>,     // source of serialized buffers
}

impl<T: Bytesable> PullerInner<T> {
    /// Creates a new `PullerInner` instance from a shared queue.
    pub fn new(inner: Box<dyn Pull<T>>, receiver: Rc<RefCell<VecDeque<Bytes>>>, _canary: Canary) -> Self {
        PullerInner {
            inner,
            _canary,
            current: None,
            receiver,
        }
    }
}

impl<T: Bytesable> Pull<T> for PullerInner<T> {
    #[inline]
    fn pull(&mut self) -> &mut Option<T> {

        let inner = self.inner.pull();
        if inner.is_some() {
            inner
        }
        else {
            self.current =
            self.receiver
                .borrow_mut()
                .pop_front()
                .map(T::from_bytes);

            &mut self.current
        }
    }
}
