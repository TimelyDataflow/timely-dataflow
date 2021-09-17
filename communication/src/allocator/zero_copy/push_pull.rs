//! Push and Pull implementations wrapping serialized data.

use std::rc::Rc;
use std::cell::RefCell;
use std::collections::VecDeque;

use bytes::arc::Bytes;

use crate::allocator::canary::Canary;
use crate::networking::MessageHeader;

use crate::{Data, Push, Pull};
use crate::allocator::Message;

use super::bytes_exchange::{BytesPush, SendEndpoint};
use crate::message::FromAllocated;

/// An adapter into which one may push elements of type `T`.
///
/// This pusher has a fixed MessageHeader, and access to a SharedByteBuffer which it uses to
/// acquire buffers for serialization.
pub struct Pusher<T, A, P: BytesPush> {
    header:     MessageHeader,
    sender:     Rc<RefCell<SendEndpoint<P>>>,
    phantom:    ::std::marker::PhantomData<(T, A)>,
}

impl<T, A, P: BytesPush> Pusher<T, A, P> {
    /// Creates a new `Pusher` from a header and shared byte buffer.
    pub fn new(header: MessageHeader, sender: Rc<RefCell<SendEndpoint<P>>>) -> Pusher<T, A, P> {
        Pusher {
            header,
            sender,
            phantom:    ::std::marker::PhantomData,
        }
    }
}

impl<T:Data, A: From<T>, P: BytesPush> Push<Message<T>, A> for Pusher<T, A, P> {
    #[inline]
    fn push(&mut self, element: Option<Message<T>>, allocation: &mut Option<A>) {
        if let Some(element) = element {

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
            *allocation = element.hollow();
        }
    }
}

/// An adapter from which one can pull elements of type `T`.
///
/// This type is very simple, and just consumes owned `Vec<u8>` allocations. It is
/// not the most efficient thing possible, which would probably instead be something
/// like the `bytes` crate (../bytes/) which provides an exclusive view of a shared
/// allocation.
pub struct Puller<T, A> {
    _canary: Canary,
    current: (Option<Message<T>>, Option<A>),
    receiver: Rc<RefCell<VecDeque<Bytes>>>,    // source of serialized buffers
}

impl<T:Data, A: Data> Puller<T, A> {
    /// Creates a new `Puller` instance from a shared queue.
    pub fn new(receiver: Rc<RefCell<VecDeque<Bytes>>>, _canary: Canary) -> Self {
        Self {
            _canary,
            current: (None, None),
            receiver,
        }
    }
}

impl<T:Data, A: Data> Pull<Message<T>, A> for Puller<T, A> {
    #[inline]
    fn pull(&mut self) -> &mut (Option<Message<T>>, Option<A>) {
        self.current.0 =
        self.receiver
            .borrow_mut()
            .pop_front()
            .map(|bytes| unsafe { Message::from_bytes(bytes) });

        &mut self.current
    }
}

/// An adapter from which one can pull elements of type `T`.
///
/// This type is very simple, and just consumes owned `Vec<u8>` allocations. It is
/// not the most efficient thing possible, which would probably instead be something
/// like the `bytes` crate (../bytes/) which provides an exclusive view of a shared
/// allocation.
pub struct PullerInner<T, A> {
    inner: Box<dyn Pull<Message<T>, A>>,               // inner pullable (e.g. intra-process typed queue)
    _canary: Canary,
    current: (Option<Message<T>>, Option<A>),
    receiver: Rc<RefCell<VecDeque<Bytes>>>,     // source of serialized buffers
}

impl<T: Data, A> PullerInner<T, A> {
    /// Creates a new `PullerInner` instance from a shared queue.
    pub fn new(inner: Box<dyn Pull<Message<T>, A>>, receiver: Rc<RefCell<VecDeque<Bytes>>>, _canary: Canary) -> Self {
        Self {
            inner,
            _canary,
            current: (None, None),
            receiver,
        }
    }
}

impl<T: Data, A> Pull<Message<T>, A> for PullerInner<T, A> {
    #[inline]
    fn pull(&mut self) -> &mut (Option<Message<T>>, Option<A>) {

        let inner = self.inner.pull();
        if inner.0.is_some() {
            inner
        }
        else {
            self.current.0 =
            self.receiver
                .borrow_mut()
                .pop_front()
                .map(|bytes| unsafe { Message::from_bytes(bytes) });

            &mut self.current
        }
    }
}