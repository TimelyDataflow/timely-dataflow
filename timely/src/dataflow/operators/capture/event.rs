//! Traits and types describing timely dataflow events.
//!
//! The `Event` type describes the information an operator can observe about a timely dataflow
//! stream. There are two types of events, (i) the receipt of data and (ii) reports of progress
//! of timestamps.

/// Data and progress events of the captured stream.
#[derive(Debug, Clone, Abomonation, Hash, Ord, PartialOrd, Eq, PartialEq, Deserialize, Serialize)]
pub enum EventCore<T, C> {
    /// Progress received via `push_external_progress`.
    Progress(Vec<(T, i64)>),
    /// Messages received via the data stream.
    Messages(T, C),
}

/// Data and progress events of the captured stream, specialized to vector-based containers.
pub type Event<T, D> = EventCore<T, Vec<D>>;

/// Iterates over contained `EventCore<T, C>`.
///
/// The `EventIterator` trait describes types that can iterate over references to events,
/// and which can be used to replay a stream into a new timely dataflow computation.
///
/// This method is not simply an iterator because of the lifetime in the result.
pub trait EventIteratorCore<T, C> {
    /// Iterates over references to `EventCore<T, C>` elements.
    fn next(&mut self) -> Option<&EventCore<T, C>>;
}

/// A [EventIteratorCore] specialized to vector-based containers.
// TODO: use trait aliases once stable.
pub trait EventIterator<T, D>: EventIteratorCore<T, Vec<D>> {
    /// Iterates over references to `Event<T, D>` elements.
    fn next(&mut self) -> Option<&Event<T, D>>;
}
impl<T, D, E: EventIteratorCore<T, Vec<D>>> EventIterator<T, D> for E {
    fn next(&mut self) -> Option<&Event<T, D>> {
        <Self as EventIteratorCore<_, _>>::next(self)
    }
}


/// Receives `EventCore<T, C>` events.
pub trait EventPusherCore<T, C> {
    /// Provides a new `Event<T, D>` to the pusher.
    fn push(&mut self, event: EventCore<T, C>);
}

/// A [EventPusherCore] specialized to vector-based containers.
// TODO: use trait aliases once stable.
pub trait EventPusher<T, D>: EventPusherCore<T, Vec<D>> {}
impl<T, D, E: EventPusherCore<T, Vec<D>>> EventPusher<T, D> for E {}


// implementation for the linked list behind a `Handle`.
impl<T, C> EventPusherCore<T, C> for ::std::sync::mpsc::Sender<EventCore<T, C>> {
    fn push(&mut self, event: EventCore<T, C>) {
        // NOTE: An Err(x) result just means "data not accepted" most likely
        //       because the receiver is gone. No need to panic.
        let _ = self.send(event);
    }
}

/// A linked-list event pusher and iterator.
pub mod link {

    use std::rc::Rc;
    use std::cell::RefCell;

    use super::{EventCore, EventPusherCore, EventIteratorCore};

    /// A linked list of EventCore<T, C>.
    pub struct EventLinkCore<T, C> {
        /// An event, if one exists.
        ///
        /// An event might not exist, if either we want to insert a `None` and have the output iterator pause,
        /// or in the case of the very first linked list element, which has no event when constructed.
        pub event: Option<EventCore<T, C>>,
        /// The next event, if it exists.
        pub next: RefCell<Option<Rc<EventLinkCore<T, C>>>>,
    }

    /// A [EventLinkCore] specialized to vector-based containers.
    pub type EventLink<T, D> = EventLinkCore<T, Vec<D>>;

    impl<T, C> EventLinkCore<T, C> {
        /// Allocates a new `EventLink`.
        pub fn new() -> EventLinkCore<T, C> {
            EventLinkCore { event: None, next: RefCell::new(None) }
        }
    }

    // implementation for the linked list behind a `Handle`.
    impl<T, C> EventPusherCore<T, C> for Rc<EventLinkCore<T, C>> {
        fn push(&mut self, event: EventCore<T, C>) {
            *self.next.borrow_mut() = Some(Rc::new(EventLinkCore { event: Some(event), next: RefCell::new(None) }));
            let next = self.next.borrow().as_ref().unwrap().clone();
            *self = next;
        }
    }

    impl<T, C> EventIteratorCore<T, C> for Rc<EventLinkCore<T, C>> {
        fn next(&mut self) -> Option<&EventCore<T, C>> {
            let is_some = self.next.borrow().is_some();
            if is_some {
                let next = self.next.borrow().as_ref().unwrap().clone();
                *self = next;
                self.event.as_ref()
            }
            else {
                None
            }
        }
    }

    // Drop implementation to prevent stack overflow through naive drop impl.
    impl<T, C> Drop for EventLinkCore<T, C> {
        fn drop(&mut self) {
            while let Some(link) = self.next.replace(None) {
                if let Ok(head) = Rc::try_unwrap(link) {
                    *self = head;
                }
            }
        }
    }

    impl<T, C> Default for EventLinkCore<T, C> {
        fn default() -> Self {
            Self::new()
        }
    }

    #[test]
    fn avoid_stack_overflow_in_drop() {
        let mut event1 = Rc::new(EventLinkCore::<(),()>::new());
        let _event2 = event1.clone();
        for _ in 0 .. 1_000_000 {
            event1.push(EventCore::Progress(vec![]));
        }
    }
}

/// A binary event pusher and iterator.
pub mod binary {

    use std::io::Write;
    use abomonation::Abomonation;
    use super::{EventCore, EventPusherCore, EventIteratorCore};

    /// A wrapper for `W: Write` implementing `EventPusherCore<T, C>`.
    pub struct EventWriterCore<T, C, W: ::std::io::Write> {
        stream: W,
        phant: ::std::marker::PhantomData<(T, C)>,
    }

    /// [EventWriterCore] specialized to vector-based containers.
    pub type EventWriter<T, D, W> = EventWriterCore<T, Vec<D>, W>;

    impl<T, C, W: ::std::io::Write> EventWriterCore<T, C, W> {
        /// Allocates a new `EventWriter` wrapping a supplied writer.
        pub fn new(w: W) -> Self {
            Self {
                stream: w,
                phant: ::std::marker::PhantomData,
            }
        }
    }

    impl<T: Abomonation, C: Abomonation, W: ::std::io::Write> EventPusherCore<T, C> for EventWriterCore<T, C, W> {
        fn push(&mut self, event: EventCore<T, C>) {
            // TODO: `push` has no mechanism to report errors, so we `unwrap`.
            unsafe { ::abomonation::encode(&event, &mut self.stream).expect("Event abomonation/write failed"); }
        }
    }

    /// A Wrapper for `R: Read` implementing `EventIterator<T, D>`.
    pub struct EventReaderCore<T, C, R: ::std::io::Read> {
        reader: R,
        bytes: Vec<u8>,
        buff1: Vec<u8>,
        buff2: Vec<u8>,
        consumed: usize,
        valid: usize,
        phant: ::std::marker::PhantomData<(T, C)>,
    }

    /// [EventReaderCore] specialized to vector-based containers.
    pub type EventReader<T, D, R> = EventReaderCore<T, Vec<D>, R>;

    impl<T, C, R: ::std::io::Read> EventReaderCore<T, C, R> {
        /// Allocates a new `EventReader` wrapping a supplied reader.
        pub fn new(r: R) -> Self {
            Self {
                reader: r,
                bytes: vec![0u8; 1 << 20],
                buff1: vec![],
                buff2: vec![],
                consumed: 0,
                valid: 0,
                phant: ::std::marker::PhantomData,
            }
        }
    }

    impl<T: Abomonation, C: Abomonation, R: ::std::io::Read> EventIteratorCore<T, C> for EventReaderCore<T, C, R> {
        fn next(&mut self) -> Option<&EventCore<T, C>> {

            // if we can decode something, we should just return it! :D
            if unsafe { ::abomonation::decode::<EventCore<T, C>>(&mut self.buff1[self.consumed..]) }.is_some() {
                let (item, rest) = unsafe { ::abomonation::decode::<EventCore<T, C>>(&mut self.buff1[self.consumed..]) }.unwrap();
                self.consumed = self.valid - rest.len();
                return Some(item);
            }
            // if we exhaust data we should shift back (if any shifting to do)
            if self.consumed > 0 {
                self.buff2.clear();
                self.buff2.write_all(&self.buff1[self.consumed..]).unwrap();
                ::std::mem::swap(&mut self.buff1, &mut self.buff2);
                self.valid = self.buff1.len();
                self.consumed = 0;
            }

            if let Ok(len) = self.reader.read(&mut self.bytes[..]) {
                self.buff1.write_all(&self.bytes[..len]).unwrap();
                self.valid = self.buff1.len();
            }

            None
        }
    }
}
