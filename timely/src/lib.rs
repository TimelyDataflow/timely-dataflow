//! Timely dataflow is a framework for managing and executing data-parallel dataflow computations.
//!
//! The code is organized in crates and modules that are meant to depend as little as possible on each other.
//!
//! **Serialization**: The [`abomonation`] crate contains simple and highly unsafe
//! serialization routines.
//!
//! **Communication**: The [`timely_communication`] crate defines several primitives for
//! communicating between dataflow workers, and across machine boundaries.
//!
//! **Progress tracking**: The [`timely::progress`](progress) module defines core dataflow structures for
//! tracking and reporting progress in a timely dataflow system, namely the number of outstanding
//! dataflow messages and un-exercised message capabilities throughout the timely dataflow graph.
//! It depends on `timely_communication` to exchange progress messages.
//!
//! **Dataflow construction**: The [`timely::dataflow`](dataflow) module defines an example dataflow system
//! using `communication` and `progress` to both exchange data and progress information, in support
//! of an actual data-parallel timely dataflow computation. It depends on `timely_communication` to
//! move data, and `timely::progress` to provide correct operator notifications.
//!
//! # Examples
//!
//! The following is a hello-world dataflow program.
//!
//! ```
//! use timely::*;
//! use timely::dataflow::operators::{Input, Inspect};
//!
//! // construct and execute a timely dataflow
//! timely::execute_from_args(std::env::args(), |worker| {
//!
//!     // add an input and base computation off of it
//!     let mut input = worker.dataflow(|scope| {
//!         let (input, stream) = scope.new_input();
//!         stream.inspect(|x| println!("hello {:?}", x));
//!         input
//!     });
//!
//!     // introduce input, advance computation
//!     for round in 0..10 {
//!         input.send(round);
//!         input.advance_to(round + 1);
//!         worker.step();
//!     }
//! });
//! ```
//!
//! The program uses `timely::execute_from_args` to spin up a computation based on command line arguments
//! and a closure specifying what each worker should do, in terms of a handle to a timely dataflow
//! `Scope` (in this case, `root`). A `Scope` allows you to define inputs, feedback
//! cycles, and dataflow subgraphs, as part of building the dataflow graph of your dreams.
//!
//! In this example, we define a new scope of root using `scoped`, add an exogenous
//! input using `new_input`, and add a dataflow `inspect` operator to print each observed record.
//! We then introduce input at increasing rounds, indicate the advance to the system (promising
//! that we will introduce no more input at prior rounds), and step the computation.

#![forbid(missing_docs)]

#[macro_use]
extern crate abomonation_derive;
extern crate abomonation;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate timely_communication;
extern crate timely_bytes;
extern crate timely_logging;

pub use execute::{execute, execute_directly, example};
#[cfg(feature = "getopts")]
pub use execute::execute_from_args;
pub use order::PartialOrder;

pub use timely_communication::Config as CommunicationConfig;
pub use worker::Config as WorkerConfig;
pub use execute::Config as Config;
use std::ops::RangeBounds;

/// Re-export of the `timely_communication` crate.
pub mod communication {
    pub use timely_communication::*;
}

/// Re-export of the `timely_bytes` crate.
pub mod bytes {
    pub use timely_bytes::*;
}

/// Re-export of the `timely_logging` crate.
pub mod logging_core {
    pub use timely_logging::*;
}

pub mod worker;
pub mod progress;
pub mod dataflow;
pub mod synchronization;
pub mod execute;
pub mod order;

pub mod logging;
// pub mod log_events;

pub mod scheduling;

/// A composite trait for types usable as data in timely dataflow.
///
/// The `Data` trait is necessary for all types that go along timely dataflow channels.
pub trait Data: Clone+'static { }
impl<T: Clone+'static> Data for T { }

/// A composite trait for types usable on exchange channels in timely dataflow.
///
/// The `ExchangeData` trait extends `Data` with any requirements imposed by the `timely_communication`
/// `Data` trait, which describes requirements for communication along channels.
pub trait ExchangeData: Data + communication::Data { }
impl<T: Data + communication::Data> ExchangeData for T { }

/// A container of data passing on a dataflow edge
pub trait Container: Data {
    /// The type of elements contained by this collection
    type Inner: Data;

    /// The builder for this container
    type Builder: ContainerBuilder<Container=Self>;

    /// Construct an empty container
    fn empty() -> Self {
       Self::Builder::new().build()
    }

    /// Get the capacity of this container.
    fn capacity(&self) -> usize;

    /// Check if the container is empty.
    fn is_empty(&self) -> bool;

    /// The number of elements in this container.
    fn len(&self) -> usize;

    /// Take a container leaving an empty container behind.
    fn take(container: &mut Self) -> Self {
        ::std::mem::replace(container, Self::empty())
    }

    /// Default buffer size.
    fn default_length() -> usize {
        const MESSAGE_BUFFER_SIZE: usize = 1 << 13;
        let size = std::mem::size_of::<Self::Inner>();
        if size == 0 {
            // We could use usize::MAX here, but to avoid overflows we
            // limit the default length for zero-byte types.
            MESSAGE_BUFFER_SIZE
        } else if size <= MESSAGE_BUFFER_SIZE {
            MESSAGE_BUFFER_SIZE / size
        } else {
            1
        }
    }
}

/// A builder for containers
pub trait ContainerBuilder: Extend<<<Self as ContainerBuilder>::Container as Container>::Inner> {
    /// The container this builder builds
    type Container: Container;

    /// Create a new empty builder
    fn new() -> Self;

    /// Create an empty builder, reusing the allocation in `container`.
    fn with_allocation(container: Self::Container) -> Self;

    /// Create en empty builder, reusing the allocation in `container` and leaving an empty
    /// container behind.
    fn with_allocation_ref(container: &mut Self::Container) -> Self where Self: Sized {
        Self::with_allocation(::std::mem::replace(container, Self::Container::empty()))
    }

    /// Take this builder, leaving an empty builder behind.
    fn take_ref(builder: &mut Self) -> Self where Self: Sized {
        ::std::mem::replace(builder, Self::new())
    }

    /// Create a new container and reserve space for at least `capacity` elements.
    fn with_capacity(capacity: usize) -> Self;

    /// Get the capacity of this container.
    fn capacity(&self) -> usize;

    /// Check if the container is empty.
    fn is_empty(&self) -> bool;

    /// The number of elements in this container.
    fn len(&self) -> usize;

    /// Initialize this container by copying all elements from `other` into self.
    fn initialize_from(&mut self, other: &Self::Container);

    /// Push a single element into this builder
    fn push(&mut self, element: <<Self as ContainerBuilder>::Container as Container>::Inner);

    /// Build a container from elements in this builder.
    fn build(self) -> Self::Container;
}

/// A container specialized to be passed on exchange dataflow edges.
pub trait ExchangeContainer: Container+ExchangeData {}

impl<D: Data> Container for Vec<D> {
    type Inner = D;
    type Builder = Vec<D>;

    fn capacity(&self) -> usize {
        Vec::capacity(self)
    }

    fn is_empty(&self) -> bool {
        Vec::is_empty(self)
    }

    fn len(&self) -> usize {
        Vec::len(self)
    }

}

impl<D: Data> ContainerBuilder for Vec<D> {
    type Container = Vec<D>;

    fn new() -> Self {
        Vec::new()
    }

    fn with_allocation(mut container: Self::Container) -> Self {
        container.clear();
        container
    }

    fn with_capacity(capacity: usize) -> Self {
        Vec::with_capacity(capacity)
    }

    fn capacity(&self) -> usize {
        Vec::capacity(self)
    }

    fn is_empty(&self) -> bool {
        Vec::is_empty(self)
    }

    fn len(&self) -> usize {
        Vec::len(self)
    }

    fn initialize_from(&mut self, other: &Self::Container) {
        self.clear();
        self.extend_from_slice(&other);
    }

    fn push(&mut self, element: <<Self as ContainerBuilder>::Container as Container>::Inner) {
        self.push(element)
    }

    fn build(self) -> Self::Container {
        self
    }
}

impl<D: ExchangeData> ExchangeContainer for Vec<D> {}

/// A trait describing how to drain a container
///
/// A trait to permit Timely to extract the data from a container. The trait cannot be used directly
/// for trait bounds due to the lifetime potentially captured by `Self::Drain`. For this reason, the
/// trait needs to be implemented for mutable references to a container. Typically, the following
/// trait bound achieves this:
/// `where D: Data, C: Container<Inner=Data>, for<'a> &'a mut C: DrainContainer<Inner=D>`.
pub trait DrainContainer {
    /// The type of elements stored by this container
    type Inner: Data;
    /// The drain type
    type Drain: Iterator<Item=Self::Inner>;

    /// Drain a range of elements from this container
    fn drain<R: RangeBounds<usize>>(self, range: R) -> Self::Drain;
}

impl<'a, D: Data> DrainContainer for &'a mut Vec<D> {
    type Inner = D;
    type Drain = std::vec::Drain<'a, D>;

    fn drain<R: RangeBounds<usize>>(self, range: R) -> Self::Drain {
        Vec::drain(self, range)
    }
}

mod rc {
    use crate::{Container, ContainerBuilder};
    use std::rc::Rc;

    #[derive(Clone)]
    struct RcContainer<T: Container> {
        inner: Rc<T>,
    }

    impl<T: Container> Container for RcContainer<T> {
        type Inner = T::Inner;
        type Builder = RcContainerBuilder<T::Builder>;

        fn capacity(&self) -> usize {
            T::capacity(&self.inner)
        }

        fn is_empty(&self) -> bool {
            T::is_empty(&self.inner)
        }

        fn len(&self) -> usize {
            T::len(&self.inner)
        }

        fn default_length() -> usize {
            T::default_length()
        }
    }

    struct RcContainerBuilder<T: ContainerBuilder> {
        inner: T,
    }

    impl<T: ContainerBuilder> Extend<<<Self as ContainerBuilder>::Container as Container>::Inner> for RcContainerBuilder<T> {
        fn extend<I: IntoIterator<Item=<<Self as ContainerBuilder>::Container as Container>::Inner>>(&mut self, iter: I) {
            self.inner.extend(iter)
        }
    }

    impl<T: ContainerBuilder> ContainerBuilder for RcContainerBuilder<T> {
        type Container = RcContainer<T::Container>;

        fn new() -> Self {
            Self { inner: T::new() }
        }

        fn with_allocation(container: Self::Container) -> Self {
            match Rc::try_unwrap(container.inner) {
                Ok(inner) => Self { inner: T::with_allocation(inner) },
                Err(_) => Self { inner: T::with_capacity(T::Container::default_length()) },
            }
        }

        fn with_capacity(capacity: usize) -> Self {
            Self { inner: T::with_capacity(capacity) }
        }

        fn capacity(&self) -> usize {
            self.inner.capacity()
        }

        fn is_empty(&self) -> bool {
            self.inner.is_empty()
        }

        fn len(&self) -> usize {
            self.inner.len()
        }

        fn initialize_from(&mut self, other: &Self::Container) {
            self.inner.initialize_from(&other.inner)
        }

        fn push(&mut self, element: <<Self as ContainerBuilder>::Container as Container>::Inner) {
            T::push(&mut self.inner, element)
        }

        fn build(self) -> Self::Container {
            RcContainer { inner: Rc::new(T::build(self.inner)) }
        }
    }
}
