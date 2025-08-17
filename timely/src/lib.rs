//! Timely dataflow is a framework for managing and executing data-parallel dataflow computations.
//!
//! The code is organized in crates and modules that are meant to depend as little as possible on each other.
//!
//! **Serialization**: Timely uses the `bincode` crate for serialization. Performance could be improved.
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

pub use execute::{execute, execute_directly, example};
#[cfg(feature = "getopts")]
pub use execute::execute_from_args;
pub use order::PartialOrder;

pub use timely_communication::Config as CommunicationConfig;
pub use worker::Config as WorkerConfig;
pub use execute::Config as Config;

pub use timely_container::WithProgress;
/// Re-export of the `timely_container` crate.
pub mod container {
    pub use timely_container::*;
}

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

/// A composite trait for types usable as containers in timely dataflow.
///
/// The `Container` trait is necessary for all containers in timely dataflow channels.
pub trait Container: WithProgress + Default + Clone + 'static { }
impl<C: WithProgress + Default + Clone + 'static> Container for C { }

/// A composite trait for types usable on exchange channels in timely dataflow.
///
/// The `ExchangeData` trait extends `Data` with any requirements imposed by the `timely_communication`
/// `Data` trait, which describes requirements for communication along channels.
pub trait ExchangeData: Data + encoding::Data { }
impl<T: Data + encoding::Data> ExchangeData for T { }

#[doc = include_str!("../../README.md")]
#[cfg(doctest)]
pub struct ReadmeDoctests;

/// A wrapper that indicates a serialization/deserialization strategy.
pub use encoding::Bincode;

mod encoding {

    use std::any::Any;
    use serde::{Serialize, Deserialize};
    use timely_bytes::arc::Bytes;
    use timely_communication::Bytesable;

    /// A composite trait for types that may be used with channels.
    pub trait Data : Send+Any+Serialize+for<'a>Deserialize<'a> { }
    impl<T: Send+Any+Serialize+for<'a>Deserialize<'a>> Data for T { }

    /// A wrapper that indicates `bincode` as the serialization/deserialization strategy.
    #[derive(Clone)]
    pub struct Bincode<T> {
        /// Bincode contents.
        pub payload: T,
    }

    impl<T> From<T> for Bincode<T> {
        fn from(payload: T) -> Self {
            Self { payload }
        }
    }

    // We will pad out anything we write to make the result `u64` aligned.
    impl<T: Data> Bytesable for Bincode<T> {
        fn from_bytes(bytes: Bytes) -> Self {
            let typed = ::bincode::deserialize(&bytes[..]).expect("bincode::deserialize() failed");
            let typed_size = ::bincode::serialized_size(&typed).expect("bincode::serialized_size() failed") as usize;
            assert_eq!(bytes.len(), (typed_size + 7) & !7);
            Bincode { payload: typed }
        }

        fn length_in_bytes(&self) -> usize {
            let typed_size = ::bincode::serialized_size(&self.payload).expect("bincode::serialized_size() failed") as usize;
            (typed_size + 7) & !7
        }

        fn into_bytes<W: ::std::io::Write>(&self, mut writer: &mut W) {
            let typed_size = ::bincode::serialized_size(&self.payload).expect("bincode::serialized_size() failed") as usize;
            let typed_slop = ((typed_size + 7) & !7) - typed_size;
            ::bincode::serialize_into(&mut writer, &self.payload).expect("bincode::serialize_into() failed");
            writer.write_all(&[0u8; 8][..typed_slop]).unwrap();
        }
    }

    impl<T> ::std::ops::Deref for Bincode<T> {
        type Target = T;
        fn deref(&self) -> &Self::Target {
            &self.payload
        }
    }
    impl<T> ::std::ops::DerefMut for Bincode<T> {
        fn deref_mut(&mut self) -> &mut Self::Target {
            &mut self.payload
        }
    }
}
