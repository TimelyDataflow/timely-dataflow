//! Timely dataflow is framework for managing and executing data-parallel dataflow computations.
//!
//! The code is organized in crates and modules that are meant to depend as little as possible on each other.
//!
//! **Serialization**: The [`abomonation`](../abomonation/index.html) crate contains simple and highly unsafe
//! serialization routines.
//!
//! **Communication**: The [`timely_communication`](../timely_communication/index.html) crate defines several primitives for
//! communicating between dataflow workers, and across machine boundaries.
//!
//! **Progress tracking**: The [`timely::progress`](progress/index.html) module defines core dataflow structures for
//! tracking and reporting progress in a timely dataflow system, namely the number of outstanding
//! dataflow messages and un-exercised message capabilities throughout the timely dataflow graph.
//! It depends on `timely_communication` to exchange progress messages.
//!
//! **Dataflow construction**: The [`timely::dataflow`](dataflow/index.html) module defines an example dataflow system
//! using `communication` and `progress` to both exchange data and progress information, in support
//! of an actual data-parallel timely dataflow computation. It depends on `timely_communication` to
//! move data, and `timely::progress` to provide correct operator notifications.
//!
//! #Examples
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

extern crate abomonation;
extern crate timely_communication;
extern crate time;
extern crate timely_logging;
extern crate byteorder;

pub use execute::{execute, execute_logging, execute_from_args, execute_from_args_logging, example};
pub use timely_communication::{Push, Pull, Configuration};
pub use order::PartialOrder;

pub mod progress;
pub mod dataflow;
pub mod execute;
pub mod order;

pub mod logging;

/// A composite trait for types usable as data in timely dataflow.
///
/// The `Data` trait is necessary for all types that go along timely dataflow channels.
pub trait Data: ::abomonation::Abomonation+Clone+'static { }
impl<T: ::abomonation::Abomonation+Clone+'static> Data for T { }

/// A composite trait for types usable on exchange channels in timely dataflow.
///
/// The `ExchangeData` trait extends `Data` with any requirements imposed by the `timely_communication` 
/// `Data` trait, which describes requirements for communication along channels. 
pub trait ExchangeData: Data + timely_communication::Data { }
impl<T: Data + timely_communication::Data> ExchangeData for T { }


// /// A composite trait for types usable in timely dataflow.
// pub trait Data: timely_communication::Data + abomonation::Abomonation { }
// impl<T: timely_communication::Data+abomonation::Abomonation> Data for T { }
