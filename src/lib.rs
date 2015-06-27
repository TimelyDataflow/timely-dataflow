// #![feature(alloc)]
// #![feature(collections_drain)]

#![allow(dead_code)]

//! Timely dataflow is framework for managing and executing data-parallel dataflow computations.
//!
//! The code is organized in layers that are meant to depend as little as possible on those above them.
//!
//! [/communication](communication/index.html) defines several primitives for communicating.
//!
//! [/progress](progress/index.html) defines core dataflow structures for tracking and reporting
//! outstanding dataflow messages.
//!
//! [/example_static](example_static/index.html) defines an example dataflow system using
//! communication and progress to both exchange data and progress information, in support of an
//! actual data-parallel computation
//!
//!
//!
//!
//!

//! #Examples
//!
//! The following is a hello-world dataflow program. It constructs a `GraphRoot` from a `Communicator`,
//! specifically `ThreadCommunicator` which exchanges data only with itself. It then defines a new
//! `SubgraphBuilder` named `builder`, from which it constructs an input, enables the stream, and applies
//! the `inspect` method for examining records that pass by.

//! Once the dataflow graph is assembled, the program introduces records, before closing the input.
//!
//!
//!
//!

//! ```ignore
//! extern crate timely;
//!
//! use timely::*;
//! use timely::example_static::inspect::InspectExt;
//!
//! // initialize a new computation root
//! let mut computation = GraphRoot::new(ThreadCommunicator);
//!
//! let mut input = {
//!
//!     // allocate and use a scoped subgraph builder
//!     let mut builder = computation.new_subgraph();
//!     let (input, stream) = builder.new_input();
//!     stream.enable(builder)
//!           .inspect(|x| println!("hello {:?}", x));
//!
//!     input   // return the input handle
//! };
//!
//! for round in 0..10 {
//!     input.send_at(round, round..round+1);
//!     input.advance_to(round + 1);
//!     computation.step();
//! }
//!
//! input.close();
//!
//! while computation.step() { } // finish off any remaining work
//! ```

extern crate columnar;
extern crate byteorder;

pub use communication::ThreadCommunicator;
pub use example_static::{GraphRoot, GraphBuilder};
pub use example_static::InputExtensionTrait;

pub mod networking;
pub mod progress;
// pub mod example;
pub mod example_shared;
pub mod example_static;
pub mod communication;
pub mod serialization;

pub mod drain;
