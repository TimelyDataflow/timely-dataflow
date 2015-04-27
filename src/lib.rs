#![feature(std_misc)]
#![feature(collections)]
#![feature(hash)]

#![allow(dead_code)]

//! Timely dataflow is framework for managing and executing data-parallel dataflow computations.

//! #Examples
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
pub use example_static::{GraphRoot, InputExtensionTrait, GraphBuilder};

pub mod networking;
pub mod progress;
// pub mod example;
pub mod example_static;
pub mod communication;
