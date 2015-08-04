#![allow(dead_code)]

//! Timely dataflow is framework for managing and executing data-parallel dataflow computations.
//!
//! The code is organized in modules that are meant to depend as little as possible on each other.
//!
//! **Communication**: [timely::communication](communication/index.html) defines several primitives for
//! communicating between dataflow workers, and across machine boundaries.
//!
//! **Progress tracking**: [timely::progress](progress/index.html) defines core dataflow structures for
//! tracking and reporting progress in a timely dataflow system, namely the number of outstanding
//! dataflow messages and un-exercised message capabilities throughout the timely dataflow graph.
//!
//! **Graph construction**: [timely::construction](construction/index.html) defines an example dataflow system
//! using `communication` and `progress` to both exchange data and progress information, in support
//! of an actual data-parallel timely dataflow computation.
//!
//! #Examples
//!
//! The following is a hello-world dataflow program.
//!
//! ```ignore
//! extern crate timely;
//!
//! use timely::*;
//! use timely::construction::inspect::InspectExt;
//!
//! // construct and execute a timely dataflow
//! timely::execute(std::env::args(), |root| {
//!
//!     // add an input and base computation off of it
//!     let mut input = root.subcomputation(|subgraph| {
//!         let (input, stream) = subgraph.new_input();
//!         stream.inspect(|x| println!("hello {:?}", x));
//!         input
//!     });
//!
//!     // introduce input, advance computation
//!     for round in 0..10 {
//!         input.send_at(round, round..round+1);
//!         input.advance_to(round + 1);
//!         root.step();
//!     }
//!
//!     input.close();          // close the input
//!     while root.step() { }   // finish off the computation
//! });
//! ```
//!
//! The program uses `timely::execute` to spin up a computation based on command line arguments
//! and a closure specifying what each worker should do, in terms of a handle to a timely dataflow
//! `GraphBuilder` (in this case, `root`). A `GraphBuilder` allows you to define inputs, feedback
//! cycles, and dataflow subgraphs, as part of building the dataflow graph of your dreams.
//!
//! In this example, we define a new subgraph of root using `subcomputation`, add an exogenous
//! input using `new_input`, and add a dataflow `inspect` operator to print each observed record.
//! We then introduce input at increasing rounds, indicate the advance to the system (promising
//! that we will introduce no more input at prior rounds), and step the computation. Finally, we
//! close the input (promisng that we will introduce no more input ever) and step the computation
//! until there is no more work to do.
//!

extern crate abomonation;
extern crate byteorder;
extern crate getopts;
extern crate fabric;

pub use execute::execute;
pub mod progress;
pub mod construction;
pub mod communication;
pub mod drain;
pub mod execute;

pub trait Data: fabric::Data + abomonation::Abomonation { }
impl<T: fabric::Data+abomonation::Abomonation> Data for T { }
