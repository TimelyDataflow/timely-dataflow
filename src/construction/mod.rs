//! Tools for constructing timely dataflow graphs.
//!
//! Much of timely dataflow programming is done through manipulation of strongly typed stream of
//! data, represented by the type `Stream<G: GraphBuilder, D: Data>`. A stream can be viewed as a
//! sequence of pairs `(G::Timestamp, Vec<D>)`, although these precise types are not exactly correct.
//!
//! Each `Stream` exists in a graph context `G: GraphBuilder`, where the `GraphBuilder` trait provides
//! methods to define dataflow vertices and connect them with edges. A `GraphBuilder` also provides
//! methods to define new subcomputations, which appear as vertices but whose implementation can be
//! backed by other timely dataflow computations.
//!
//! Several common dataflow operators are available in [`operators/`](./operators/index.html), and are intended
//! to provide a programming experience like iterator chaining in Rust (and other languages,
//! for example LINQ in C#).
//!
//! Several additional types of operators are required to accommodate the "timely" nature of timely
//! dataflow, including operators to manipulate timestamps, enter and leave subcomputations, and to
//! delay and coordinate messages by timestamp.

pub use self::stream::Stream;
pub use self::builder::{GraphBuilder, GraphRoot};

pub mod stream;
pub mod builder;
pub mod operators;
