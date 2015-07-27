//! Tools for constructing timely dataflow graphs.

pub use self::stream::Stream;
pub use self::builder::{GraphBuilder, GraphRoot};

pub mod stream;
pub mod builder;
pub mod operators;
