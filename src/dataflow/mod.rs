//! Abstractions for timely dataflow programming.
//!
//! Timely dataflow programs are constructed by manipulating [`Stream`](./stream/index.html) objects,
//! most often using pre-defined [operators](./operators/index.html) that implement known patterns.
//!
//! #Examples
//! ```
//! use timely::dataflow::operators::{ToStream, Inspect};
//!
//! timely::example(|scope| {
//!     (0..10).to_stream(scope)
//!            .inspect(|x| println!("seen: {:?}", x));
//! });
//! ```

pub use self::stream::Stream;
pub use self::scopes::{Scope, ScopeParent};

pub use self::operators::input::Handle as InputHandle;
pub use self::operators::probe::Handle as ProbeHandle;

pub mod operators;
pub mod channels;
pub mod scopes;
pub mod stream;
