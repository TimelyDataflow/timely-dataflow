//! A handle to a typed stream of timely data.
//!
//! Most high-level timely dataflow programming is done with streams, which are each a handle to an
//! operator output. Extension methods on the `Stream` type provide the appearance of higher-level
//! declarative progamming, while constructing a dataflow graph underneath.

use progress::Timestamp;
use progress::nested::subgraph::{Source, Target};

use {Data, Push};
use dataflow::Scope;
use dataflow::channels::pushers::tee::TeeHelper;
use dataflow::channels::Content;

/// Abstraction of a stream of `D: Data` records timestamped with `S::Timestamp`.
///
/// Internally `Stream` maintains a list of data recipients who should be presented with data
/// produced by the source of the stream.
#[derive(Clone)]
pub struct Stream<S: Scope, D:Data> {
    /// The progress identifier of the stream's data source.
    name: Source,
    /// The `Scope` containing the stream.
    scope: S,
    /// Maintains a list of Push<(T, Content<D>)> interested in the stream's output.
    ports: TeeHelper<S::Timestamp, D>,
}

impl<S: Scope, D:Data> Stream<S, D> {

    pub fn connect_to<P: Push<(S::Timestamp, Content<D>)>+'static>(&self, target: Target, pusher: P, _identifier: usize) {
        // LOGGING
        if cfg!(feature = "logging") {
            println!("CREATION_EDGE: {}:\t{:?}: {:?} -> {:?}", _identifier, self.scope.addr(), self.name, target);
        }
        self.scope.add_edge(self.name, target);
        self.ports.add_pusher(pusher);
    }

    pub fn new(source: Source, output: TeeHelper<S::Timestamp, D>, scope: S) -> Self {
        Stream { name: source, ports: output, scope: scope }
    }
    pub fn name(&self) -> &Source { &self.name }

    pub fn scope(&self) -> S { self.scope.clone() }
}
