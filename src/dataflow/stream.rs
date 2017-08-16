//! A handle to a typed stream of timely data.
//!
//! Most high-level timely dataflow programming is done with streams, which are each a handle to an
//! operator output. Extension methods on the `Stream` type provide the appearance of higher-level
//! declarative progamming, while constructing a dataflow graph underneath.

use progress::nested::subgraph::{Source, Target};

use Push;
use dataflow::Scope;
use dataflow::channels::pushers::tee::TeeHelper;
use dataflow::channels::Content;

// use dataflow::scopes::root::loggers::CHANNELS_Q;

/// Abstraction of a stream of `D: Data` records timestamped with `S::Timestamp`.
///
/// Internally `Stream` maintains a list of data recipients who should be presented with data
/// produced by the source of the stream.
#[derive(Clone)]
pub struct Stream<S: Scope, D> {
    /// The progress identifier of the stream's data source.
    name: Source,
    /// The `Scope` containing the stream.
    scope: S,
    /// Maintains a list of Push<(T, Content<D>)> interested in the stream's output.
    ports: TeeHelper<S::Timestamp, D>,
}

impl<S: Scope, D> Stream<S, D> {
    /// Connects the stream to a destination.
    ///
    /// The destination is described both by a `Target`, for progress tracking information, and a `P: Push` where the
    /// records should actually be sent. The identifier is unique to the edge and is used only for logging purposes.
    pub fn connect_to<P: Push<(S::Timestamp, Content<D>)>+'static>(&self, target: Target, pusher: P, identifier: usize) {

        let logging = self.scope().logging();
        logging.log(::timely_logging::Event::Channels(::timely_logging::ChannelsEvent {
            id: identifier,
            scope_addr: self.scope.addr(),
            source: (self.name.index, self.name.port),
            target: (target.index, target.port),
        }));

        self.scope.add_edge(self.name, target);
        self.ports.add_pusher(pusher);
    }
    /// Allocates a `Stream` from a supplied `Source` name and rendezvous point.
    pub fn new(source: Source, output: TeeHelper<S::Timestamp, D>, scope: S) -> Self {
        Stream { name: source, ports: output, scope: scope }
    }
    /// The name of the stream's source operator.
    pub fn name(&self) -> &Source { &self.name }
    /// The scope immediately containing the stream.
    pub fn scope(&self) -> S { self.scope.clone() }
}
