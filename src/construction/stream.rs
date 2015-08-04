use construction::builder::GraphBuilder;

use progress::Timestamp;
use progress::nested::subgraph::{Source, Target};
use fabric::{Data, Push};
use communication::observer::TeeHelper;
use communication::message::Content;

/// Abstraction of a stream of `D: Data` records timestamped with `G::Timestamp`.
///
/// Internally `Stream` tracks any observer `Observer`
#[derive(Clone)]
pub struct Stream<G: GraphBuilder, D:Data> {
    name: Source,                       // used to name the source in the host graph.
    ports: TeeHelper<G::Timestamp, D>,  // used to register interest in the output.
    builder: G,                         // access to a communicator to allocate edges.
}

impl<G: GraphBuilder, D:Data> Stream<G, D> {

    pub fn connect_to<P: Push<(G::Timestamp, Content<D>)>+'static>(&self, target: Target, pusher: P) {
        self.builder.add_edge(self.name, target);
        self.ports.add_pusher(pusher);
    }

    pub fn new(source: Source, output: TeeHelper<G::Timestamp, D>, builder: G) -> Self {
        Stream { name: source, ports: output, builder: builder }
    }
    pub fn name(&self) -> &Source { &self.name }

    pub fn builder(&self) -> G { self.builder.clone() }
}
