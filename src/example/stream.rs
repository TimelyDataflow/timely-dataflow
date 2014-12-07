use progress::{Timestamp, PathSummary, Graph};
use progress::subgraph::Source;

use example::ports::SourcePort;

pub struct Stream<T: Timestamp, S: PathSummary<T>, D:Copy+'static>
{
    pub name:   Source,                         // used to name the source in the host graph.
    pub port:   Box<SourcePort<T, D>>,          // used to register interest in the output.
    pub graph:  Box<Graph<T, S>>
}
