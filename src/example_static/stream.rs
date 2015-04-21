use example_static::builder::GraphBuilder;

use progress::Timestamp;
use progress::nested::subgraph::{Source, Target};

use communication::Observer;
use communication::Data;
use communication::output_port::Registrar;

#[derive(Clone)]
pub struct Stream<T: Timestamp, D:Data> {
    pub name:   Source,                 // used to name the source in the host graph.
    pub ports:  Registrar<T, D>,        // used to register interest in the output.
}

impl<T: Timestamp, D:Data> Stream<T, D> {
    pub fn enable<G: GraphBuilder<Timestamp=T>>(&self, builder: G) -> ActiveStream<G, D> {
        ActiveStream { stream: self.clone(), builder: builder }
    }

    pub fn new(source: Source, output: Registrar<T, D>) -> Self {
        Stream { name: source, ports: output }
    }
}

pub trait EnableExt<G: GraphBuilder> {
    fn enable<D: Data>(self, stream: &Stream<G::Timestamp, D>) -> ActiveStream<G, D>;
}

impl<'a, G: GraphBuilder> EnableExt<G> for G {
    fn enable<D: Data>(self, stream: &Stream<G::Timestamp, D>) -> ActiveStream<G, D> {
        ActiveStream { stream: stream.clone(), builder: self }
    }
}

pub struct ActiveStream<G: GraphBuilder, D: Data> {
    pub stream:  Stream<G::Timestamp, D>,
    pub builder: G,
}

impl<G: GraphBuilder, D: Data> ActiveStream<G, D> {
    pub fn disable(self) -> Stream<G::Timestamp, D> {
        self.stream
    }

    pub fn connect_to<O>(&mut self, target: Target, observer: O)
    where O: Observer<Time=G::Timestamp, Data=D>+'static {
        self.builder.connect(self.stream.name, target);
        self.stream.ports.add_observer(observer);
    }

    pub fn transfer_borrow_to<D2: Data>(self, source: Source, ports: Registrar<G::Timestamp, D2>) -> ActiveStream<G, D2> {
        ActiveStream {
            stream: Stream {
                name: source,
                ports: ports,
            },
            builder: self.builder,
        }
    }
}
