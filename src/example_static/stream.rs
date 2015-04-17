use example_static::builder::GraphBuilder;

use progress::Timestamp;
use progress::nested::subgraph::{Source, Target};

use communication::Observer;
use communication::Data;
use communication::channels::OutputPort;

#[derive(Clone)]
pub struct Stream<T: Timestamp, D:Data> {
    pub name:   Source,                 // used to name the source in the host graph.
    pub ports:  OutputPort<T, D>,       // used to register interest in the output.
}

impl<T: Timestamp, D:Data> Stream<T, D> {
    pub fn enable<'a, G: GraphBuilder<Timestamp=T>+'a>(&self, builder: &'a mut G) -> ActiveStream<'a, G, D> {
        ActiveStream { stream: self.clone(), builder: builder }
    }

    pub fn new(source: Source, output: OutputPort<T, D>) -> Self {
        Stream { name: source, ports: output }
    }
}

pub struct ActiveStream<'a, G: GraphBuilder+'a, D: Data> {
    pub stream:  Stream<G::Timestamp, D>,
    pub builder: &'a mut G,
}

impl<'a, G: GraphBuilder+'a, D: Data> ActiveStream<'a, G, D> {
    pub fn disable(self) -> Stream<G::Timestamp, D> {
        self.stream
    }

    pub fn connect_to<O>(&mut self, target: Target, observer: O)
    where O: Observer<Time=G::Timestamp, Data=D>+'static {
        self.builder.connect(self.stream.name, target);
        self.stream.ports.add_observer(observer);
    }
    pub fn transfer_borrow_to<D2: Data>(self, source: Source, ports: OutputPort<G::Timestamp, D2>) -> ActiveStream<'a, G, D2> {
        ActiveStream {
            stream: Stream {
                name: source,
                ports: ports,
            },
            builder: self.builder,
        }
    }
}
