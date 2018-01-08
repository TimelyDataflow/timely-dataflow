//! Partition a stream of records into multiple streams.

use progress::{Timestamp, Operate};
use progress::nested::{Source, Target};
use progress::ChangeBatch;

use Data;

use dataflow::channels::pact::{ParallelizationContract, Pipeline};
use dataflow::channels::pushers::tee::Tee;
use dataflow::channels::pushers::counter::Counter as PushCounter;
use dataflow::channels::pushers::buffer::Buffer as PushBuffer;
use dataflow::channels::pullers::Counter as PullCounter;

use dataflow::{Stream, Scope};

/// Partition a stream of records into multiple streams.
pub trait Partition<G: Scope, D: Data, D2: Data, F: Fn(D)->(u64, D2)> {
    /// Produces `parts` output streams, containing records produced and assigned by `route`.
    ///
    /// #Examples
    /// ```
    /// use timely::dataflow::operators::{ToStream, Partition, Inspect};
    ///
    /// timely::example(|scope| {
    ///     let streams = (0..10).to_stream(scope)
    ///                          .partition(3, |x| (x % 3, x));
    ///
    ///     streams[0].inspect(|x| println!("seen 0: {:?}", x));
    ///     streams[1].inspect(|x| println!("seen 1: {:?}", x));
    ///     streams[2].inspect(|x| println!("seen 2: {:?}", x));
    /// });
    /// ```
    fn partition(&self, parts: u64, route: F) -> Vec<Stream<G, D2>>;
}

impl<G: Scope, D: Data, D2: Data, F: Fn(D)->(u64, D2)+'static> Partition<G, D, D2, F> for Stream<G, D> {
    fn partition(&self, parts: u64, route: F) -> Vec<Stream<G, D2>> {

        let mut scope = self.scope();
        let channel_id = scope.new_identifier();

        let (sender, receiver) = Pipeline.connect(&mut scope, channel_id, self.scope().logging());

        let mut targets = Vec::new();
        let mut registrars = Vec::new();
        for _ in 0..parts {
            let (target, registrar) = Tee::<G::Timestamp,D2>::new();
            targets.push(target);
            registrars.push(registrar);
        }

        let operator = Operator::new(PullCounter::new(receiver), targets, route);
        let index = scope.add_operator(operator);
        self.connect_to(Target { index: index, port: 0 }, sender, channel_id);

        let mut results = Vec::new();
        for (output, registrar) in registrars.into_iter().enumerate() {
            results.push(Stream::new(Source { index: index, port: output }, registrar, scope.clone()));
        }

        results
    }
}

struct Operator<T:Timestamp, D: Data, D2: Data, F: Fn(D)->(u64, D2)> {
    input:   PullCounter<T, D, <Pipeline as ParallelizationContract<T, D>>::Puller>,
    outputs: Vec<PushBuffer<T, D2, PushCounter<T, D2, Tee<T, D2>>>>,
    route:    F,
}

impl<T:Timestamp, D: Data, D2: Data, F: Fn(D)->(u64, D2)> Operator<T, D, D2, F> {
    fn new(input: PullCounter<T, D, <Pipeline as ParallelizationContract<T, D>>::Puller>, outputs: Vec<Tee<T, D2>>, route: F) -> Operator<T, D, D2, F> {
        Operator {
            input: input,
            outputs: outputs.into_iter().map(|x| PushBuffer::new(PushCounter::new(x))).collect(),
            route: route,
        }
    }
}

impl<T:Timestamp, D: Data, D2: Data, F: Fn(D)->(u64, D2)> Operate<T> for Operator<T, D, D2, F> {
    fn name(&self) -> String { "Partition".to_owned() }
    fn inputs(&self) -> usize { 1 }
    fn outputs(&self) -> usize { self.outputs.len() }

    fn pull_internal_progress(&mut self, consumed: &mut [ChangeBatch<T>],
                                        _internal: &mut [ChangeBatch<T>],
                                         produced: &mut [ChangeBatch<T>]) -> bool {

        while let Some((time, data)) = self.input.next() {
            let outputs = self.outputs.iter_mut();

            // TODO : This results in small sends for many parts, as sessions does the buffering
            let mut sessions: Vec<_> = outputs.map(|x| x.session(time)).collect();

            for (part, datum) in data.drain(..).map(&self.route) {
                sessions[part as usize].give(datum);
            }
        }

        self.input.consumed().borrow_mut().drain_into(&mut consumed[0]);
        for (index, output) in self.outputs.iter_mut().enumerate() {
            output.cease();
            output.inner().produced().borrow_mut().drain_into(&mut produced[index]);
        }

        false   // no reason to keep running on Partition's account
    }

    fn notify_me(&self) -> bool { false }
}
