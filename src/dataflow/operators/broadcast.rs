//! Broadcast records to all workers.

use ::Data;
use progress::nested::subgraph::{Source, Target};
use dataflow::{Stream, Scope};
use progress::count_map::CountMap;
use progress::{Timestamp, Operate, Antichain};
use dataflow::channels::{Message};
use dataflow::channels::pushers::Counter as PushCounter;
use dataflow::channels::pushers::buffer::Buffer as PushBuffer;
use dataflow::channels::pushers::Tee;
use dataflow::channels::pullers::Counter as PullCounter;
use dataflow::channels::pact::{Pusher, Puller};
use timely_communication::{Allocate};
use std::rc::Rc;
use std::cell::RefCell;

/// Broadcast records to all workers.
pub trait Broadcast<D: Data> {
    /// Broadcast records to all workers.
    ///
    /// #Examples
    /// ```
    /// use timely::dataflow::operators::{ToStream, Broadcast, Inspect};
    ///
    /// timely::example(|scope| {
    ///     (0..10).to_stream(scope)
    ///            .broadcast()
    ///            .inspect(|x| println!("seen: {:?}", x));
    /// });
    /// ```
    fn broadcast(&self) -> Self;
}

impl<G: Scope, D: Data> Broadcast<D> for Stream<G, D> {
    fn broadcast(&self) -> Stream<G, D> {
        let mut scope = self.scope();

        let (pushers, puller) = scope.allocate::<Message<G::Timestamp, D>>();
        let (targets, registrar) = Tee::<G::Timestamp, D>::new();

        let channel_id = scope.new_identifier();

        assert!(pushers.len() == scope.peers());

        let receiver = Puller::new(puller, scope.index(), channel_id);

        let operator = BroadcastOperator {
            index: scope.index(),
            peers: scope.peers(),
            input: PullCounter::new(Box::new(receiver)),
            output: PushBuffer::new(PushCounter::new(targets, Rc::new(RefCell::new(CountMap::new())))),
        };

        let operator_index = scope.add_operator(operator);

        for (i, pusher) in pushers.into_iter().enumerate() {
            let sender = Pusher::new(pusher, scope.index(), i, channel_id);
            self.connect_to(Target { index: operator_index, port: i }, sender, channel_id);
        }

        Stream::new(Source { index: operator_index, port: 0 }, registrar, scope)
    }
}

struct BroadcastOperator<T: Timestamp, D: Data> {
    index: usize,
    peers: usize,
    input: PullCounter<T, D>,
    output: PushBuffer<T, D, PushCounter<T, D, Tee<T, D>>>,
}

impl<T: Timestamp, D: Data> Operate<T> for BroadcastOperator<T, D> {
    fn name(&self) -> String { "Broadcast".to_owned() }
    fn inputs(&self) -> usize { self.peers }
    fn outputs(&self) -> usize { 1 }

    fn get_internal_summary(&mut self) -> (Vec<Vec<Antichain<T::Summary>>>, Vec<CountMap<T>>) {
        // TODO: (optimization) some of these internal paths do not actually exist
        let summary = (0..self.peers).map(|_| vec![Antichain::from_elem(Default::default())]).collect::<Vec<_>>();
        (summary, vec![CountMap::new()])
    }

    fn pull_internal_progress(&mut self, consumed: &mut [CountMap<T>],
                                         _internal: &mut [CountMap<T>],
                                         produced: &mut [CountMap<T>]) -> bool {

        while let Some((time, data)) = self.input.next() {
            self.output.session(time).give_content(data);
        }
        self.output.cease();
        self.input.pull_progress(&mut consumed[self.index]);
        self.output.inner().pull_progress(&mut produced[0]);
        false
    }

    fn notify_me(&self) -> bool { false }
}

