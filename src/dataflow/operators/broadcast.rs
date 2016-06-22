//! Broadcast records to all workers.

use timely_communication::Pull;

use ::ExchangeData;
use progress::nested::subgraph::{Source, Target};
use dataflow::{Stream, Scope};
use progress::ChangeBatch;
use progress::{Timestamp, Operate, Antichain};
use dataflow::channels::{Message};
use dataflow::channels::pushers::Counter as PushCounter;
use dataflow::channels::pushers::buffer::Buffer as PushBuffer;
use dataflow::channels::pushers::Tee;
use dataflow::channels::pullers::Counter as PullCounter;
use dataflow::channels::pact::{Pusher, Puller};

/// Broadcast records to all workers.
pub trait Broadcast<D: ExchangeData> {
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

impl<G: Scope, D: ExchangeData> Broadcast<D> for Stream<G, D> {
    fn broadcast(&self) -> Stream<G, D> {
        let mut scope = self.scope();

        let (pushers, puller, comm_chan) = scope.allocate::<Message<G::Timestamp, D>>();
        let (targets, registrar) = Tee::<G::Timestamp, D>::new();

        let channel_id = scope.new_identifier();

        assert_eq!(pushers.len(), scope.peers());

        let receiver = Puller::new(puller, scope.index(), channel_id, comm_chan);

        let operator = BroadcastOperator {
            index: scope.index(),
            peers: scope.peers(),
            input: PullCounter::new(receiver),
            output: PushBuffer::new(PushCounter::new(targets)),
        };

        let operator_index = scope.add_operator(operator);

        for (i, pusher) in pushers.into_iter().enumerate() {
            let sender = Pusher::new(pusher, scope.index(), i, channel_id, comm_chan);
            self.connect_to(Target { index: operator_index, port: i }, sender, channel_id);
        }

        Stream::new(Source { index: operator_index, port: 0 }, registrar, scope)
    }
}

struct BroadcastOperator<T: Timestamp, D: ExchangeData> {
    index: usize,
    peers: usize,
    input: PullCounter<T, D, Puller<T, D, Box<Pull<Message<T, D>>>>>,
    output: PushBuffer<T, D, PushCounter<T, D, Tee<T, D>>>,
}

impl<T: Timestamp, D: ExchangeData> Operate<T> for BroadcastOperator<T, D> {
    fn name(&self) -> String { "Broadcast".to_owned() }
    fn inputs(&self) -> usize { self.peers }
    fn outputs(&self) -> usize { 1 }

    fn get_internal_summary(&mut self) -> (Vec<Vec<Antichain<T::Summary>>>, Vec<ChangeBatch<T>>) {
        // TODO: (optimization) some of these internal paths do not actually exist
        let summary = (0..self.peers).map(|_| vec![Antichain::from_elem(Default::default())]).collect::<Vec<_>>();
        (summary, vec![ChangeBatch::new()])
    }

    fn pull_internal_progress(&mut self, consumed: &mut [ChangeBatch<T>],
                                         _internal: &mut [ChangeBatch<T>],
                                         produced: &mut [ChangeBatch<T>]) -> bool {

        while let Some((time, data)) = self.input.next() {
            self.output.session(time).give_content(data);
        }
        self.output.cease();
        self.input.consumed().borrow_mut().drain_into(&mut consumed[self.index]);
        self.output.inner().produced().borrow_mut().drain_into(&mut produced[0]);
        false
    }

    fn notify_me(&self) -> bool { false }
}

