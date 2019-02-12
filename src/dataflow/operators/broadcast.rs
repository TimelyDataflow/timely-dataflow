//! Broadcast records to all workers.

// use std::rc::Rc;
// use std::cell::RefCell;

// use communication::Pull;

use ::ExchangeData;
// use progress::{Source, Target};
// use progress::{Timestamp, Operate, operate::{Schedule, SharedProgress}, Antichain};
use dataflow::{Stream, Scope};
use dataflow::operators::{Map, Exchange};

// use dataflow::channels::{Message, Bundle};
// use dataflow::channels::pushers::Counter as PushCounter;
// use dataflow::channels::pushers::buffer::Buffer as PushBuffer;
// use dataflow::channels::pushers::Tee;
// use dataflow::channels::pullers::Counter as PullCounter;
// use dataflow::channels::pact::{LogPusher, LogPuller};

/// Broadcast records to all workers.
pub trait Broadcast<D: ExchangeData> {
    /// Broadcast records to all workers.
    ///
    /// # Examples
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

        // NOTE: Simplified implementation due to underlying motion
        // in timely dataflow internals. Optimize once they have
        // settled down.
        let peers = self.scope().peers() as u64;
        self.flat_map(move |x| (0 .. peers).map(move |i| (i,x.clone())))
            .exchange(|ix| ix.0)
            .map(|(_i,x)| x)

        // let mut scope = self.scope();

        // let channel_id = scope.new_identifier();

        // let (pushers, puller) = scope.allocate::<Message<G::Timestamp, D>>(channel_id);
        // let (targets, registrar) = Tee::<G::Timestamp, D>::new();

        // assert_eq!(pushers.len(), scope.peers());

        // let receiver = LogPuller::new(puller, scope.index(), channel_id, scope.logging());

        // let operator_index = self.allocate_operator_index();
        // let mut address = self.addr();
        // address.push(operator_index);

        // let operator = BroadcastOperator {
        //     address,
        //     shared_progress: Rc::new(RefCell::new(SharedProgress::new(scope.peers(), 1))),
        //     index: scope.index(),
        //     peers: scope.peers(),
        //     input: PullCounter::new(receiver),
        //     output: PushBuffer::new(PushCounter::new(targets)),
        // };

        // scope.add_operator_with_index(Box::new(operator), operator_index);

        // for (i, pusher) in pushers.into_iter().enumerate() {
        //     let sender = LogPusher::new(pusher, scope.index(), i, channel_id, scope.logging());
        //     self.connect_to(Target { index: operator_index, port: i }, sender, channel_id);
        // }

        // Stream::new(Source { index: operator_index, port: 0 }, registrar, scope)
    }
}

// struct BroadcastOperator<T: Timestamp, D: ExchangeData> {
//     index: usize,
//     peers: usize,
//     address: Vec<usize>,
//     shared_progress: Rc<RefCell<SharedProgress<T>>>,
//     input: PullCounter<T, D, LogPuller<T, D, Box<Pull<Bundle<T, D>>>>>,
//     output: PushBuffer<T, D, PushCounter<T, D, Tee<T, D>>>,
// }

// impl<T: Timestamp, D: ExchangeData> Schedule for BroadcastOperator<T, D> {
//     fn name(&self) -> &str { "Broadcast" }
//     fn path(&self) -> &[usize] { &self.address[..] }
//     fn schedule(&mut self) -> bool {

//         let mut vec = Vec::new();
//         while let Some(bundle) = self.input.next() {

//             use communication::message::RefOrMut;

//             match bundle.as_ref_or_mut() {
//                 RefOrMut::Ref(bundle) => {
//                     RefOrMut::Ref(&bundle.data).swap(&mut vec);
//                     self.output.session(&bundle.time).give_vec(&mut vec);
//                 },
//                 RefOrMut::Mut(bundle) => {
//                     self.output.session(&bundle.time).give_vec(&mut bundle.data);
//                 },
//             }
//         }
//         self.output.cease();

//         let shared_progress = &mut *self.shared_progress.borrow_mut();
//         self.input.consumed().borrow_mut().drain_into(&mut shared_progress.consumeds[self.index]);
//         self.output.inner().produced().borrow_mut().drain_into(&mut shared_progress.produceds[0]);
//         false
//     }
// }

// impl<T: Timestamp, D: ExchangeData> Operate<T> for BroadcastOperator<T, D> {
//     fn inputs(&self) -> usize { self.peers }
//     fn outputs(&self) -> usize { 1 }

//     fn get_internal_summary(&mut self) -> (Vec<Vec<Antichain<T::Summary>>>, Rc<RefCell<SharedProgress<T>>>) {
//         // TODO: (optimization) some of these internal paths do not actually exist
//         let summary = (0..self.peers).map(|_| vec![Antichain::from_elem(Default::default())]).collect::<Vec<_>>();
//         (summary, self.shared_progress.clone())
//     }

//     fn notify_me(&self) -> bool { false }
// }

