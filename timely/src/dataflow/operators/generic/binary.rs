//! Methods to construct generic streaming and blocking binary operators.

use dataflow::operators::generic::{Notificator, FrontierNotificator};

use ::Data;
use dataflow::channels::pushers::tee::Tee;
use dataflow::channels::pact::ParallelizationContract;

use dataflow::operators::generic::Operator as GenericOperator;
use dataflow::operators::generic::handles::{InputHandle, OutputHandle};

use dataflow::{Stream, Scope};

/// Methods to construct generic streaming and blocking binary operators.
pub trait Binary<G: Scope, D1: Data> {
    /// Creates a new dataflow operator that partitions each of its input stream by a parallelization
    /// strategy `pact`, and repeatedly invokes `logic` which can read from the input streams and
    /// write to the output stream.
    ///
    /// # Examples
    /// ```
    /// use timely::dataflow::operators::ToStream;
    /// use timely::dataflow::operators::generic::binary::Binary;
    /// use timely::dataflow::channels::pact::Pipeline;
    ///
    /// timely::example(|scope| {
    ///     let stream1 = (0..10).to_stream(scope);
    ///     let stream2 = (0..10).to_stream(scope);
    ///
    ///     let mut vector1 = Vec::new();
    ///     let mut vector2 = Vec::new();
    ///
    ///     stream1.binary_stream(&stream2, Pipeline, Pipeline, "example", move |input1, input2, output| {
    ///         input1.for_each(|time, data| {
    ///             data.swap(&mut vector1);
    ///             output.session(&time).give_vec(&mut vector1);
    ///         });
    ///         input2.for_each(|time, data| {
    ///             data.swap(&mut vector2);
    ///             output.session(&time).give_vec(&mut vector2);
    ///         });
    ///     });
    /// });
    /// ```
    #[deprecated(since="0.5", note="please use `Operator`'s `binary` method directly")]
    fn binary_stream<D2: Data,
              D3: Data,
              L: FnMut(&mut InputHandle<G::Timestamp, D1, P1::Puller>,
                       &mut InputHandle<G::Timestamp, D2, P2::Puller>,
                       &mut OutputHandle<G::Timestamp, D3, Tee<G::Timestamp, D3>>)+'static,
              P1: ParallelizationContract<G::Timestamp, D1>,
              P2: ParallelizationContract<G::Timestamp, D2>>
            (&self, &Stream<G, D2>, pact1: P1, pact2: P2, name: &str, logic: L) -> Stream<G, D3>;

    /// Creates a new dataflow operator that partitions its input stream by a parallelization
    /// strategy `pact`, and repeatedly invokes `logic` which can read from the input streams,
    /// write to the output stream, and request and receive notifications. The method also requires
    /// a vector of the initial notifications the operator requires (commonly none).
    ///
    /// # Examples
    /// ```
    /// use timely::dataflow::operators::ToStream;
    /// use timely::dataflow::operators::generic::binary::Binary;
    /// use timely::dataflow::channels::pact::Pipeline;
    ///
    /// timely::example(|scope| {
    ///     let stream1 = (0..10).to_stream(scope);
    ///     let stream2 = (0..10).to_stream(scope);
    ///
    ///     let mut vector1 = Vec::new();
    ///     let mut vector2 = Vec::new();
    ///
    ///     stream1.binary_notify(&stream2, Pipeline, Pipeline, "example", Vec::new(), move |input1, input2, output, notificator| {
    ///         input1.for_each(|time, data| {
    ///             data.swap(&mut vector1);
    ///             output.session(&time).give_vec(&mut vector1);
    ///             notificator.notify_at(time.retain());
    ///         });
    ///         input2.for_each(|time, data| {
    ///             data.swap(&mut vector2);
    ///             output.session(&time).give_vec(&mut vector2);
    ///             notificator.notify_at(time.retain());
    ///         });
    ///         notificator.for_each(|time,_count,_notificator| {
    ///             println!("done with time: {:?}", time.time());
    ///         });
    ///     });
    /// });
    /// ```
    #[deprecated(since="0.5", note="please use `Operator`'s `binary_notify` method directly")]
    fn binary_notify<D2: Data,
              D3: Data,
              L: FnMut(&mut InputHandle<G::Timestamp, D1, P1::Puller>,
                       &mut InputHandle<G::Timestamp, D2, P2::Puller>,
                       &mut OutputHandle<G::Timestamp, D3, Tee<G::Timestamp, D3>>,
                       &mut Notificator<G::Timestamp>)+'static,
              P1: ParallelizationContract<G::Timestamp, D1>,
              P2: ParallelizationContract<G::Timestamp, D2>>
            (&self, &Stream<G, D2>, pact1: P1, pact2: P2, name: &str, notify: Vec<G::Timestamp>, logic: L) -> Stream<G, D3>;
}

impl<G: Scope, D1: Data> Binary<G, D1> for Stream<G, D1> {
    #[inline]
    fn binary_stream<
             D2: Data,
             D3: Data,
             L: FnMut(&mut InputHandle<G::Timestamp, D1, P1::Puller>,
                      &mut InputHandle<G::Timestamp, D2, P2::Puller>,
                      &mut OutputHandle<G::Timestamp, D3, Tee<G::Timestamp, D3>>)+'static,
             P1: ParallelizationContract<G::Timestamp, D1>,
             P2: ParallelizationContract<G::Timestamp, D2>>
             (&self, other: &Stream<G, D2>, pact1: P1, pact2: P2, name: &str, logic: L) -> Stream<G, D3> {

        self.binary(other, pact1, pact2, name, |_, _| logic)
    }

    #[inline]
    fn binary_notify<
             D2: Data,
             D3: Data,
             L: FnMut(&mut InputHandle<G::Timestamp, D1, P1::Puller>,
                      &mut InputHandle<G::Timestamp, D2, P2::Puller>,
                      &mut OutputHandle<G::Timestamp, D3, Tee<G::Timestamp, D3>>,
                      &mut Notificator<G::Timestamp>)+'static,
             P1: ParallelizationContract<G::Timestamp, D1>,
             P2: ParallelizationContract<G::Timestamp, D2>>
             (&self, other: &Stream<G, D2>, pact1: P1, pact2: P2, name: &str, init: Vec<G::Timestamp>, mut logic: L) -> Stream<G, D3> {

        self.binary_frontier(other, pact1, pact2, name, |capability, _info| {
            let mut notificator = FrontierNotificator::new();
            for time in init {
                notificator.notify_at(capability.delayed(&time));
            }

            let logging = self.scope().logging();
            move |input1, input2, output| {
                let frontiers = &[input1.frontier(), input2.frontier()];
                let notificator = &mut Notificator::new(frontiers, &mut notificator, &logging);
                logic(&mut input1.handle, &mut input2.handle, output, notificator);
            }
        })
    }
}
