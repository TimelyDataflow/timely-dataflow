//! Methods to construct generic streaming and blocking unary operators.

use dataflow::operators::generic::{Notificator, FrontierNotificator};
use dataflow::channels::pushers::Tee;
use dataflow::channels::pact::ParallelizationContract;

use dataflow::operators::generic::Operator as GenericOperator;
use dataflow::operators::generic::handles::{InputHandle, OutputHandle};

use ::Data;

use dataflow::{Stream, Scope};

/// Methods to construct generic streaming and blocking unary operators.
pub trait Unary<G: Scope, D1: Data> {
    /// Creates a new dataflow operator that partitions its input stream by a parallelization
    /// strategy `pact`, and repeatedly invokes `logic` which can read from the input stream and
    /// write to the output stream.
    ///
    /// #Examples
    /// ```
    /// use timely::dataflow::operators::ToStream;
    /// use timely::dataflow::operators::generic::unary::Unary;
    /// use timely::dataflow::channels::pact::Pipeline;
    ///
    /// timely::example(|scope| {
    ///     (0..10).to_stream(scope)
    ///            .unary_stream(Pipeline, "example", |input, output| {
    ///                input.for_each(|time, data| {
    ///                    output.session(&time).give_content(data);
    ///                });
    ///            });
    /// });
    /// ```
    fn unary_stream<D2, L, P> (&self, pact: P, name: &str, logic: L) -> Stream<G, D2>
    where
        D2: Data,
        L: FnMut(&mut InputHandle<G::Timestamp, D1, P::Puller>,
                 &mut OutputHandle<G::Timestamp, D2, Tee<G::Timestamp, D2>>)+'static,
        P: ParallelizationContract<G::Timestamp, D1>;
    /// Creates a new dataflow operator that partitions its input stream by a parallelization
    /// strategy `pact`, and repeatedly invokes `logic` which can read from the input stream,
    /// write to the output stream, and request and receive notifications. The method also requires
    /// a vector of the initial notifications the operator requires (commonly none).
    ///
    /// #Examples
    /// ```
    /// use timely::dataflow::operators::ToStream;
    /// use timely::dataflow::operators::generic::unary::Unary;
    /// use timely::dataflow::channels::pact::Pipeline;
    ///
    /// timely::example(|scope| {
    ///     (0..10).to_stream(scope)
    ///            .unary_notify(Pipeline, "example", Vec::new(), |input, output, notificator| {
    ///                input.for_each(|time, data| {
    ///                    output.session(&time).give_content(data);
    ///                    notificator.notify_at(time);
    ///                });
    ///                notificator.for_each(|time,_,_| {
    ///                    println!("done with time: {:?}", time.time());
    ///                });
    ///            });
    /// });
    /// ```
    fn unary_notify<D2, L, P>(&self, pact: P, name: &str, init: Vec<G::Timestamp>, logic: L) -> Stream<G, D2>
    where
        D2: Data,
        L: FnMut(&mut InputHandle<G::Timestamp, D1, P::Puller>,
                 &mut OutputHandle<G::Timestamp, D2, Tee<G::Timestamp, D2>>,
                 &mut Notificator<G::Timestamp>)+'static,
         P: ParallelizationContract<G::Timestamp, D1>;
}

impl<G: Scope, D1: Data> Unary<G, D1> for Stream<G, D1> {
    fn unary_notify<D2: Data,
            L: FnMut(&mut InputHandle<G::Timestamp, D1, P::Puller>,
                     &mut OutputHandle<G::Timestamp, D2, Tee<G::Timestamp, D2>>,
                     &mut Notificator<G::Timestamp>)+'static,
             P: ParallelizationContract<G::Timestamp, D1>>
             (&self, pact: P, name: &str, init: Vec<G::Timestamp>, mut logic: L) -> Stream<G, D2> {

        self.unary_frontier(pact, name, move |capability| {
            let mut notificator = FrontierNotificator::new();
            for time in init {
                notificator.notify_at(capability.delayed(&time));
            }

            let logging = self.scope().logging();
            move |input, output| {
                let frontier = &[input.frontier()];
                let notificator = &mut Notificator::new(frontier, &mut notificator, &logging);
                logic(&mut input.handle, output, notificator);
            }
        })
    }

    fn unary_stream<D2: Data,
             L: FnMut(&mut InputHandle<G::Timestamp, D1, P::Puller>,
                      &mut OutputHandle<G::Timestamp, D2, Tee<G::Timestamp, D2>>)+'static,
             P: ParallelizationContract<G::Timestamp, D1>>
             (&self, pact: P, name: &str, logic: L) -> Stream<G, D2> {

        self.unary(pact, name, |_| logic)
    }
}
