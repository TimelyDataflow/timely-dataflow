//! Methods to construct generic streaming and blocking binary operators.

use dataflow::operators::generic::{Notificator, FrontierNotificator};

use ::Data;
use dataflow::channels::pushers::tee::Tee;
use dataflow::channels::pact::ParallelizationContract;

use dataflow::operators::generic::Operator as GenericOperator;
use dataflow::operators::generic::handles::{InputHandle, OutputHandle};

use dataflow::{Stream, Scope};

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
