//! Methods to construct generic streaming and blocking unary operators.

use crate::dataflow::operators::generic::{Notificator, FrontierNotificator};
use crate::dataflow::channels::pushers::Tee;
use crate::dataflow::channels::pact::ParallelizationContract;

use crate::dataflow::operators::generic::Operator as GenericOperator;
use crate::dataflow::operators::generic::handles::{InputHandle, OutputHandle};

use crate::Data;

use crate::dataflow::{Stream, Scope};

impl<G: Scope, D1: Data> Unary<G, D1> for Stream<G, D1> {
    fn unary_notify<D2: Data,
            L: FnMut(&mut InputHandle<G::Timestamp, D1, P::Puller>,
                     &mut OutputHandle<G::Timestamp, D2, Tee<G::Timestamp, D2>>,
                     &mut Notificator<G::Timestamp>)+'static,
             P: ParallelizationContract<G::Timestamp, D1>>
             (&self, pact: P, name: &str, init: Vec<G::Timestamp>, mut logic: L) -> Stream<G, D2> {

        self.unary_frontier(pact, name, move |capability, _info| {
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

        self.unary(pact, name, |_, _| logic)
    }
}
