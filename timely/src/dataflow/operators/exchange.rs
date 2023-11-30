//! Exchange records between workers.

use crate::Container;
use crate::ExchangeData;
use crate::container::PushPartitioned;
use crate::dataflow::channels::pact::ExchangeCore;
use crate::dataflow::operators::generic::operator::Operator;
use crate::dataflow::{Scope, OwnedStream, StreamLike};

/// Exchange records between workers.
pub trait Exchange<G: Scope, C: Container> {
    /// Exchange records between workers.
    ///
    /// The closure supplied should map a reference to a record to a `u64`,
    /// whose value determines to which worker the record will be routed.
    ///
    /// # Examples
    /// ```
    /// use timely::dataflow::operators::{ToStream, Exchange, Inspect};
    ///
    /// timely::example(|scope| {
    ///     (0..10).to_stream(scope)
    ///            .exchange(|x| *x)
    ///            .inspect(|x| println!("seen: {:?}", x));
    /// });
    /// ```
    fn exchange(self, route: impl FnMut(&C::Item) -> u64 + 'static) -> OwnedStream<G, C>;
}

impl<G: Scope, C, S: StreamLike<G, C>> Exchange<G, C> for S
where
    C: PushPartitioned + ExchangeData,
    C::Item: ExchangeData,
{
    fn exchange(self, route: impl FnMut(&C::Item) -> u64 + 'static) -> OwnedStream<G, C> {
        let mut container = Default::default();
        self.unary(ExchangeCore::new(route), "Exchange", |_, _| {
            move |input, output| {
                input.for_each(|time, data| {
                    data.swap(&mut container);
                    output.session(&time).give_container(&mut container);
                });
            }
        })
    }
}
