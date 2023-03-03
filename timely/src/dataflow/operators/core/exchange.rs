//! Exchange records between workers.

use crate::Container;
use crate::ExchangeData;
use crate::container::{SizableContainer, PushInto};
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
    fn exchange<F>(self, route: F) -> OwnedStream<G, C>
    where
        for<'a> F: FnMut(&C::Item<'a>) -> u64 + 'static;
}

impl<G: Scope, C, S> Exchange<G, C> for S
where
    C: SizableContainer + ExchangeData + crate::dataflow::channels::ContainerBytes,
    C: for<'a> PushInto<C::Item<'a>>,
    S: StreamLike<G, C>
{
    fn exchange<F>(self, route: F) -> OwnedStream<G, C>
    where
        for<'a> F: FnMut(&C::Item<'a>) -> u64 + 'static,
    {
        self.unary(ExchangeCore::new(route), "Exchange", |_, _| {
            move |input, output| {
                input.for_each(|time, data| {
                    output.session(&time).give_container(data);
                });
            }
        })
    }
}
