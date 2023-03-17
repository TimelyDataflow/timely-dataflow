//! Exchange records between workers.

use crate::ExchangeData;
use crate::container::PushPartitioned;
use crate::dataflow::channels::pact::Exchange as ExchangePact;
use crate::dataflow::operators::generic::operator::Operator;
use crate::dataflow::{Scope, Stream};

/// Exchange records between workers.
pub trait Exchange<D> {
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
    fn exchange(&self, route: impl FnMut(&D) -> u64 + 'static) -> Self;
}

impl<G: Scope, C> Exchange<C::Item> for Stream<G, C>
where
    C: PushPartitioned + ExchangeData,
    C::Item: ExchangeData,
{
    fn exchange(&self, route: impl FnMut(&C::Item) -> u64 + 'static) -> Stream<G, C> {
        let mut container = Default::default();
        self.unary(ExchangePact::new(route), "Exchange", |_, _| {
            move |input, output| {
                input.for_each(|time, data| {
                    data.swap(&mut container);
                    output.session(&time).give_container(&mut container);
                });
            }
        })
    }
}
