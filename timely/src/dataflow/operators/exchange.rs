//! Exchange records between workers.

use crate::{ExchangeData, Container, ExchangeContainer, DrainContainer};
use crate::dataflow::channels::pact::Exchange as ExchangePact;
use crate::dataflow::{CoreStream, Scope};
use crate::dataflow::operators::generic::operator::Operator;

/// Exchange records between workers.
pub trait Exchange<T, D: ExchangeData> {
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
    fn exchange(&self, route: impl Fn(&D)->u64+'static) -> Self;
}

// impl<T: Timestamp, G: Scope<Timestamp=T>, D: ExchangeData> Exchange<T, D> for Stream<G, D> {
impl<G: Scope, C: Container<Inner=D>+ExchangeContainer, D: ExchangeData> Exchange<G::Timestamp, D> for CoreStream<G, C>
    where for<'a> &'a mut C: DrainContainer<Inner=D>,
{
    fn exchange(&self, route: impl Fn(&D)->u64+'static) -> CoreStream<G, C> {
        let mut vector = None;
        self.unary(ExchangePact::new(route), "Exchange", move |_,_| move |input, output| {
            input.for_each(|time, data| {
                let data = data.assemble(&mut vector);
                output.session(&time).give_container(data, &mut vector);
            });
        })
    }
}
