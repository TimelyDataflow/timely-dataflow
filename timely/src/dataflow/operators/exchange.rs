//! Exchange records between workers.

use crate::ExchangeData;
use crate::dataflow::channels::pact::Exchange as ExchangePact;
use crate::dataflow::{Stream, Scope};
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
    fn exchange(&self, route: impl FnMut(&D)->u64+'static) -> Self;
}

// impl<T: Timestamp, G: Scope<Timestamp=T>, D: ExchangeData> Exchange<T, D> for Stream<G, D> {
impl<G: Scope, D: ExchangeData> Exchange<G::Timestamp, D> for Stream<G, D> {
    fn exchange(&self, route: impl FnMut(&D)->u64+'static) -> Stream<G, D> {
        let mut vector = Default::default();
        self.unary(ExchangePact::new(route), "Exchange", move |_,_| move |input, output| {
            input.for_each(|time, data| {
                data.swap(&mut vector);
                output.session(&time).give_container(&mut vector);
            });
        })
    }
}
