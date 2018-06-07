//! Exchange records between workers.

use ::ExchangeData;
use dataflow::channels::pact::Exchange as ExchangePact;
// use dataflow::channels::pact::TimeExchange as TimeExchangePact;
use dataflow::{Stream, Scope};
use dataflow::operators::generic::operator::Operator;
use progress::timestamp::Timestamp;

/// Exchange records between workers.
pub trait Exchange<T, D: ExchangeData> {
    /// Exchange records so that all records with the same `route` are at the same worker.
    ///
    /// #Examples
    /// ```
    /// use timely::dataflow::operators::{ToStream, Exchange, Inspect};
    ///
    /// timely::example(|scope| {
    ///     (0..10).to_stream(scope)
    ///            .exchange(|&x| x)
    ///            .inspect(|x| println!("seen: {:?}", x));
    /// });
    /// ```
    fn exchange(&self, route: impl Fn(&D)->u64+'static) -> Self;
}

impl<T: Timestamp, G: Scope<Timestamp=T>, D: ExchangeData> Exchange<T, D> for Stream<G, D> {
    fn exchange(&self, route: impl Fn(&D)->u64+'static) -> Stream<G, D> {
        let mut vector = Vec::new();
        self.unary(ExchangePact::new(route), "Exchange", move |_,_| move |input, output| {
            input.for_each(|time, data| {
                data.swap(&mut vector);
                output.session(&time).give_vec(&mut vector);
            });
        })
    }
}
