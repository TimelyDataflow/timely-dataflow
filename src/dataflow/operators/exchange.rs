//! Exchange records between workers.

use ::ExchangeData;
use dataflow::channels::pact::Exchange as ExchangePact;
use dataflow::channels::pact::TimeExchange as TimeExchangePact;
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

    /// Exchange records by time so that all records whose time and data
    /// evaluate to the same `route` are at the same worker.
    ///
    /// #Examples
    /// ```
    /// use timely::dataflow::operators::{ToStream, Exchange, Inspect};
    ///
    /// timely::example(|scope| {
    ///     (0..10).to_stream(scope)
    ///            .exchange_ts(|&t, &x| t.inner & 1 ^ x)
    ///            .inspect(|x| println!("seen: {:?}", x));
    /// });
    /// ```
    fn exchange_ts(&self, route: impl Fn(&T, &D)->u64+'static) -> Self;
}

impl<T: Timestamp, G: Scope<Timestamp=T>, D: ExchangeData> Exchange<T, D> for Stream<G, D> {
    fn exchange(&self, route: impl Fn(&D)->u64+'static) -> Stream<G, D> {
        self.unary(ExchangePact::new(route), "Exchange", |_,_| |input, output| {
            input.for_each(|time, data| {
                output.session(&time).give_content(data);
            });
        })
    }

    fn exchange_ts(&self, route: impl Fn(&T, &D)->u64+'static) -> Stream<G, D> {
        self.unary(TimeExchangePact::new(route), "Exchange", |_,_| |input, output| {
            input.for_each(|time, data| {
                output.session(&time).give_content(data);
            });
        })
    }
}
