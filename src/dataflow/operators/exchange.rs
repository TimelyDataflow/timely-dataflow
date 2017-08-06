//! Exchange records between workers.

use ::ExchangeData;
use dataflow::channels::pact::Exchange as ExchangePact;
use dataflow::channels::pact::TimeExchange as TimeExchangePact;
use dataflow::{Stream, Scope};
use dataflow::operators::generic::unary::Unary;
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
    fn exchange<F: Fn(&D)->u64+'static>(&self, route: F) -> Self;

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
    fn exchange_ts<F: Fn(&T, &D)->u64+'static>(&self, route: F) -> Self;
}

impl<T: Timestamp, G: Scope<Timestamp=T>, D: ExchangeData> Exchange<T, D> for Stream<G, D> {
    fn exchange<F: Fn(&D)->u64+'static>(&self, route: F) -> Stream<G, D> {
        self.unary_stream(ExchangePact::new(route), "Exchange", |input, output| {
            input.for_each(|time, data| {
                output.session(&time).give_content(data);
            });
        })
    }

    fn exchange_ts<F: Fn(&T, &D)->u64+'static>(&self, route: F) -> Stream<G, D> {
        self.unary_stream(TimeExchangePact::new(route), "Exchange", |input, output| {
            input.for_each(|time, data| {
                output.session(&time).give_content(data);
            });
        })
    }
}
