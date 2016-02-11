//! Exchange records between workers.

use ::Data;
use dataflow::channels::pact::Exchange as ExchangePact;
use dataflow::{Stream, Scope};
use dataflow::operators::unary::Unary;

/// Exchange records between workers.
pub trait Exchange<D: Data> {
    /// Exchange records so that all records with the same `route` are at the same worker.
    ///
    /// #Examples
    /// ```
    /// use timely::dataflow::operators::{ToStream, ExchangeExtension, Inspect};
    ///
    /// timely::example(|scope| {
    ///     (0..10).to_stream(scope)
    ///            .exchange(|&x| x)
    ///            .inspect(|x| println!("seen: {:?}", x));
    /// });
    /// ```
    fn exchange<F: Fn(&D)->u64+'static>(&self, route: F) -> Self;
}

impl<G: Scope, D: Data> Exchange<D> for Stream<G, D> {
    fn exchange<F: Fn(&D)->u64+'static>(&self, route: F) -> Stream<G, D> {
        self.unary_stream(ExchangePact::new(route), "Exchange", |input, output| {
            while let Some((time, data)) = input.next() {
                output.session(&time).give_content(data);
            }
        })
    }
}
