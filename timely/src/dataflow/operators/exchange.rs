//! Exchange records between workers.

use crate::ExchangeData;
use crate::dataflow::channels::pact::{Exchange as ExchangePact, ParallelizationContract};
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
    fn exchange(&self, route: impl Fn(&D)->u64+'static) -> Self;

    /// Apply a parallelization contract on the data in a stream
    ///
    /// # Examples
    /// ```
    /// use timely::dataflow::operators::{ToStream, Exchange, Inspect};
    /// use timely::dataflow::channels::pact::LazyExchange;
    ///
    /// timely::example(|scope| {
    ///     (0..10).to_stream(scope)
    ///            .apply_pact(LazyExchange::new(|x| *x))
    ///            .inspect(|x| println!("seen: {:?}", x));
    /// });
    /// ```
    fn apply_pact<P: ParallelizationContract<T, D>>(&self, pact: P) -> Self where T: 'static;
}

impl<G: Scope, D: ExchangeData> Exchange<G::Timestamp, D> for Stream<G, D> {
    fn exchange(&self, route: impl Fn(&D)->u64+'static) -> Stream<G, D> {
        self.apply_pact(ExchangePact::new(route))
    }

    fn apply_pact<P: ParallelizationContract<G::Timestamp, D>>(&self, pact: P) -> Stream<G, D> {
        let mut vector = Vec::new();
        self.unary(pact, "Exchange", move |_,_| move |input, output| {
            input.for_each(|time, data| {
                data.swap(&mut vector);
                output.session(&time).give_vec(&mut vector);
            });
        })
    }
}
