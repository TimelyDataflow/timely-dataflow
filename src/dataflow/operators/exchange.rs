use ::Data;
use dataflow::channels::pact::Exchange as ExchangePact;
use dataflow::{Stream, Scope};
use dataflow::operators::unary::Unary;

pub trait Exchange<D: Data> {
    fn exchange<F: Fn(&D)->u64+'static>(&self, func: F) -> Self;
}

impl<G: Scope, D: Data> Exchange<D> for Stream<G, D> {
    fn exchange<F: Fn(&D)->u64+'static>(&self, func: F) -> Stream<G, D> {
        self.unary_stream(ExchangePact::new(func), "Exchange", |input, output| {
            while let Some((time, data)) = input.next() {
                output.session(time).give_content(data);
            }
        })
    }
}
