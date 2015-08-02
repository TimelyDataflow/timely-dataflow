use communication::Data;
use communication::pact::Exchange as ExchangePact;
use construction::{Stream, GraphBuilder};
use construction::operators::unary::Extension;

pub trait Exchange<D: Data> {
    fn exchange<F: Fn(&D)->u64+'static>(&self, func: F) -> Self;
}

impl<G: GraphBuilder, D: Data> Exchange<D> for Stream<G, D> {
    fn exchange<F: Fn(&D)->u64+'static>(&self, func: F) -> Stream<G, D> {
        self.unary_stream(ExchangePact::new(func), "Exchange", |input, output| {
            while let Some((time, data)) = input.pull() {
                output.session(time).give_message(data);
            }
        })
    }
}
