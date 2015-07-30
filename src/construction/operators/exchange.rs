use communication::Data;
use communication::pact::Exchange;
use construction::{Stream, GraphBuilder};
use construction::operators::unary::UnaryStreamExt;

pub trait ExchangeExt<D: Data> {
    fn exchange<F: Fn(&D)->u64+'static>(&self, func: F) -> Self;
}

impl<G: GraphBuilder, D: Data> ExchangeExt<D> for Stream<G, D> {
    fn exchange<F: Fn(&D)->u64+'static>(&self, func: F) -> Stream<G, D> {
        self.unary_stream(Exchange::new(func), "Exchange", |input, output| {
            while let Some((time, data)) = input.pull() {
                output.session(time).give_message(data);
            }
        })
    }
}
