//! Broadcast records to all workers.

use crate::ExchangeData;
use crate::dataflow::{OwnedStream, StreamLike, Scope};
use crate::dataflow::operators::{Map, Exchange};

/// Broadcast records to all workers.
pub trait Broadcast<G: Scope, D: ExchangeData> {
    /// Broadcast records to all workers.
    ///
    /// # Examples
    /// ```
    /// use timely::dataflow::operators::{ToStream, Broadcast, Inspect};
    ///
    /// timely::example(|scope| {
    ///     (0..10).to_stream(scope)
    ///            .broadcast()
    ///            .inspect(|x| println!("seen: {:?}", x));
    /// });
    /// ```
    fn broadcast(self) -> OwnedStream<G, Vec<D>>;
}

impl<G: Scope, D: ExchangeData, S: StreamLike<G, Vec<D>>> Broadcast<G, D> for S {
    fn broadcast(self) -> OwnedStream<G, Vec<D>> {

        // NOTE: Simplified implementation due to underlying motion
        // in timely dataflow internals. Optimize once they have
        // settled down.
        let peers = self.scope().peers() as u64;
        self.flat_map(move |x| (0 .. peers).map(move |i| (i,x.clone())))
            .exchange(|ix| ix.0)
            .map(|(_i,x)| x)
    }
}
