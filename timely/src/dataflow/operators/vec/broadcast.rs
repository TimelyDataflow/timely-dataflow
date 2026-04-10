//! Broadcast records to all workers.

use crate::ExchangeData;
use crate::progress::Timestamp;
use crate::dataflow::StreamVec;
use crate::dataflow::operators::{vec::Map, Exchange};

/// Broadcast records to all workers.
pub trait Broadcast<D: ExchangeData> {
    /// Broadcast records to all workers.
    ///
    /// # Examples
    /// ```
    /// use timely::dataflow::operators::{ToStream, Inspect, vec::Broadcast};
    ///
    /// timely::example(|scope| {
    ///     (0..10).to_stream(scope)
    ///            .broadcast()
    ///            .inspect(|x| println!("seen: {:?}", x));
    /// });
    /// ```
    fn broadcast(self) -> Self;
}

impl<T: Timestamp, D: ExchangeData + Clone> Broadcast<D> for StreamVec<T, D> {
    fn broadcast(self) -> StreamVec<T, D> {

        // NOTE: Simplified implementation due to underlying motion
        // in timely dataflow internals. Optimize once they have
        // settled down.
        let peers = self.peers() as u64;
        self.flat_map(move |x| (0 .. peers).map(move |i| (i,x.clone())))
            .exchange(|ix| ix.0)
            .map(|(_i,x)| x)
    }
}
