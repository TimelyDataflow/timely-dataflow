//! Partition a stream of records into multiple streams.

use crate::container::CapacityContainerBuilder;
use crate::dataflow::operators::core::Partition as PartitionCore;
use crate::dataflow::{Scope, Stream};
use crate::Data;

/// Partition a stream of records into multiple streams.
pub trait Partition<G: Scope, D: Data, D2: Data, F: Fn(D) -> (u64, D2)> {
    /// Produces `parts` output streams, containing records produced and assigned by `route`.
    ///
    /// # Examples
    /// ```
    /// use timely::dataflow::operators::{ToStream, Partition, Inspect};
    ///
    /// timely::example(|scope| {
    ///     let streams = (0..10).to_stream(scope)
    ///                          .partition(3, |x| (x % 3, x));
    ///
    ///     streams[0].inspect(|x| println!("seen 0: {:?}", x));
    ///     streams[1].inspect(|x| println!("seen 1: {:?}", x));
    ///     streams[2].inspect(|x| println!("seen 2: {:?}", x));
    /// });
    /// ```
    fn partition(&self, parts: u64, route: F) -> Vec<Stream<G, D2>>;
}

impl<G: Scope, D: Data, D2: Data, F: Fn(D)->(u64, D2)+'static> Partition<G, D, D2, F> for Stream<G, D> {
    fn partition(&self, parts: u64, route: F) -> Vec<Stream<G, D2>> {
        PartitionCore::partition::<CapacityContainerBuilder<_>, _, _>(self, parts, route)
    }
}
