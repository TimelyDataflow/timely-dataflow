//! Partition a stream of records into multiple streams.

use crate::container::CapacityContainerBuilder;
use crate::progress::Timestamp;
use crate::dataflow::operators::core::Partition as PartitionCore;
use crate::dataflow::StreamVec;

/// Partition a stream of records into multiple streams.
pub trait Partition<'scope, T: Timestamp, D: 'static> {
    /// Produces `parts` output streams, containing records produced and assigned by `route`.
    ///
    /// # Examples
    /// ```
    /// use timely::dataflow::operators::{ToStream, Inspect, vec::Partition};
    ///
    /// timely::example(|scope| {
    ///     let mut streams = (0..10).to_stream(scope)
    ///                              .partition(3, |x| (x % 3, x));
    ///
    ///     streams.pop().unwrap().inspect(|x| println!("seen 2: {:?}", x));
    ///     streams.pop().unwrap().inspect(|x| println!("seen 1: {:?}", x));
    ///     streams.pop().unwrap().inspect(|x| println!("seen 0: {:?}", x));
    /// });
    /// ```
    fn partition<D2: 'static, F: Fn(D) -> (u64, D2)+'static>(self, parts: u64, route: F) -> Vec<StreamVec<'scope, T, D2>>;
}

impl<'scope, T: Timestamp, D: 'static> Partition<'scope, T, D> for StreamVec<'scope, T, D> {
    fn partition<D2: 'static, F: Fn(D)->(u64, D2)+'static>(self, parts: u64, route: F) -> Vec<StreamVec<'scope, T, D2>> {
        PartitionCore::partition::<CapacityContainerBuilder<_>, _, _>(self, parts, route)
    }
}
