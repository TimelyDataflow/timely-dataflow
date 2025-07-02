//! Partition a stream of records into multiple streams.

use timely_container::{Container, ContainerBuilder, PushInto};

use crate::dataflow::channels::pact::Pipeline;
use crate::dataflow::operators::generic::builder_rc::OperatorBuilder;
use crate::dataflow::{Scope, StreamCore};
use crate::Data;

/// Partition a stream of records into multiple streams.
pub trait Partition<G: Scope, C: Container> {
    /// Produces `parts` output streams, containing records produced and assigned by `route`.
    ///
    /// # Examples
    /// ```
    /// use timely::dataflow::operators::ToStream;
    /// use timely::dataflow::operators::core::{Partition, Inspect};
    /// use timely_container::CapacityContainerBuilder;
    ///
    /// timely::example(|scope| {
    ///     let streams = (0..10).to_stream(scope)
    ///                          .partition::<CapacityContainerBuilder<Vec<_>>, _, _>(3, |x| (x % 3, x));
    ///
    ///     for (idx, stream) in streams.into_iter().enumerate() {
    ///         stream
    ///             .inspect(move |x| println!("seen {idx}: {x:?}"));
    ///     }
    /// });
    /// ```
    fn partition<CB, D2, F>(&self, parts: u64, route: F) -> Vec<StreamCore<G, CB::Container>>
    where
        CB: ContainerBuilder + PushInto<D2>,
        CB::Container: Data,
        F: FnMut(C::Item<'_>) -> (u64, D2) + 'static;
}

impl<G: Scope, C: Container + Data> Partition<G, C> for StreamCore<G, C> {
    fn partition<CB, D2, F>(&self, parts: u64, mut route: F) -> Vec<StreamCore<G, CB::Container>>
    where
        CB: ContainerBuilder + PushInto<D2>,
        CB::Container: Data,
        F: FnMut(C::Item<'_>) -> (u64, D2) + 'static,
    {
        let mut builder = OperatorBuilder::new("Partition".to_owned(), self.scope());
        builder.set_notify(false);

        let mut input = builder.new_input(self, Pipeline);
        let mut outputs = Vec::with_capacity(parts as usize);
        let mut streams = Vec::with_capacity(parts as usize);

        for _ in 0..parts {
            let (output, stream) = builder.new_output::<CB>();
            outputs.push(output);
            streams.push(stream);
        }

        builder.build(move |_| {
            move |_frontiers| {
                let mut handles = outputs.iter_mut().map(|o| o.activate()).collect::<Vec<_>>();
                input.for_each(|time, data| {
                    let mut sessions = handles
                        .iter_mut()
                        .map(|h| h.session_with_builder(&time))
                        .collect::<Vec<_>>();

                    for datum in data.drain() {
                        let (part, datum2) = route(datum);
                        sessions[part as usize].give(datum2);
                    }
                });
            }
        });

        streams
    }
}
