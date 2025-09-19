//! Partition a stream of records into multiple streams.
use std::collections::BTreeMap;

use crate::container::{DrainContainer, ContainerBuilder, PushInto};
use crate::dataflow::channels::pact::Pipeline;
use crate::dataflow::operators::generic::builder_rc::OperatorBuilder;
use crate::dataflow::{Scope, StreamCore};
use crate::Container;

/// Partition a stream of records into multiple streams.
pub trait Partition<G: Scope, C: DrainContainer> {
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
        F: FnMut(C::Item<'_>) -> (u64, D2) + 'static;
}

impl<G: Scope, C: Container + DrainContainer> Partition<G, C> for StreamCore<G, C> {
    fn partition<CB, D2, F>(&self, parts: u64, mut route: F) -> Vec<StreamCore<G, CB::Container>>
    where
        CB: ContainerBuilder + PushInto<D2>,
        F: FnMut(C::Item<'_>) -> (u64, D2) + 'static,
    {
        let mut builder = OperatorBuilder::new("Partition".to_owned(), self.scope());
        builder.set_notify(false);

        let mut input = builder.new_input(self, Pipeline);
        let mut outputs = Vec::with_capacity(parts as usize);
        let mut streams = Vec::with_capacity(parts as usize);

        let mut c_build = CB::default();

        for _ in 0..parts {
            let (output, stream) = builder.new_output::<CB::Container>();
            outputs.push(output);
            streams.push(stream);
        }

        let mut caps = BTreeMap::default();

        builder.build(move |_| {
            let mut todo: BTreeMap<_,Vec<_>> = Default::default();
            move |_frontiers| {
                let mut handles = outputs.iter_mut().map(|o| o.activate()).collect::<Vec<_>>();
                let mut parts = BTreeMap::<u64,Vec<_>>::default();
                while let Some((cap, data)) = input.next() {
                    todo.entry(cap.time().clone()).or_default().push(std::mem::take(data));
                    caps.insert(cap.time().clone(), cap);
                }

                while let Some((time, dataz)) = todo.pop_first() {
                    let cap = caps.remove(&time).unwrap();
                    for mut data in dataz.into_iter() {
                        for datum in data.drain() {
                            let (part, datum) = route(datum);
                            parts.entry(part).or_default().push(datum);
                        }
                    }
                    while let Some((part, data)) = parts.pop_first() {
                        for datum in data.into_iter() {
                            c_build.push_into(datum);
                            while let Some(container) = c_build.extract() {
                                handles[part as usize].give(&cap, container);
                            }
                        }
                        while let Some(container) = c_build.finish() {
                            handles[part as usize].give(&cap, container);
                        }
                    }
                }
            }
        });

        streams
    }
}
