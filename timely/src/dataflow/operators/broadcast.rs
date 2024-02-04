//! Broadcast records to all workers.

use crate::ExchangeData;
use crate::dataflow::{Scope, Stream};
use crate::dataflow::operators::{Map, Exchange};

/// Broadcast records to all workers.
pub trait Broadcast<C: ExchangeData> {
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
    fn broadcast(&self) -> Self;
}

impl<G: Scope, D: ExchangeData> Broadcast<Vec<D>> for Stream<G, D> {
    fn broadcast(&self) -> Stream<G, D> {

        // NOTE: Simplified implementation due to underlying motion
        // in timely dataflow internals. Optimize once they have
        // settled down.
        let peers = self.scope().peers() as u64;
        self.flat_map(move |x| (0 .. peers).map(move |i| (i, x.clone())))
            .exchange(|ix| ix.0)
            .map(|(_i,x)| x)
    }
}

#[cfg(feature = "bincode")]
mod flatcontainer {
    use crate::container::flatcontainer::MirrorRegion;
    use crate::container::flatcontainer::FlatStack;
    use crate::container::flatcontainer::impls::tuple::TupleABRegion;
    use crate::container::flatcontainer::Region;
    use crate::dataflow::operators::{Broadcast, Operator};
    use crate::dataflow::{Scope, StreamCore};
    use crate::dataflow::channels::pact::{ExchangeCore, Pipeline};
    use crate::ExchangeData;

    impl<G: Scope, R: Region + ExchangeData> Broadcast<FlatStack<R>> for StreamCore<G, FlatStack<R>>
    where
        FlatStack<R>: ExchangeData,
        R::Index: ExchangeData,
    {
        fn broadcast(&self) -> Self {
            let peers = self.scope().peers() as u64;
            self.unary::<FlatStack<TupleABRegion<MirrorRegion<u64>, R>>, _, _, _>(Pipeline, "Broadcast send", |_cap, _info| {
                move |input, output| {
                    while let Some((time, data)) = input.next() {
                        let mut session = output.session(&time);
                        for i in 0..peers {
                            for item in &*data {
                                session.give((i, item))
                            }
                        }
                    }
                }
            })
                .unary(ExchangeCore::new(|(i, _)| *i), "Broadcast recv", |_cap, _info| {
                    |input, output| {
                        while let Some((time, data)) = input.next() {
                            let mut session = output.session(&time);
                            for (_, item) in &*data {
                                session.give(item)
                            }
                        }
                    }
                })
        }
    }
}
