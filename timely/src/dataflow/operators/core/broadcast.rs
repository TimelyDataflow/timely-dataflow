//! Broadcast records to all workers.

use crate::ExchangeData;
use crate::dataflow::{StreamCore, Scope};
use crate::dataflow::channels::ContainerBytes;
use crate::dataflow::operators::Operator;

/// Broadcast records to all workers.
pub trait Broadcast<D: ExchangeData> {
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

impl<G: Scope, C: crate::Container + Send + ExchangeData + ContainerBytes> Broadcast<C> for StreamCore<G, C> {
    fn broadcast(&self) -> StreamCore<G, C> {
        self.unary(crate::dataflow::channels::pact::Broadcast, "Broadcast", |_, _| {
            // This operator does not need to do anything, as the
            // broadcast pact will handle the distribution of data.
            move |input, output| {
                input.for_each(|time, data| {
                    output.session(&time).give_container(data);
                });
            }
        })
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;
    use crate::dataflow::channels::pact::Pipeline;
    use crate::dataflow::operators::Operator;

    #[test]
    fn test_broadcast() {
        use crate::dataflow::operators::{ToStream, Broadcast, Inspect};
        crate::example(|scope| {
            (0..10).to_stream(scope)
                .unary(Pipeline, "ToArc", |_, _| {
                    move |input, output| {
                        input.for_each(|time, data| {
                            output.session(&time).give_container(&mut Arc::new(std::mem::take(data)));
                        });
                    }
                })
                .broadcast()
                .inspect(|x| println!("seen: {:?}", x));
        });
    }
}