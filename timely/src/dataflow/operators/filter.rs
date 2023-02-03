//! Filters a stream by a predicate.

use crate::Data;
use crate::communication::message::RefOrMut;
use crate::dataflow::channels::pact::Pipeline;
use crate::dataflow::{Stream, Scope};
use crate::dataflow::operators::generic::builder_rc::OperatorBuilder;

/// Extension trait for filtering.
pub trait Filter<D: Data> {
    /// Returns a new instance of `self` containing only records satisfying `predicate`.
    ///
    /// # Examples
    /// ```
    /// use timely::dataflow::operators::{ToStream, Filter, Inspect};
    ///
    /// timely::example(|scope| {
    ///     (0..10).to_stream(scope)
    ///            .filter(|x| *x % 2 == 0)
    ///            .inspect(|x| println!("seen: {:?}", x));
    /// });
    /// ```
    fn filter<P: FnMut(&D)->bool+'static>(&self, predicate: P) -> Self;
}

impl<G: Scope, D: Data> Filter<D> for Stream<G, D> {
    fn filter<P: FnMut(&D)->bool+'static>(&self, mut predicate: P) -> Stream<G, D> {
        let mut builder = OperatorBuilder::new("Filter".to_owned(), self.scope());

        let filter = move |data: RefOrMut<Vec<D>>, buffer: &mut Vec<D>| {
            match data {
                RefOrMut::Ref(data) => {
                    buffer.extend(data.into_iter().filter(|x| predicate(*x)).cloned())
                },
                RefOrMut::Mut(data) => {
                    data.retain(|x| predicate(x));
                    std::mem::swap(buffer, data);
                }
            }
        };
        let mut input = builder.new_input_filter(self, Pipeline, vec![], filter);
        let (mut output, stream) = builder.new_output();
        builder.set_notify(false);

        builder.build(|_capabilities| {
            let mut vector = Vec::new();
            move |_frontiers| {
                let mut output_handle = output.activate();
                input.for_each(|time, data| {
                    data.swap(&mut vector);
                    output_handle.session(&time).give_vec(&mut vector);
                })
            }
        });

        stream
    }
}

#[cfg(test)]
mod test {
    use crate::dataflow::operators::{Filter, Inspect, ToStream};

    #[test]
    fn test_filter_example() {
        crate::example(|scope| {
            (0..10).to_stream(scope)
                .filter(|x| *x % 2 == 0)
                .inspect(|x| println!("seen: {:?}", x));
        });
    }
}
