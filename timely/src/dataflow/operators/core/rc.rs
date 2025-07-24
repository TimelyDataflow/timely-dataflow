//! Shared containers

use std::rc::Rc;

use crate::container::{PassthroughContainerBuilder, WithProgress};
use crate::dataflow::channels::pact::Pipeline;
use crate::dataflow::operators::Operator;
use crate::dataflow::{Scope, StreamCore};
use crate::Data;

/// Convert a stream into a stream of shared containers
pub trait SharedStream<S: Scope, C> {
    /// Convert a stream into a stream of shared data
    ///
    /// # Examples
    /// ```
    /// use timely::dataflow::operators::{ToStream, Inspect};
    /// use timely::dataflow::operators::rc::SharedStream;
    ///
    /// timely::example(|scope| {
    ///     (0..10).to_stream(scope)
    ///            .shared()
    ///            .inspect(|x| println!("seen: {:?}", x));
    /// });
    /// ```
    fn shared(&self) -> StreamCore<S, Rc<C>>;
}

impl<S: Scope, C: WithProgress + Data> SharedStream<S, C> for StreamCore<S, C> {
    fn shared(&self) -> StreamCore<S, Rc<C>> {
        self.unary::<PassthroughContainerBuilder<_>,_,_,_>(Pipeline, "Shared", move |_, _| {
            move |input, output| {
                input.for_each(|time, data| {
                    output
                        .session_with_builder(&time)
                        .give_container(&mut Rc::new(std::mem::take(data)));
                });
            }
        })
    }
}

#[cfg(test)]
mod test {
    use crate::dataflow::channels::pact::Pipeline;
    use crate::dataflow::operators::capture::Extract;
    use crate::dataflow::operators::rc::SharedStream;
    use crate::dataflow::operators::{Capture, Concatenate, Operator, ToStream};

    #[test]
    fn test_shared() {
        let output = crate::example(|scope| {
            let shared = vec![Ok(0), Err(())].to_stream(scope).container::<Vec<_>>().shared();
            scope
                .concatenate([
                    shared.unary(Pipeline, "read shared 1", |_, _| {
                        move |input, output| {
                            input.for_each(|time, data| {
                                output.session(&time).give(data.as_ptr() as usize);
                            });
                        }
                    }),
                    shared.unary(Pipeline, "read shared 2", |_, _| {
                        move |input, output| {
                            input.for_each(|time, data| {
                                output.session(&time).give(data.as_ptr() as usize);
                            });
                        }
                    }),
                ])
                .container::<Vec<_>>()
                .capture()
        });
        let output = &mut output.extract()[0].1;
        output.sort();
        output.dedup();
        assert_eq!(output.len(), 1);
    }
}
