//! Conversion to the `StreamCore` type from iterators.

use crate::container::{PushContainer, PushInto};
use crate::Container;
use crate::dataflow::operators::generic::operator::source;
use crate::dataflow::{StreamCore, Scope};

/// Converts to a timely [StreamCore].
pub trait ToStream<C: Container> {
    /// Converts to a timely [StreamCore].
    ///
    /// # Examples
    ///
    /// ```
    /// use timely::dataflow::operators::core::{ToStream, Capture};
    /// use timely::dataflow::operators::core::capture::Extract;
    ///
    /// let (data1, data2) = timely::example(|scope| {
    ///     let data1 = (0..3).to_stream(scope).container::<Vec<_>>().capture();
    ///     let data2 = vec![0,1,2].to_stream(scope).container::<Vec<_>>().capture();
    ///     (data1, data2)
    /// });
    ///
    /// assert_eq!(data1.extract(), data2.extract());
    /// ```
    fn to_stream<S: Scope>(self, scope: &mut S) -> StreamCore<S, C>;
}

impl<C: PushContainer, I: IntoIterator+'static> ToStream<C> for I where I::Item: PushInto<C> {
    fn to_stream<S: Scope>(self, scope: &mut S) -> StreamCore<S, C> {

        source(scope, "ToStream", |capability, info| {

            // Acquire an activator, so that the operator can rescheduled itself.
            let activator = scope.activator_for(&info.address[..]);

            let mut iterator = self.into_iter().fuse();
            let mut capability = Some(capability);

            move |output| {

                if let Some(element) = iterator.next() {
                    let mut session = output.session(capability.as_ref().unwrap());
                    session.give(element);
                    let n = 256 * crate::container::buffer::default_capacity::<I::Item>();
                    for element in iterator.by_ref().take(n - 1) {
                        session.give(element);
                    }
                    activator.activate();
                }
                else {
                    capability = None;
                }
            }
        })
    }
}
