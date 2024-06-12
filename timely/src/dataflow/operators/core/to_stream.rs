//! Conversion to the `StreamCore` type from iterators.

use crate::container::{CapacityContainerBuilder, ContainerBuilder, SizableContainer, PushInto};
use crate::Container;
use crate::dataflow::operators::generic::operator::source;
use crate::dataflow::{StreamCore, Scope};

/// Converts to a timely [StreamCore], using a container builder.
pub trait ToStreamBuilder<CB: ContainerBuilder> {
    /// Converts to a timely [StreamCore], using the supplied container builder type.
    ///
    /// # Examples
    ///
    /// ```
    /// use timely::dataflow::operators::core::{ToStreamBuilder, Capture};
    /// use timely::dataflow::operators::core::capture::Extract;
    /// use timely::container::CapacityContainerBuilder;
    ///
    /// let (data1, data2) = timely::example(|scope| {
    ///     let data1 = ToStreamBuilder::<CapacityContainerBuilder<_>>::to_stream_with_builder(0..3, scope)
    ///         .container::<Vec<_>>()
    ///         .capture();
    ///     let data2 = ToStreamBuilder::<CapacityContainerBuilder<_>>::to_stream_with_builder(vec![0,1,2], scope)
    ///         .container::<Vec<_>>()
    ///         .capture();
    ///     (data1, data2)
    /// });
    ///
    /// assert_eq!(data1.extract(), data2.extract());
    /// ```
    fn to_stream_with_builder<S: Scope>(self, scope: &mut S) -> StreamCore<S, CB::Container>;
}

impl<CB: ContainerBuilder, I: IntoIterator+'static> ToStreamBuilder<CB> for I where CB: PushInto<I::Item> {
    fn to_stream_with_builder<S: Scope>(self, scope: &mut S) -> StreamCore<S, CB::Container> {

        source::<_, CB, _, _>(scope, "ToStreamBuilder", |capability, info| {

            // Acquire an activator, so that the operator can rescheduled itself.
            let activator = scope.activator_for(&info.address[..]);

            let mut iterator = self.into_iter().fuse();
            let mut capability = Some(capability);

            move |output| {

                if let Some(element) = iterator.next() {
                    let mut session = output.session_with_builder(capability.as_ref().unwrap());
                    session.give(element);
                    let n = 256 * crate::container::buffer::default_capacity::<I::Item>();
                    session.give_iterator(iterator.by_ref().take(n - 1));
                    activator.activate();
                }
                else {
                    capability = None;
                }
            }
        })
    }
}

/// Converts to a timely [StreamCore]. Equivalent to [`ToStreamBuilder`] but
/// uses a [`CapacityContainerBuilder`].
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

impl<C: SizableContainer, I: IntoIterator+'static> ToStream<C> for I where C: PushInto<I::Item> {
    fn to_stream<S: Scope>(self, scope: &mut S) -> StreamCore<S, C> {
        ToStreamBuilder::<CapacityContainerBuilder<C>>::to_stream_with_builder(self, scope)
    }
}
