//! Conversion to the `StreamVec` type from iterators.

use crate::dataflow::{StreamVec, Scope};
use crate::dataflow::operators::core::{ToStream as ToStreamCore};

/// Converts to a timely `StreamVec`.
pub trait ToStream<D: 'static> {
    /// Converts to a timely `StreamVec`.
    ///
    /// # Examples
    ///
    /// ```
    /// use timely::dataflow::operators::{ToStream, Capture};
    /// use timely::dataflow::operators::capture::Extract;
    ///
    /// let (data1, data2) = timely::example(|scope| {
    ///     let data1 = (0..3).to_stream(scope).container::<Vec<_>>().capture();
    ///     let data2 = vec![0,1,2].to_stream(scope).container::<Vec<_>>().capture();
    ///     (data1, data2)
    /// });
    ///
    /// assert_eq!(data1.extract(), data2.extract());
    /// ```
    fn to_stream<S: Scope>(self, scope: &mut S) -> StreamVec<S, D>;
}

impl<I: IntoIterator+'static> ToStream<I::Item> for I {
    fn to_stream<S: Scope>(self, scope: &mut S) -> StreamVec<S, I::Item> {
        ToStreamCore::to_stream(self, scope)
    }
}
