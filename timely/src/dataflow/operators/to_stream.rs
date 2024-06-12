//! Conversion to the `Stream` type from iterators.

use crate::Data;
use crate::dataflow::{Stream, Scope};
use crate::dataflow::operators::core::{ToStream as ToStreamCore};

/// Converts to a timely `Stream`.
pub trait ToStream<D: Data> {
    /// Converts to a timely `Stream`.
    ///
    /// # Examples
    ///
    /// ```
    /// use timely::dataflow::operators::{ToStream, Capture};
    /// use timely::dataflow::operators::capture::Extract;
    ///
    /// let (data1, data2) = timely::example(|scope| {
    ///     let data1 = (0..3).to_stream(scope).capture();
    ///     let data2 = vec![0,1,2].to_stream(scope).capture();
    ///     (data1, data2)
    /// });
    ///
    /// assert_eq!(data1.extract(), data2.extract());
    /// ```
    fn to_stream<S: Scope>(self, scope: &mut S) -> Stream<S, D>;
}

impl<I: IntoIterator+'static> ToStream<I::Item> for I where I::Item: Data {
    fn to_stream<S: Scope>(self, scope: &mut S) -> Stream<S, I::Item> {
        ToStreamCore::to_stream(self, scope)
    }
}
