//! Filters and maps a [`Stream`] using the given predicate

use crate::{
    dataflow::{channels::pact::Pipeline, operators::Operator, Scope, Stream},
    Data,
};

/// Filters and maps a [`Stream`] at the same time
pub trait FilterMap<D, D2> {
    /// The output type of the operator
    type Output;

    /// An operator that both filters and maps a [`Stream`].
    ///
    /// The returned stream contains only the values for which the supplied closure returns `Some(value)`.
    ///
    /// # Examples
    ///
    /// ```
    /// use timely::dataflow::operators::{ToStream, FilterMap, Inspect};
    ///
    /// timely::example(|scope| {
    ///     let filtered = ["10", "11", "12", "invalid number"]
    ///            .to_stream(scope)
    ///            .filter_map(|x| x.parse::<i32>().ok())
    ///            .inspect(|x| println!("valid number: {:?}", x));
    ///
    ///     // The expected contents of the `filtered` stream
    ///     let expected = [10, 11, 12].to_stream(scope);
    ///
    ///     filtered.assert_eq(&expected);
    /// });
    /// ```
    ///
    fn filter_map<L>(&self, logic: L) -> Self::Output
    where
        L: FnMut(D) -> Option<D2> + 'static,
    {
        self.filter_map_named("FilterMap", logic)
    }

    /// An operator that both filters and maps a [`Stream`] with the additional ability
    /// to rename the operator.
    ///
    /// The returned stream contains only the values for which the supplied closure returns `Some(value)`.
    ///
    /// # Examples
    ///
    /// ```
    /// use timely::dataflow::operators::{ToStream, FilterMap, Inspect};
    ///
    /// timely::example(|scope| {
    ///     let filtered = ["10", "11", "12", "invalid number"]
    ///            .to_stream(scope)
    ///            .filter_map_named("MySpecialFilterMap", |x| x.parse::<i32>().ok())
    ///            .inspect(|x| println!("valid number: {:?}", x));
    ///
    ///     // The expected contents of the `filtered` stream
    ///     let expected = [10, 11, 12].to_stream(scope);
    ///
    ///     filtered.assert_eq(&expected);
    /// });
    /// ```
    ///
    fn filter_map_named<L>(&self, name: &str, logic: L) -> Self::Output
    where
        L: FnMut(D) -> Option<D2> + 'static;
}

impl<S, D, D2> FilterMap<D, D2> for Stream<S, D>
where
    S: Scope,
    D: Data,
    D2: Data,
{
    type Output = Stream<S, D2>;

    fn filter_map_named<L>(&self, name: &str, mut logic: L) -> Self::Output
    where
        L: FnMut(D) -> Option<D2> + 'static,
    {
        let mut buffer = Vec::new();

        self.unary(Pipeline, name, move |_capability, _info| {
            move |input, output| {
                input.for_each(|capability, data| {
                    data.swap(&mut buffer);

                    output
                        .session(&capability)
                        .give_iterator(buffer.drain(..).filter_map(|data| logic(data)));
                });
            }
        })
    }
}
