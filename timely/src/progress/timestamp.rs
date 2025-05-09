//! A partially ordered measure of progress at each timely dataflow location.

use std::fmt::Debug;
use std::any::Any;
use std::default::Default;
use std::hash::Hash;

use crate::ExchangeData;
use crate::order::PartialOrder;

/// A composite trait for types that serve as timestamps in timely dataflow.
///
/// By implementing this trait, you promise that the type's [PartialOrder] implementation
/// is compatible with [Ord], such that if `a.less_equal(b)` then `a <= b`.
pub trait Timestamp: Clone+Eq+PartialOrder+Debug+Send+Any+ExchangeData+Hash+Ord {
    /// A type summarizing action on a timestamp along a dataflow path.
    type Summary : PathSummary<Self> + 'static;
    /// A unique minimum value in our partial order.
    ///
    /// This value will often be used as an initial value, and should be cheap to construct.
    fn minimum() -> Self;
}

/// A summary of how a timestamp advances along a timely dataflow path.
pub trait PathSummary<T> : Clone+'static+Eq+PartialOrder+Debug+Default {
    /// Advances a timestamp according to the timestamp actions on the path.
    ///
    /// The path may advance the timestamp sufficiently that it is no longer valid, for example if
    /// incrementing fields would result in integer overflow. In this case, `results_in` should
    /// return `None`.
    ///
    /// The `feedback` operator, apparently the only point where timestamps are actually incremented
    /// in computation, uses this method and will drop messages with timestamps that when advanced
    /// result in `None`. Ideally, all other timestamp manipulation should behave similarly.
    ///
    /// This function must be monotonic increasing in both inputs.
    /// If `s1.less_equal(&s2)` then for all `t` we have `s1.results_in(&t).less_equal(&s2.results_in(&t))`.
    /// If `t1.less_equal(&t2)` then for all `s` we have `s.results_in(&t1).less_equal(&s.results_in(&t2))`.
    ///
    /// Note that `Self::default()` is expected to behave as an "empty" or "noop" summary, such that
    /// `Self::default().results_in(&t) == Some(t)`. The default summary does not need to be a minimal
    /// summary, in that summaries are technically permitted to walk timestamps backwards. Care should
    /// be used when doing this to avoid potentially cyclic dataflows without strict timestamp advancement.
    ///
    /// # Examples
    /// ```
    /// use timely::progress::timestamp::PathSummary;
    ///
    /// let timestamp = 3;
    ///
    /// let summary1 = 5;
    /// let summary2 = usize::MAX - 2;
    ///
    /// assert_eq!(summary1.results_in(&timestamp), Some(8));
    /// assert_eq!(summary2.results_in(&timestamp), None);
    /// ```
    fn results_in(&self, src: &T) -> Option<T>;
    /// Composes this path summary with another path summary.
    ///
    /// It is possible that the two composed paths result in an invalid summary, for example when
    /// integer additions overflow. If it is correct that all timestamps moved along these paths
    /// would also result in overflow and be discarded, `followed_by` can return `None`. It is very
    /// important that this not be used casually, as this does not prevent the actual movement of
    /// data.
    ///
    /// Calling `results_in` on the composed summary should behave the same as though the two
    /// summaries were applied to the argument in order.
    ///
    /// # Examples
    /// ```
    /// use timely::progress::timestamp::PathSummary;
    ///
    /// let summary1 = 5;
    /// let summary2 = usize::MAX - 3;
    ///
    /// assert_eq!(summary1.followed_by(&summary2), None);
    ///
    /// let time = 10;
    /// let summary2 = 15;
    /// assert_eq!(
    ///     // Applying the composed summary...
    ///     summary1.followed_by(&summary2).and_then(|s| s.results_in(&time)),
    ///     // ...has the same result as applying the two summaries in sequence.
    ///     summary1.results_in(&time).and_then(|t| summary2.results_in(&t)),
    /// );
    ///
    /// ```
    fn followed_by(&self, other: &Self) -> Option<Self>;
}

impl Timestamp for () { type Summary = (); fn minimum() -> Self { }}
impl PathSummary<()> for () {
    #[inline] fn results_in(&self, _src: &()) -> Option<()> { Some(()) }
    #[inline] fn followed_by(&self, _other: &()) -> Option<()> { Some(()) }
}

/// Implements [`Timestamp`] and [`PathSummary`] for types with a `checked_add` method.
macro_rules! implement_timestamp_add {
    ($($index_type:ty,)*) => (
        $(
            impl Timestamp for $index_type {
                type Summary = $index_type;
                fn minimum() -> Self { Self::MIN }
            }
            impl PathSummary<$index_type> for $index_type {
                #[inline]
                fn results_in(&self, src: &$index_type) -> Option<$index_type> { self.checked_add(*src) }
                #[inline]
                fn followed_by(&self, other: &$index_type) -> Option<$index_type> { self.checked_add(*other) }
            }
        )*
    )
}

implement_timestamp_add!(usize, u128, u64, u32, u16, u8, isize, i128, i64, i32, i16, i8,);

impl Timestamp for ::std::time::Duration {
    type Summary = ::std::time::Duration;
    fn minimum() -> Self { ::std::time::Duration::new(0, 0) }
}
impl PathSummary<::std::time::Duration> for ::std::time::Duration {
    #[inline]
    fn results_in(&self, src: &::std::time::Duration) -> Option<::std::time::Duration> { self.checked_add(*src) }
    #[inline]
    fn followed_by(&self, other: &::std::time::Duration) -> Option<::std::time::Duration> { self.checked_add(*other) }
}

pub use self::refines::Refines;
mod refines {

    use crate::progress::Timestamp;

    /// Conversion between pointstamp types.
    ///
    /// This trait is central to nested scopes, for which the inner timestamp must be
    /// related to the outer timestamp. These methods define those relationships.
    ///
    /// It would be ideal to use Rust's From and Into traits, but they seem to be messed
    /// up due to coherence: we can't implement `Into` because it induces a from implementation
    /// we can't control.
    pub trait Refines<T: Timestamp> : Timestamp {
        /// Converts the outer timestamp to an inner timestamp.
        fn to_inner(other: T) -> Self;
        /// Converts the inner timestamp to an outer timestamp.
        fn to_outer(self) -> T;
        /// Summarizes an inner path summary as an outer path summary.
        ///
        /// It is crucial for correctness that the result of this summarization's `results_in`
        /// method is equivalent to `|time| path.results_in(time.to_inner()).to_outer()`, or
        /// at least produces times less or equal to that result.
        fn summarize(path: <Self as Timestamp>::Summary) -> <T as Timestamp>::Summary;
    }

    /// All types "refine" themselves,
    impl<T: Timestamp> Refines<T> for T {
        fn to_inner(other: T) -> T { other }
        fn to_outer(self) -> T { self }
        fn summarize(path: <T as Timestamp>::Summary) -> <T as Timestamp>::Summary { path }
    }

    /// Implements `Refines<()>` for most types.
    ///
    /// We have a macro here because a blanket implement would conflict with the "refines self"
    /// blanket implementation just above. Waiting on specialization to fix that, I guess.
    macro_rules! implement_refines_empty {
        ($($index_type:ty,)*) => (
            $(
                impl Refines<()> for $index_type {
                    fn to_inner(_: ()) -> $index_type { Default::default() }
                    fn to_outer(self) -> () { }
                    fn summarize(_: <$index_type as Timestamp>::Summary) -> () { }
                }
            )*
        )
    }

    implement_refines_empty!(usize, u128, u64, u32, u16, u8, isize, i128, i64, i32, i16, i8, ::std::time::Duration,);
}
