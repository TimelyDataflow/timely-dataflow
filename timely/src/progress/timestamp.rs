//! A partially ordered measure of progress at each timely dataflow location.

use std::fmt::Debug;
use std::any::Any;
use std::default::Default;
use std::hash::Hash;

use crate::communication::Data;
use crate::order::{PartialOrder, TotalOrder};

/// A composite trait for types that serve as timestamps in timely dataflow.
pub trait Timestamp: Clone+Eq+PartialOrder+Debug+Send+Any+Data+Hash+Ord {
    /// A type storing timestamps.
    type Container: TimestampContainer<Self> + Debug;
    /// A type summarizing action on a timestamp along a dataflow path.
    type Summary : Timestamp + PathSummary<Self> + 'static;
    /// A minimum value suitable as a default.
    fn minimum() -> Self;
}

/// A type to store timestamps. This serves as a means to avoid heap-allocating space for
/// totally ordered timestamps, where we know that a frontier is either empty or has exactly
/// one element.
pub trait TimestampContainer<T>: Default + Clone {
    /// Create this container with the specified capacity hint.
    fn with_capacity(_capacity: usize) -> Self { Default::default()}
    /// Create this container from the supplied element.
    fn from_element(element: T) -> Self;
    /// Create this container from an iterator of elements.
    fn from_iter<I: IntoIterator<Item=T>>(iterator: I) -> Self {
        let iter = iterator.into_iter();
        let mut this = Self::with_capacity(iter.size_hint().0);
        this.extend(iter);
        this
    }
    /// Remove all elements from this container, potentially leaving allocations in place.
    fn clear(&mut self);
    /// Sort the elements of this container.
    fn sort(&mut self) where T: Ord;

    /// Insert a single element into this container while maintaining the frontier invariants.
    /// Returns whether the container changed because of the insert.
    fn insert(&mut self, element: T) -> bool;

    /// Extend the container by individually inserting the elments. Returns whether the container
    /// changed.
    fn extend<I: IntoIterator<Item=T>>(&mut self, iterator: I) -> bool {
        let mut added = false;
        for element in iterator {
            added = self.insert(element) || added;
        }
        added
    }
    /// Reserve space for additional elements.
    fn reserve(&mut self, _additional: usize) {}
    /// Test if this container is less than the provided `time`.
    fn less_than(&self, time: &T) -> bool;
    /// Test if this container is less or equal the provided `time`.
    fn less_equal(&self, time: &T) -> bool;
    /// Represent this container as a reference to a slice.
    fn as_ref(&self) -> &[T];
    /// Convert to the at most one element the antichain contains.
    fn into_option(self) -> Option<T>;
    /// Return a reference to the at most one element the antichain contains.
    fn as_option(&self) -> Option<&T>;
    /// Convert this container into a vector.
    fn into_vec(self) -> Vec<T>;
}

impl<T: TotalOrder + Clone> TimestampContainer<T> for Option<T> {
    fn from_element(element: T) -> Self { Some(element) }
    fn insert(&mut self, element: T) -> bool {
        match self {
            Some(e) if !e.less_equal(&element) => {
                *self = Some(element);
                true
            }
            None => {
                *self = Some(element);
                true
            }
            _ => false,
        }
    }
    fn clear(&mut self) {
        *self = None
    }
    fn sort(&mut self) where T: Ord {
    }

    fn less_than(&self, time: &T) -> bool {
        self.as_ref().map(|x| x.less_than(time)).unwrap_or(false)
    }

    fn less_equal(&self, time: &T) -> bool {
        self.as_ref().map(|x| x.less_equal(time)).unwrap_or(false)
    }

    fn as_ref(&self) -> &[T] {
        match self {
            None => &[],
            Some(element) => std::slice::from_ref(element),
        }
    }
    fn into_option(self) -> Option<T> {
        self
    }
    fn as_option(&self) -> Option<&T> {
        self.as_ref()
    }
    fn into_vec(self) -> Vec<T> {
        match self {
            None => vec![],
            Some(element) => vec![element],
        }
    }
}

impl<T: PartialOrder + Clone> TimestampContainer<T> for Vec<T> {
    fn from_element(element: T) -> Self { vec![element] }
    fn with_capacity(capacity: usize) -> Self {
        Vec::with_capacity(capacity)
    }
    fn insert(&mut self, element: T) -> bool {
        if !self.iter().any(|x| x.less_equal(&element)) {
            self.retain(|x| !element.less_equal(x));
            self.push(element);
            true
        } else {
            false
        }
    }
    fn clear(&mut self) {
        self.clear()
    }
    fn sort(&mut self) where T: Ord {
        <[T]>::sort(self)
    }

    fn less_than(&self, time: &T) -> bool {
        self.iter().any(|x| x.less_than(time))
    }

    fn less_equal(&self, time: &T) -> bool {
        self.iter().any(|x| x.less_equal(time))
    }
    fn as_ref(&self) -> &[T] {
        &self
    }
    fn into_option(mut self) -> Option<T> {
        debug_assert!(self.len() <= 1);
        self.pop()
    }
    fn as_option(&self) -> Option<&T> {
        debug_assert!(self.len() <= 1);
        self.last()
    }
    fn into_vec(self) -> Vec<T> {
        self
    }
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
    /// # Examples
    /// ```
    /// use timely::progress::timestamp::PathSummary;
    ///
    /// let timestamp = 3;
    ///
    /// let summary1 = 5;
    /// let summary2 = usize::max_value() - 2;
    ///
    /// assert_eq!(summary1.results_in(&timestamp), Some(8));
    /// assert_eq!(summary2.results_in(&timestamp), None);
    /// ```
    fn results_in(&self, src: &T) -> Option<T>;
    /// Composes this path summary with another path summary.
    ///
    /// It is possible that the two composed paths result in an invalid summary, for example when
    /// integer additions overflow. If it is correct that all timestamps moved along these paths
    /// would also result in overflow and be discarded, `followed_by` can return `None. It is very
    /// important that this not be used casually, as this does not prevent the actual movement of
    /// data.
    ///
    /// # Examples
    /// ```
    /// use timely::progress::timestamp::PathSummary;
    ///
    /// let summary1 = 5;
    /// let summary2 = usize::max_value() - 3;
    ///
    /// assert_eq!(summary1.followed_by(&summary2), None);
    /// ```
    fn followed_by(&self, other: &Self) -> Option<Self>;
}

impl Timestamp for () { type Summary = (); type Container = Option<()>; fn minimum() -> Self { () }}
impl PathSummary<()> for () {
    #[inline] fn results_in(&self, _src: &()) -> Option<()> { Some(()) }
    #[inline] fn followed_by(&self, _other: &()) -> Option<()> { Some(()) }
}

/// Implements Timestamp and PathSummary for types with a `checked_add` method.
macro_rules! implement_timestamp_add {
    ($($index_type:ty,)*) => (
        $(
            impl Timestamp for $index_type {
                type Summary = $index_type;
                type Container = Option<$index_type>;
                fn minimum() -> Self { Self::min_value() }
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
    type Container = Option<::std::time::Duration>;
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
                    fn to_outer(self) -> () { () }
                    fn summarize(_: <$index_type as Timestamp>::Summary) -> () { () }
                }
            )*
        )
    }

    implement_refines_empty!(usize, u128, u64, u32, u16, u8, isize, i128, i64, i32, i16, i8, ::std::time::Duration,);
}
