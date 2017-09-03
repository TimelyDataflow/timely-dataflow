//! Traits and types for partially ordered sets.

/// A type that is partially ordered.
///
/// This trait is distinct from Rust's `PartialOrd` trait, because the implementation
/// of that trait precludes a distinct `Ord` implementation. We need an independent 
/// trait if we want to have a partially ordered type that can also be sorted.
pub trait PartialOrder : Eq {
    /// Returns true iff one element is strictly less than the other.
    fn less_than(&self, other: &Self) -> bool {
        self.less_equal(other) && self != other
    }
    /// Returns true iff one element is less than or equal to the other.
    fn less_equal(&self, other: &Self) -> bool;
}

/// A type that is totally ordered.
///
/// This trait is a "carrier trait", in the sense that it adds no additional functionality
/// over `PartialOrder`, but instead indicates that the `less_than` and `less_equal` methods
/// are total, meaning that `x.less_than(&y)` is equivalent to `!y.less_equal(&x)`.
///
/// This trait is distinct from Rust's `Ord` trait, because several implementors of
/// `PartialOrd` also implement `Ord` for efficient canonicalization, deduplication, 
/// and other sanity-maintaining operations.
pub trait TotalOrder : PartialOrder { }

macro_rules! implement_partial {
    ($($index_type:ty,)*) => (
        $(
            impl PartialOrder for $index_type {
                #[inline(always)] fn less_than(&self, other: &Self) -> bool { self < other }
                #[inline(always)] fn less_equal(&self, other: &Self) -> bool { self <= other }
            }
        )*
    )
}

macro_rules! implement_total {
    ($($index_type:ty,)*) => (
        $(
            impl TotalOrder for $index_type { }
        )*
    )
}

implement_partial!(u8, u16, u32, u64, usize, i8, i16, i32, i64, isize, (),);
implement_total!(u8, u16, u32, u64, usize, i8, i16, i32, i64, isize, (),);