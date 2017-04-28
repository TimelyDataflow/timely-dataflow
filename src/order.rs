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

impl PartialOrder for u8 { #[inline(always)] fn less_equal(&self, other: &Self) -> bool { *self <= *other } }
impl PartialOrder for u16 { #[inline(always)] fn less_equal(&self, other: &Self) -> bool { *self <= *other } }
impl PartialOrder for u32 { #[inline(always)] fn less_equal(&self, other: &Self) -> bool { *self <= *other } }
impl PartialOrder for u64 { #[inline(always)] fn less_equal(&self, other: &Self) -> bool { *self <= *other } }
impl PartialOrder for usize { #[inline(always)] fn less_equal(&self, other: &Self) -> bool { *self <= *other } }

impl PartialOrder for i8 { #[inline(always)] fn less_equal(&self, other: &Self) -> bool { *self <= *other } }
impl PartialOrder for i16 { #[inline(always)] fn less_equal(&self, other: &Self) -> bool { *self <= *other } }
impl PartialOrder for i32 { #[inline(always)] fn less_equal(&self, other: &Self) -> bool { *self <= *other } }
impl PartialOrder for i64 { #[inline(always)] fn less_equal(&self, other: &Self) -> bool { *self <= *other } }
impl PartialOrder for isize { #[inline(always)] fn less_equal(&self, other: &Self) -> bool { *self <= *other } }

impl PartialOrder for () { #[inline(always)] fn less_equal(&self, _other: &Self) -> bool { true } }
