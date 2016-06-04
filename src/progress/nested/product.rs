//! A pair timestamp suitable for use with the product partial order.

use std::cmp::Ordering;
use std::fmt::{Formatter, Error, Debug};

use progress::Timestamp;
use progress::nested::summary::Summary;

use abomonation::Abomonation;

/// A nested pair of timestamps, one outer and one inner.
///
/// We use `Product` rather than `(TOuter, TInner)` so that we can derive our own `PartialOrd`,
/// because Rust just uses the lexicographic total order.
#[derive(Copy, Clone, Hash, Eq, PartialEq, Default, Ord)]
pub struct Product<TOuter, TInner> {
    /// Outer timestamp.
    pub outer: TOuter,
    /// Inner timestamp.
    pub inner: TInner,
}

impl<TOuter, TInner> Product<TOuter, TInner> {
    /// Creates a new product from outer and inner coordinates.
    pub fn new(outer: TOuter, inner: TInner) -> Product<TOuter, TInner> {
        Product {
            outer: outer,
            inner: inner,
        }
    }
}

/// Debug implementation to avoid seeing fully qualified path names.
impl<TOuter: Debug, TInner: Debug> Debug for Product<TOuter, TInner> {
    fn fmt(&self, f: &mut Formatter) -> Result<(), Error> {
        f.write_str(&format!("({:?}, {:?})", self.outer, self.inner))
    }
}

impl<TOuter: PartialOrd, TInner: PartialOrd> PartialOrd for Product<TOuter, TInner> {
    #[inline(always)]
    fn partial_cmp(&self, other: &Product<TOuter, TInner>) -> Option<Ordering> {
        match (self <= other, other <= self) {
            (true, true)   => Some(Ordering::Equal),
            (true, false)  => Some(Ordering::Less),
            (false, true)  => Some(Ordering::Greater),
            (false, false) => None,
        }
    }
    #[inline(always)]
    fn le(&self, other: &Product<TOuter, TInner>) -> bool {
        self.inner <= other.inner && self.outer <= other.outer
    }
    #[inline(always)]
    fn ge(&self, other: &Product<TOuter, TInner>) -> bool {
        self.inner >= other.inner && self.outer >= other.outer
    }
}

impl<TOuter: Timestamp, TInner: Timestamp> Timestamp for Product<TOuter, TInner> {
    type Summary = Summary<TOuter::Summary, TInner::Summary>;
}

impl<TOuter: Abomonation, TInner: Abomonation> Abomonation for Product<TOuter, TInner> {
    unsafe fn embalm(&mut self) { self.outer.embalm(); self.inner.embalm(); }
    unsafe fn entomb(&self, bytes: &mut Vec<u8>) { self.outer.entomb(bytes); self.inner.entomb(bytes); }
    unsafe fn exhume<'a,'b>(&'a mut self, mut bytes: &'b mut [u8]) -> Option<&'b mut [u8]> {
        let tmp = bytes; bytes = if let Some(bytes) = self.outer.exhume(tmp) { bytes } else { return None };
        let tmp = bytes; bytes = if let Some(bytes) = self.inner.exhume(tmp) { bytes } else { return None };
        Some(bytes)
    }
}
