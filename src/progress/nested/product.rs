//! A product-order timestamp combinator

use std::fmt::{Formatter, Error, Debug};

use ::order::{PartialOrder, TotalOrder};
use progress::Timestamp;
use progress::timestamp::Refines;

impl<TOuter: Timestamp, TInner: Timestamp> Refines<TOuter> for Product<TOuter, TInner> {
    fn to_inner(other: TOuter) -> Self {
        Product::new(other, Default::default())
    }
    fn to_outer(self: Product<TOuter, TInner>) -> TOuter {
        self.outer
    }
    fn summarize(path: <Self as Timestamp>::Summary) -> <TOuter as Timestamp>::Summary {
        path.outer
    }
}

/// A nested pair of timestamps, one outer and one inner.
///
/// We use `Product` rather than `(TOuter, TInner)` so that we can derive our own `PartialOrd`,
/// because Rust just uses the lexicographic total order.
#[derive(Abomonation, Copy, Clone, Hash, Eq, PartialEq, Default, Ord, PartialOrd, Serialize, Deserialize)]
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
            outer,
            inner,
        }
    }
}

/// Debug implementation to avoid seeing fully qualified path names.
impl<TOuter: Debug, TInner: Debug> Debug for Product<TOuter, TInner> {
    fn fmt(&self, f: &mut Formatter) -> Result<(), Error> {
        f.write_str(&format!("({:?}, {:?})", self.outer, self.inner))
    }
}

impl<TOuter: PartialOrder, TInner: PartialOrder> PartialOrder for Product<TOuter, TInner> {
    #[inline(always)]
    fn less_equal(&self, other: &Self) -> bool {
        self.outer.less_equal(&other.outer) && self.inner.less_equal(&other.inner)
    }
}

impl<TOuter: Timestamp, TInner: Timestamp> Timestamp for Product<TOuter, TInner> {
    type Summary = Product<TOuter::Summary, TInner::Summary>;
}

use progress::timestamp::PathSummary;
impl<TOuter: Timestamp, TInner: Timestamp> PathSummary<Product<TOuter, TInner>> for Product<TOuter::Summary, TInner::Summary> {
    #[inline]
    fn results_in(&self, product: &Product<TOuter, TInner>) -> Option<Product<TOuter, TInner>> {
        self.outer.results_in(&product.outer)
            .and_then(|outer|
                self.inner.results_in(&product.inner)
                    .map(|inner| Product::new(outer, inner))
            )
    }
    #[inline]
    fn followed_by(&self, other: &Product<TOuter::Summary, TInner::Summary>) -> Option<Product<TOuter::Summary, TInner::Summary>> {
        self.outer.followed_by(&other.outer)
            .and_then(|outer|
                self.inner.followed_by(&other.inner)
                    .map(|inner| Product::new(outer, inner))
            )
    }
}

/// A type that does not affect total orderedness.
///
/// This trait is not useful, but must be made public and documented or else Rust
/// complains about its existence in the constraints on the implementation of
/// public traits for public types.
pub trait Empty : PartialOrder { }

impl Empty for () { }
impl<T1: Empty, T2: Empty> Empty for Product<T1, T2> { }

impl<T1, T2> TotalOrder for Product<T1, T2> where T1: Empty, T2: TotalOrder { }
