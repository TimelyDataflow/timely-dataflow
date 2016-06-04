//! Path summaries that are either child local, or leave the scope and re-enter from the parent.

use std::cmp::Ordering;
use std::default::Default;
use std::fmt::{Display, Formatter};
use std::fmt::Error;

use progress::{Timestamp, PathSummary};
use progress::nested::product::Product;
use progress::nested::summary::Summary::{Local, Outer};

/// Summarizes a path within a scope.
///
/// The path summary can either be entirely local to the scope (`Local`) or it may leave
/// the scope and return (`Outer`).
#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub enum Summary<S, T> {
    /// Reachable within the scope, and a summary adjusting only the inner coordinate.
    Local(T),
    /// Reachable outside the scope, with adjustments to both coordinates.
    Outer(S, T),
}

impl<S, T: Default> Default for Summary<S, T> {
    fn default() -> Summary<S, T> { Local(Default::default()) }
}

impl<S:PartialOrd+Copy, T:PartialOrd+Copy> PartialOrd for Summary<S, T> {
    #[inline]
    fn partial_cmp(&self, other: &Summary<S, T>) -> Option<Ordering> {
        // Two summaries are comparable if they are of the same type (Local, Outer).
        // Otherwise, as Local *updates* and Outer *sets* the inner coordinate, we 
        // cannot be sure that either strictly improves on the other.
        match (*self, *other) {
            (Local(t1),    Local(t2))    => t1.partial_cmp(&t2),
            (Outer(s1,t1), Outer(s2,t2)) => (s1,t1).partial_cmp(&(s2,t2)),
            (Local(_),     Outer(_,_))   |
            (Outer(_,_),   Local(_))     => None,
        }
    }
}

impl<TOuter, SOuter, TInner, SInner> PathSummary<Product<TOuter, TInner>> for Summary<SOuter, SInner>
where TOuter: Timestamp,
      TInner: Timestamp,
      SOuter: PathSummary<TOuter>,
      SInner: PathSummary<TInner>,
{
    // this makes sense for a total order, but less clear for a partial order.
    #[inline]
    fn results_in(&self, product: &Product<TOuter, TInner>) -> Product<TOuter, TInner> {
        match *self {
            Local(ref iters)              => Product::new(product.outer, iters.results_in(&product.inner)),
            Outer(ref summary, ref iters) => Product::new(summary.results_in(&product.outer), iters.results_in(&Default::default())),
        }
    }
    #[inline]
    fn followed_by(&self, other: &Summary<SOuter, SInner>) -> Summary<SOuter, SInner> {
        match (*self, *other) {
            (Local(inner1), Local(inner2))             => Local(inner1.followed_by(&inner2)),
            (Local(_), Outer(_, _))                    => *other,
            (Outer(outer1, inner1), Local(inner2))     => Outer(outer1, inner1.followed_by(&inner2)),
            (Outer(outer1, _), Outer(outer2, inner2))  => Outer(outer1.followed_by(&outer2), inner2),
        }
    }
}

impl<SOuter: Display, SInner: Display> Display for Summary<SOuter, SInner> {
    #[inline]
    fn fmt(&self, f: &mut Formatter) -> Result<(), Error> {
        match *self {
            Local(ref s) => write!(f, "Local({})", s),
            Outer(ref s, ref t) => write!(f, "Outer({}, {})", s, t)
        }
    }
}
