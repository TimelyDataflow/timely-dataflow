//! Path summaries that are either child local, or leave the scope and re-enter from the parent.

// use std::cmp::Ordering;
use std::default::Default;
use std::fmt::{Display, Formatter};
use std::fmt::Error;

use order::PartialOrder;
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

// Two summaries are comparable if they are the same type (Local or Outer). If they have different types, 
// then there is no way to order them, as Local leaves the outer coordinate unincremented, and Outer has
// a new inner coordinate that it installs, rather than advancing the existing inner coordinate.
//
// Two local summaries are ordered if their inner summaries are ordered.
// 
// Two outer summaries are ordered if their outer and inner summaries are ordered. 
//
// Note: I think the previous implementation, which compared (s1,t1) to (s2,t2), was incorrect. The order
// on tuples is lexicographic, so this would put a small outer summary and large inner installment ahead
// of a larger outer summary with small inner installment. I don't think that is correct (they should be
// unordered.
impl<S: PartialOrder, T: PartialOrder> PartialOrder for Summary<S, T> {
    #[inline(always)]
    fn less_equal(&self, other: &Self) -> bool {
        match (self, other) {
            (&Local(ref t1), &Local(ref t2)) => t1.less_equal(t2),
            (&Outer(ref s1, ref t1), &Outer(ref s2, ref t2)) => s1.less_equal(s2) && t1.less_equal(t2),
            _  => false
        }
    }
}

// impl<S:PartialOrder+Copy, T:PartialOrder+Copy> PartialOrder for Summary<S, T> {
//     #[inline]
//     fn partial_cmp(&self, other: &Summary<S, T>) -> Option<Ordering> {
//         // Two summaries are comparable if they are of the same type (Local, Outer).
//         // Otherwise, as Local *updates* and Outer *sets* the inner coordinate, we 
//         // cannot be sure that either strictly improves on the other.
//         match (*self, *other) {
//             (Local(t1),    Local(t2))    => t1.partial_cmp(&t2),
//             (Outer(s1,t1), Outer(s2,t2)) => (s1,t1).partial_cmp(&(s2,t2)),
//             (Local(_),     Outer(_,_))   |
//             (Outer(_,_),   Local(_))     => None,
//         }
//     }
// }

impl<TOuter, SOuter, TInner, SInner> PathSummary<Product<TOuter, TInner>> for Summary<SOuter, SInner>
where TOuter: Timestamp,
      TInner: Timestamp,
      SOuter: PathSummary<TOuter>,
      SInner: PathSummary<TInner>,
{
    // this makes sense for a total order, but less clear for a partial order.
    #[inline]
    fn results_in(&self, product: &Product<TOuter, TInner>) -> Option<Product<TOuter, TInner>> {
        match *self {
            Local(ref iters)              => iters.results_in(&product.inner).map(|x| Product::new(product.outer.clone(), x)),
            Outer(ref summary, ref iters) => summary.results_in(&product.outer).map(|x| Product::new(x, iters.results_in(&Default::default()).unwrap())),
        }
    }
    #[inline]
    fn followed_by(&self, other: &Summary<SOuter, SInner>) -> Option<Summary<SOuter, SInner>> {
        match (self, other) {
            (&Local(ref inner1), &Local(ref inner2))                 => inner1.followed_by(inner2).map(|x| Local(x)),
            (&Local(_), &Outer(_, _))                                => Some(other.clone()),
            (&Outer(ref outer1, ref inner1), &Local(ref inner2))     => inner1.followed_by(inner2).map(|x| Outer(outer1.clone(), x)),
            (&Outer(ref outer1, _), &Outer(ref outer2, ref inner2))  => outer1.followed_by(outer2).map(|x| Outer(x, inner2.clone())),
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
