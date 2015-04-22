use std::cmp::Ordering;
use std::default::Default;
use std::fmt::{Display, Formatter};
use std::fmt::Error;


use progress::{Timestamp, PathSummary};
use progress::nested::product::Product;
use progress::nested::summary::Summary::{Local, Outer};

#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub enum Summary<S, T> {
    Local(T),    // reachable within inner scope.
    Outer(S, T), // unreachable within scope, reachable through outer scope and then inner.
}

impl<S, T: Default> Default for Summary<S, T> {
    fn default() -> Summary<S, T> { Local(Default::default()) }
}

impl<S:PartialOrd+Copy, T:PartialOrd+Copy> PartialOrd for Summary<S, T> {
    fn partial_cmp(&self, other: &Summary<S, T>) -> Option<Ordering> {
        match (*self, *other) {
            (Local(t1),    Local(t2))    => t1.partial_cmp(&t2),
            (Local(t1),    Outer(_,t2))  => match t1.partial_cmp(&t2) {
                Some(Ordering::Less)    => Some(Ordering::Less),
                Some(Ordering::Equal)   => Some(Ordering::Less), // might not be strict if s2~=0, but lets break ties
                Some(Ordering::Greater) => None,                 // if s2 = 0 we could be leaving something else out
                None                    => None,
            },
            (Outer(s1,t1), Outer(s2,t2)) => (s1,t1).partial_cmp(&(s2,t2)),
            (Outer(_,t1),   Local(t2))   => match t1.partial_cmp(&t2) {
                Some(Ordering::Less)    => None,
                Some(Ordering::Equal)   => Some(Ordering::Greater), // might not be strict if s2~=0, but lets break ties
                Some(Ordering::Greater) => Some(Ordering::Greater), // if s2 = 0 we could be leaving something else out
                None                    => None,
            },
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
    fn results_in(&self, product: &Product<TOuter, TInner>) -> Product<TOuter, TInner> {
        match *self {
            Local(ref iters)              => Product::new(product.outer.clone(), iters.results_in(&product.inner)),
            Outer(ref summary, ref iters) => Product::new(summary.results_in(&product.outer), iters.results_in(&Default::default())),
        }
    }
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
    fn fmt(&self, f: &mut Formatter) -> Result<(), Error> {
        match self {
            &Local(ref s) => f.write_str(&format!("Local({})", s)),
            &Outer(ref s, ref t) => f.write_str(&format!("Outer({}, {})", s, t))
        }
    }
}
