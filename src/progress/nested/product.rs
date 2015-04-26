use std::cmp::Ordering;
use std::fmt::{Formatter, Display, Error, Debug};

use progress::Timestamp;
use progress::nested::summary::Summary;

use columnar::Columnar;


#[derive(Copy, Clone, Hash, Eq, PartialEq, Default)]
pub struct Product<TOuter, TInner> {
    pub outer: TOuter,
    pub inner: TInner,
}

impl<TOuter, TInner> Product<TOuter, TInner> {
    pub fn new(outer: TOuter, inner: TInner) -> Product<TOuter, TInner> {
        Product {
            outer: outer,
            inner: inner,
        }
    }
}

impl<TOuter: Display, TInner: Display> Display for Product<TOuter, TInner> {
    fn fmt(&self, f: &mut Formatter) -> Result<(), Error> {
        f.write_str(&format!("({}, {})", self.outer, self.inner))
    }
}

impl<TOuter: Debug, TInner: Debug> Debug for Product<TOuter, TInner> {
    fn fmt(&self, f: &mut Formatter) -> Result<(), Error> {
        f.write_str(&format!("({:?}, {:?})", self.outer, self.inner))
    }
}

impl<TOuter: PartialOrd, TInner: PartialOrd> PartialOrd for Product<TOuter, TInner> {
    fn partial_cmp(&self, other: &Product<TOuter, TInner>) -> Option<Ordering> {
        if let Some(cmp1) = self.outer.partial_cmp(&other.outer) {
            if let Some(cmp2) = self.inner.partial_cmp(&other.inner) {

                // if both are comparable we may have a result, as long as not LT and GT
                if cmp1 == cmp2 { Some(cmp1) }
                else {
                    if cmp1 == Ordering::Equal { Some(cmp2) } else
                    if cmp2 == Ordering::Equal { Some(cmp1) } else
                    { None }
                }
            }
            else { None }
        }
        else { None }
    }
    fn le(&self, other: &Product<TOuter, TInner>) -> bool {
        self.outer <= other.outer && self.inner <= other.inner
    }
    fn ge(&self, other: &Product<TOuter, TInner>) -> bool {
        self.outer >= other.outer && self.inner >= other.inner
    }
}

impl<TOuter: Timestamp, TInner: Timestamp> Timestamp for Product<TOuter, TInner> {
    type Summary = Summary<TOuter::Summary, TInner::Summary>;
}

// columnar implementation because Product<T1, T2> : Copy.
impl<TOuter: Copy+Columnar, TInner: Copy+Columnar> Columnar for Product<TOuter, TInner> {
    type Stack = Vec<Product<TOuter, TInner>>;
}
