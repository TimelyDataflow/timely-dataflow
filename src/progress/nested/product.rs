use std::cmp::Ordering;
use std::fmt::{Formatter, Display, Error, Debug};

use progress::Timestamp;
use progress::nested::summary::Summary;

// use columnar::Columnar;
use abomonation::Abomonation;


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
    #[inline(always)]
    fn partial_cmp(&self, other: &Product<TOuter, TInner>) -> Option<Ordering> {
        match (self <= other, self >= other) {
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

// // columnar implementation because Product<T1, T2> : Copy.
// impl<TOuter: Copy+Columnar, TInner: Copy+Columnar> Columnar for Product<TOuter, TInner> {
//     type Stack = Vec<Product<TOuter, TInner>>;
// }

impl<TOuter: Abomonation, TInner: Abomonation> Abomonation for Product<TOuter, TInner> {
    unsafe fn embalm(&mut self) { self.outer.embalm(); self.inner.embalm(); }
    unsafe fn entomb(&self, bytes: &mut Vec<u8>) { self.outer.entomb(bytes); self.inner.entomb(bytes); }
    unsafe fn exhume<'a,'b>(&'a mut self, mut bytes: &'b mut [u8]) -> Option<&'b mut [u8]> {
        let tmp = bytes; bytes = if let Some(bytes) = self.outer.exhume(tmp) { bytes } else { return None };
        let tmp = bytes; bytes = if let Some(bytes) = self.inner.exhume(tmp) { bytes } else { return None };
        Some(bytes)
    }
    fn verify<'a,'b>(&'a self, mut bytes: &'b [u8]) -> Option<&'b [u8]> {
        let tmp = bytes; bytes = if let Some(bytes) = self.outer.verify(tmp) { bytes } else { return None };
        let tmp = bytes; bytes = if let Some(bytes) = self.inner.verify(tmp) { bytes } else { return None };
        Some(bytes)
    }
}
