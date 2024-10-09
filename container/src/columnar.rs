//! Present a columnar container as a timely container.

use serde::{Serialize, Deserialize};

pub use columnar::*;
use columnar::common::IterOwn;

use crate::{Container, SizableContainer, PushInto};

/// A container based on a `columnar` store.
#[derive(Clone, Default, Serialize, Deserialize)]
pub struct Columnar<C> {
    store: C,
}

impl<C: Len + Clear + Clone + Default + 'static> Container for Columnar<C> 
where
    for<'a> &'a C: columnar::Index,
{
    fn len(&self) -> usize { self.store.len() }
    fn clear(&mut self) { self.store.clear() }

    type ItemRef<'a> = <&'a C as Index>::Ref where Self: 'a;
    type Iter<'a> = IterOwn<&'a C>;
    fn iter<'a>(&'a self) -> Self::Iter<'a> { (&self.store).into_iter() }

    type Item<'a> = <&'a C as Index>::Ref where Self: 'a;
    type DrainIter<'a> = IterOwn<&'a C>;
    fn drain<'a>(&'a mut self) -> Self::DrainIter<'a> { (&self.store).into_iter() }
}

impl<C: Len + Clear + Clone + Default + 'static> SizableContainer for Columnar<C> 
where
    for<'a> &'a C: columnar::Index,
{
    fn capacity(&self) -> usize { 1024 }
    fn preferred_capacity() -> usize { 1024 }
    fn reserve(&mut self, _additional: usize) { }
}

impl<C: columnar::Push<T>, T> PushInto<T> for Columnar<C> {
    #[inline]
    fn push_into(&mut self, item: T) {
        self.store.push(item);
    }
}


use columnar::bytes::{AsBytes, FromBytes, serialization::decode};

/// A container based on a columnar store, encoded in aligned bytes.
#[derive(Clone, Default)]
pub struct ColumnarBytes<B, C> {
    bytes: B,
    phantom: std::marker::PhantomData<C>,
}

impl<B: std::ops::Deref<Target = [u64]> + Clone + Default + 'static, C: AsBytes + Clone + Default + 'static> Container for ColumnarBytes<B, C> 
where
    for<'a> C::Borrowed<'a> : Len + Clear + Index,
{
    fn len(&self) -> usize {
        <C::Borrowed<'_> as FromBytes>::from_bytes(&mut decode(&self.bytes)).len()
    }
    // Perhpas this should be an enum that allows the bytes to be un-set, but .. not sure what this should do.
    fn clear(&mut self) { unimplemented!() }

    type ItemRef<'a> = <C::Borrowed<'a> as Index>::Ref where Self: 'a;
    type Iter<'a> = IterOwn<C::Borrowed<'a>>;
    fn iter<'a>(&'a self) -> Self::Iter<'a> {
        <C::Borrowed<'a> as FromBytes>::from_bytes(&mut decode(&self.bytes)).into_iter()
    }

    type Item<'a> = <C::Borrowed<'a> as Index>::Ref where Self: 'a;
    type DrainIter<'a> = IterOwn<C::Borrowed<'a>>;
    fn drain<'a>(&'a mut self) -> Self::DrainIter<'a> {
        <C::Borrowed<'a> as FromBytes>::from_bytes(&mut decode(&self.bytes)).into_iter()
    }
}

impl<B: std::ops::Deref<Target = [u64]> + Clone + Default + 'static, C: AsBytes + Clone + Default + 'static> SizableContainer for ColumnarBytes<B, C> 
where
    for<'a> C::Borrowed<'a> : Len + Clear + Index,
{
    fn capacity(&self) -> usize { 1024 }
    fn preferred_capacity() -> usize { 1024 }
    fn reserve(&mut self, _additional: usize) { }
}
