//! Present a [`FlatStack`] as a timely container.

pub use flatcontainer::*;
use flatcontainer::impls::offsets::OffsetContainer;
use crate::{buffer, Container, SizableContainer, PushInto};

impl<R, S> Container for FlatStack<R, S>
where
    R: Region + Clone + 'static,
    S: OffsetContainer<<R as Region>::Index> + Clone + 'static,
{
    type ItemRef<'a> = R::ReadItem<'a>  where Self: 'a;
    type Item<'a> = R::ReadItem<'a> where Self: 'a;

    fn len(&self) -> usize {
        self.len()
    }

    fn clear(&mut self) {
        self.clear()
    }

    type Iter<'a> = <&'a Self as IntoIterator>::IntoIter;

    fn iter<'a>(&'a self) -> Self::Iter<'a> {
        IntoIterator::into_iter(self)
    }

    type DrainIter<'a> = Self::Iter<'a>;

    fn drain<'a>(&'a mut self) -> Self::DrainIter<'a> {
        IntoIterator::into_iter(&*self)
    }
}

// Only implemented for `FlatStack` with `Vec` offsets.
impl<R: Region + Clone + 'static> SizableContainer for FlatStack<R> {
    fn capacity(&self) -> usize {
        self.capacity()
    }

    fn preferred_capacity() -> usize {
        buffer::default_capacity::<R::Index>()
    }

    fn reserve(&mut self, additional: usize) {
        self.reserve(additional);
    }
}

impl<R, S, T> PushInto<T> for FlatStack<R, S>
where
    R: Region + Push<T>,
    S: OffsetContainer<R::Index> + Clone + 'static,
    for<'a> &'a S: IntoIterator<Item = &'a R::Index>,
{
    #[inline]
    fn push_into(&mut self, item: T) {
        self.copy(item);
    }
}
