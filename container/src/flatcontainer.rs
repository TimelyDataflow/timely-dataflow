//! Present a [`FlatStack`] as a timely container.

pub use flatcontainer::*;
use crate::{buffer, Container, SizableContainer, PushInto};

impl<R: Region> Container for FlatStack<R> {
    type ItemRef<'a> = R::ReadItem<'a>  where Self: 'a;
    type Item<'a> = R::ReadItem<'a> where Self: 'a;

    fn len(&self) -> usize {
        self.len()
    }

    fn clear(&mut self) {
        self.clear()
    }

    type Iter<'a> = <&'a Self as IntoIterator>::IntoIter where Self: 'a;

    fn iter(&self) -> Self::Iter<'_> {
        IntoIterator::into_iter(self)
    }

    type DrainIter<'a> = Self::Iter<'a> where Self: 'a;

    fn drain(&mut self) -> Self::DrainIter<'_> {
        IntoIterator::into_iter(&*self)
    }
}

impl<R: Region> SizableContainer for FlatStack<R> {
    fn at_capacity(&self) -> bool {
        self.len() == self.capacity()
    }
    fn ensure_capacity(&mut self, stash: &mut Option<Self>) {
        if self.capacity() == 0 {
            *self = stash.take().unwrap_or_default();
            self.clear();
        }
        let preferred = buffer::default_capacity::<R::Index>();
        if self.capacity() < preferred {
            self.reserve(preferred - self.capacity());
        }
    }
}

impl<R: Region + Push<T>, T> PushInto<T> for FlatStack<R> {
    #[inline]
    fn push_into(&mut self, item: T) {
        self.copy(item);
    }
}
