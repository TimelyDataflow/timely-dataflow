//! Present a [`FlatStack`] as a timely container.

pub use flatcontainer::*;
use crate::{Container, ContainerBuilder, PushContainer, PushInto};

impl<R: Region + Clone + 'static> Container for FlatStack<R> {
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

impl<R: Region + Clone + 'static> PushContainer for FlatStack<R> {
    fn capacity(&self) -> usize {
        self.capacity()
    }

    fn preferred_capacity() -> usize {
        crate::buffer::default_capacity::<R::Index>()
    }

    fn reserve(&mut self, additional: usize) {
        self.reserve(additional);
    }
}

impl<R: Region + Clone + 'static, T: CopyOnto<R>> PushInto<FlatStack<R>> for T {
    #[inline]
    fn push_into(self, target: &mut FlatStack<R>) {
        target.copy(self);
    }
}

#[derive(Default)]
struct DefaultFlatStackBuilder<R: Region> {
    current: FlatStack<R>,
    pending: Vec<FlatStack<R>>,
}

impl<R: Region + Clone + 'static> ContainerBuilder for DefaultFlatStackBuilder<R> {
    type Container = FlatStack<R>;

    fn push<T: PushInto<Self::Container>>(&mut self, item: T) {
        let preferred_capacity = crate::buffer::default_capacity::<R::Index>();
        if self.pending.capacity() < preferred_capacity {
            self.pending.reserve(preferred_capacity - self.pending.capacity());
        }
        item.push_into(&mut self.current);
        if self.current.len() == self.current.capacity() {
            self.pending.push(std::mem::take(&mut self.current));
        }
    }

    fn extract(&mut self) -> impl Iterator<Item=Self::Container> {
        self.pending.drain(..)
    }

    fn finish(&mut self) -> impl Iterator<Item=Self::Container> {
        self.pending.drain(..).chain(std::iter::once(std::mem::take(&mut self.current)))
    }
}
