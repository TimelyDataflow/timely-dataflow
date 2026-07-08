//! A container storing [`columnar`] data, either as a typed container or as
//! serialized bytes.
//!
//! [`Column`] is [`columnar`]'s [`Stash`] fixed to timely's [`Bytes`] type. It
//! implements the container traits so that columnar data can be produced,
//! exchanged, and consumed like any other container; its contents can be borrowed
//! whether they are still typed or have been serialized to bytes.

use std::collections::VecDeque;

use columnar::bytes::{indexed, stash::Stash};
use columnar::common::IterOwn;
use columnar::{Columnar, Index, Len};

use timely_bytes::arc::Bytes;

use crate::{
    Accountable, ContainerBuilder, DrainContainer, LengthPreservingContainerBuilder, PushInto,
    SizableContainer,
};

/// A columnar container, held either as a typed container or as serialized bytes.
///
/// This is [`columnar`]'s [`Stash`] with the byte representation fixed to timely's
/// [`Bytes`]. Received data are borrowed in place (no per-record deserialization);
/// owning an element requires `into_owned`.
pub type Column<C> = Stash<C, Bytes>;

/// The [`Column`] type appropriate for a [`Columnar`] row type `T`.
pub type ColumnOf<T> = Column<<T as Columnar>::Container>;

/// A built [`Column`] is considered full once its serialized size reaches this many
/// bytes. This sets the columnar batching granularity, which is much coarser than
/// the `Vec` default (see [`crate::buffer::BUFFER_SIZE_BYTES`]).
pub const COLUMN_CAPACITY_BYTES: usize = 1 << 20;

impl<C: columnar::ContainerBytes> Accountable for Column<C> {
    #[inline]
    fn record_count(&self) -> i64 {
        i64::try_from(self.borrow().len()).unwrap()
    }
    #[inline]
    fn is_empty(&self) -> bool {
        self.borrow().is_empty()
    }
}

impl<C: columnar::ContainerBytes> DrainContainer for Column<C> {
    type Item<'a> = C::Ref<'a>;
    type DrainIter<'a> = IterOwn<C::Borrowed<'a>>;
    fn drain(&mut self) -> Self::DrainIter<'_> {
        self.borrow().into_index_iter()
    }
}

impl<C: columnar::ContainerBytes> SizableContainer for Column<C> {
    fn at_capacity(&self) -> bool {
        match self {
            Stash::Typed(t) => 8 * indexed::length_in_words(&t.borrow()) >= COLUMN_CAPACITY_BYTES,
            Stash::Bytes(_) | Stash::Align(_) => true,
        }
    }
    fn ensure_capacity(&mut self, _stash: &mut Option<Self>) {}
}

impl<C: columnar::Container + columnar::ContainerBytes, T> PushInto<T> for Column<C>
where
    C: columnar::Push<T>,
{
    #[inline]
    fn push_into(&mut self, item: T) {
        use columnar::Push;
        self.push(item)
    }
}

/// A container builder for [`Column`].
///
/// Items are accumulated into a typed columnar container. When that container's
/// serialized size approaches a target, it is encoded into an aligned byte buffer
/// which is enqueued for extraction, and the typed container is cleared for reuse.
///
/// Encoding eagerly, rather than deferring encoding until data are sent, keeps the
/// (comparatively expensive to rebuild) typed accumulator resident in the builder:
/// only the flat encoded bytes travel onward, so the columns need not be rebuilt
/// each batch. This matters on the exchange path, where a sealed container is sent
/// to a peer and never returns, so there is no opportunity to recycle it.
#[derive(Default)]
pub struct ColumnBuilder<C> {
    /// Container into which we accumulate pushed items.
    current: C,
    /// An empty allocation, retained for reuse across extractions.
    empty: Option<Column<C>>,
    /// Completed containers, pending extraction.
    pending: VecDeque<Column<C>>,
}

impl<C: columnar::ContainerBytes, T> PushInto<T> for ColumnBuilder<C>
where
    C: columnar::Push<T>,
{
    #[inline]
    fn push_into(&mut self, item: T) {
        self.current.push(item);
        // If there is less than 10% slop with a 2MB-aligned backing allocation, mint
        // a container by encoding into an exactly-sized aligned buffer.
        let words = indexed::length_in_words(&self.current.borrow());
        let round = (words + ((1 << 18) - 1)) & !((1 << 18) - 1);
        if round - words < round / 10 {
            let mut alloc = Vec::with_capacity(round);
            indexed::encode(&mut alloc, &self.current.borrow());
            self.pending.push_back(Stash::Align(alloc.into_boxed_slice().into()));
            self.current.clear();
        }
    }
}

impl<C: columnar::ContainerBytes> ContainerBuilder for ColumnBuilder<C> {
    type Container = Column<C>;

    #[inline]
    fn extract(&mut self) -> Option<&mut Self::Container> {
        if let Some(container) = self.pending.pop_front() {
            self.empty = Some(container);
            self.empty.as_mut()
        } else {
            None
        }
    }

    #[inline]
    fn finish(&mut self) -> Option<&mut Self::Container> {
        if !self.current.is_empty() {
            self.pending
                .push_back(Stash::Typed(std::mem::take(&mut self.current)));
        }
        self.empty = self.pending.pop_front();
        self.empty.as_mut()
    }

    #[inline]
    fn relax(&mut self) {
        // The caller is responsible for draining all contents; assert that we are empty.
        // The assertion is not strictly necessary, but it helps catch bugs.
        assert!(self.current.is_empty());
        assert!(self.pending.is_empty());
        *self = Self::default();
    }
}

impl<C: columnar::ContainerBytes> LengthPreservingContainerBuilder for ColumnBuilder<C> {}
