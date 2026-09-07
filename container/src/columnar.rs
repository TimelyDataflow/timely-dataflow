//! Columnar containers for allocation-conscious data transport.
//!
//! [`ColumnarContainer`] stores either mutable typed columns or an immutable
//! view over serialized communication bytes. With binary communication, a
//! receiver can therefore inspect records without reconstructing owned rows.
//! [`ColumnarBuilder`] assembles typed columns and reclaims column allocations
//! returned by synchronous serializers.

use std::collections::VecDeque;

use ::columnar::bytes::stash::Stash;
use ::columnar::{Index, Len};

use timely_bytes::arc::Bytes;

use crate::{
    Accountable, ContainerBuilder, DrainContainer, LengthPreservingContainerBuilder, PushInto,
    SizableContainer,
};

/// Preferred serialized size of a columnar transport container.
pub const DEFAULT_BUFFER_BYTES: usize = 1 << 20;

/// A columnar container that is either typed or backed by communication bytes.
#[derive(Clone, Default)]
pub struct ColumnarContainer<C> {
    stash: Stash<C, Bytes>,
}

impl<C: ::columnar::ContainerBytes> ColumnarContainer<C> {
    /// Borrows the columnar contents independent of their current representation.
    #[inline(always)]
    pub fn borrow(&self) -> C::Borrowed<'_> {
        self.stash.borrow()
    }

    /// Returns true when the container directly retains serialized bytes.
    pub fn is_bytes(&self) -> bool {
        matches!(self.stash, Stash::Bytes(_))
    }

    fn typed(container: C) -> Self {
        Self {
            stash: Stash::Typed(container),
        }
    }

    fn take_typed(&mut self) -> Option<C> {
        match std::mem::take(&mut self.stash) {
            Stash::Typed(mut container) => {
                ::columnar::Clear::clear(&mut container);
                Some(container)
            }
            Stash::Bytes(_) | Stash::Align(_) => None,
        }
    }
}

impl<C: ::columnar::ContainerBytes> Accountable for ColumnarContainer<C> {
    #[inline]
    fn record_count(&self) -> i64 {
        i64::try_from(self.borrow().len()).expect("columnar record count must fit in i64")
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.borrow().is_empty()
    }
}

impl<C: ::columnar::ContainerBytes> DrainContainer for ColumnarContainer<C> {
    type Item<'a>
        = C::Ref<'a>
    where
        C: 'a;
    type DrainIter<'a>
        = ::columnar::common::IterOwn<C::Borrowed<'a>>
    where
        C: 'a;

    #[inline]
    fn drain(&mut self) -> Self::DrainIter<'_> {
        self.borrow().into_index_iter()
    }
}

impl<C: ::columnar::ContainerBytes> SizableContainer for ColumnarContainer<C> {
    fn at_capacity(&self) -> bool {
        self.stash.length_in_bytes() >= DEFAULT_BUFFER_BYTES
    }

    fn ensure_capacity(&mut self, spare: &mut Option<Self>) {
        if matches!(self.stash, Stash::Typed(_)) {
            // `CapacityContainerBuilder` leaves a default typed container in
            // `current` after sending and places the container returned by the
            // pusher in `spare`. At the start of the next batch, prefer that
            // returned allocation. Once a record has been pushed this branch
            // no longer swaps, so the working container remains stable.
            if self.is_empty()
                && spare
                    .as_ref()
                    .is_some_and(|candidate| matches!(candidate.stash, Stash::Typed(_)))
            {
                std::mem::swap(self, spare.as_mut().expect("checked above"));
                if let Stash::Typed(container) = &mut self.stash {
                    ::columnar::Clear::clear(container);
                }
            }
            return;
        }
        if let Some(mut spare) = spare.take() {
            if let Some(container) = spare.take_typed() {
                self.stash = Stash::Typed(container);
                return;
            }
        }
        self.stash = Stash::Typed(C::default());
    }
}

impl<C, T> PushInto<T> for ColumnarContainer<C>
where
    C: ::columnar::Container + ::columnar::ContainerBytes + ::columnar::Push<T>,
{
    #[inline]
    fn push_into(&mut self, item: T) {
        ::columnar::Push::push(&mut self.stash, item);
    }
}

impl<C: ::columnar::ContainerBytes> ColumnarContainer<C> {
    /// Wraps and validates bytes containing a columnar encoding.
    pub fn from_bytes(bytes: Bytes) -> Self {
        Self {
            stash: Stash::try_from_bytes(bytes).expect("valid columnar container bytes"),
        }
    }

    /// Reports the number of bytes in this container's wire encoding.
    pub fn length_in_bytes(&self) -> usize {
        self.stash.length_in_bytes()
    }

    /// Writes this container's columnar encoding.
    pub fn write_bytes<W: std::io::Write>(&self, writer: &mut W) -> std::io::Result<()> {
        self.stash.write_bytes(writer)
    }
}

/// Builds bounded-size [`ColumnarContainer`] batches from individual records.
///
/// Typed column allocations returned by a pusher are reused while a sequence is
/// active. Draining [`ContainerBuilder::finish`] or calling
/// [`ContainerBuilder::relax`] releases all typed allocations, so quiescent
/// memory does not scale with the number of builders or logical channels.
pub struct ColumnarBuilder<C, const PREFERRED_BYTES: usize = DEFAULT_BUFFER_BYTES> {
    current: C,
    needs_current: bool,
    returned: Option<ColumnarContainer<C>>,
    spares: Vec<C>,
    pending: VecDeque<ColumnarContainer<C>>,
}

impl<C: Default, const PREFERRED_BYTES: usize> Default for ColumnarBuilder<C, PREFERRED_BYTES> {
    fn default() -> Self {
        Self {
            current: C::default(),
            needs_current: false,
            returned: None,
            spares: Vec::new(),
            pending: VecDeque::new(),
        }
    }
}

impl<C: ::columnar::ContainerBytes, const PREFERRED_BYTES: usize>
    ColumnarBuilder<C, PREFERRED_BYTES>
{
    const MAX_SPARES: usize = 2;

    fn reclaim_returned(&mut self) {
        if let Some(mut returned) = self.returned.take() {
            if let Some(container) = returned.take_typed() {
                // Prefer the most recently returned containers. Early sends
                // commonly leave allocation-free defaults behind; retaining
                // those forever would crowd out useful allocations that make
                // the round trip through a channel later.
                if self.spares.len() == Self::MAX_SPARES {
                    self.spares.remove(0);
                }
                self.spares.push(container);
            }
        }
    }

    fn ensure_current(&mut self) {
        if self.needs_current {
            self.reclaim_returned();
            self.current = self.spares.pop().unwrap_or_default();
            self.needs_current = false;
        }
    }

    fn emit_current(&mut self) {
        if !self.current.is_empty() {
            self.pending
                .push_back(ColumnarContainer::typed(std::mem::take(&mut self.current)));
            self.needs_current = true;
        }
    }
}

impl<C, T, const PREFERRED_BYTES: usize> PushInto<T> for ColumnarBuilder<C, PREFERRED_BYTES>
where
    C: ::columnar::ContainerBytes + ::columnar::Push<T>,
{
    #[inline]
    fn push_into(&mut self, item: T) {
        assert!(
            PREFERRED_BYTES > 0,
            "preferred columnar batch size must be non-zero"
        );
        self.ensure_current();
        ::columnar::Push::push(&mut self.current, item);
        if ::columnar::bytes::indexed::length_in_words(&self.current.borrow()) * 8
            >= PREFERRED_BYTES
        {
            self.emit_current();
        }
    }
}

impl<C: ::columnar::ContainerBytes, const PREFERRED_BYTES: usize> ContainerBuilder
    for ColumnarBuilder<C, PREFERRED_BYTES>
{
    type Container = ColumnarContainer<C>;

    fn extract(&mut self) -> Option<&mut Self::Container> {
        self.reclaim_returned();
        self.returned = self.pending.pop_front();
        self.returned.as_mut()
    }

    fn finish(&mut self) -> Option<&mut Self::Container> {
        if !self.needs_current {
            self.emit_current();
        }
        self.reclaim_returned();
        if let Some(container) = self.pending.pop_front() {
            self.returned = Some(container);
            self.returned.as_mut()
        } else {
            // `finish` is called until it returns `None`. Treat that final
            // call as a quiescence boundary: retaining even one preferred-size
            // allocation per builder becomes prohibitive in wide dataflows.
            self.current = C::default();
            self.needs_current = false;
            self.returned = None;
            self.spares.clear();
            None
        }
    }

    fn relax(&mut self) {
        assert!(
            self.pending.is_empty(),
            "finish must drain pending columnar containers"
        );
        assert!(self.needs_current || self.current.is_empty());
        // A fixed per-builder pool has an unacceptable aggregate bound for
        // dataflows with many channels and workers. The zero-copy transport
        // owns the process-wide byte slabs worth retaining; typed column
        // scratch space is transient.
        *self = Self::default();
    }
}

impl<C: ::columnar::ContainerBytes, const PREFERRED_BYTES: usize> LengthPreservingContainerBuilder
    for ColumnarBuilder<C, PREFERRED_BYTES>
{
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(::columnar::Columnar)]
    struct TestRecord {
        key: u64,
        value: String,
    }

    type Columns = <TestRecord as ::columnar::Columnar>::Container;

    #[test]
    fn serialized_container_retains_received_bytes() {
        let mut original = ColumnarContainer::<Columns>::default();
        original.push_into(TestRecordReference {
            key: &7,
            value: "seven",
        });

        let mut encoded = Vec::new();
        original.write_bytes(&mut encoded).unwrap();
        let received = ColumnarContainer::<Columns>::from_bytes(
            timely_bytes::arc::BytesMut::from(encoded).freeze(),
        );

        assert!(received.is_bytes());
        assert_eq!(received.record_count(), 1);
        let record = received.borrow().get(0);
        assert_eq!(*record.key, 7);
        assert_eq!(record.value, b"seven");
    }

    #[test]
    fn builder_releases_typed_containers_after_finish() {
        let mut builder = ColumnarBuilder::<Columns, 64>::default();
        for key in 0..16 {
            builder.push_into(TestRecordReference {
                key: &key,
                value: "value",
            });
        }

        while builder.finish().is_some() {}

        assert!(builder.current.is_empty());
        assert!(!builder.needs_current);
        assert!(builder.returned.is_none());
        assert!(builder.spares.is_empty());
        assert!(builder.pending.is_empty());
    }

    #[test]
    fn wide_quiescent_builder_set_has_no_pooled_containers() {
        let mut builders = std::iter::repeat_with(ColumnarBuilder::<Columns, 64>::default)
            .take(10_000)
            .collect::<Vec<_>>();

        for (key, builder) in builders.iter_mut().enumerate() {
            let key = key as u64;
            builder.push_into(TestRecordReference {
                key: &key,
                value: "value",
            });
            while builder.finish().is_some() {}
        }

        for builder in builders {
            assert!(builder.current.is_empty());
            assert!(!builder.needs_current);
            assert!(builder.returned.is_none());
            assert!(builder.spares.is_empty());
            assert!(builder.pending.is_empty());
        }
    }
}
