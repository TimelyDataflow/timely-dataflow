//! Wordcount based on flatcontainer.

use {
    std::collections::HashMap,
    timely::{Container, container::CapacityContainerBuilder},
    timely::dataflow::channels::pact::{ExchangeCore, Pipeline},
    timely::dataflow::InputHandleCore,
    timely::dataflow::operators::{Inspect, Operator, Probe},
    timely::dataflow::ProbeHandle,
};

// Creates `WordCountContainer` and `WordCountReference` structs,
// as well as various implementations relating them to `WordCount`.
#[derive(columnar::Columnar)]
struct WordCount {
    text: String,
    diff: i64,
}

fn main() {

    type Container = Column<WordCount>;

    use columnar::Len;

    let config = timely::Config {
        communication: timely::CommunicationConfig::ProcessBinary(3),
        worker: timely::WorkerConfig::default(),
    };

    // initializes and runs a timely dataflow.
    timely::execute(config, |worker| {
        let mut input = <InputHandleCore<_, CapacityContainerBuilder<Container>>>::new();
        let mut probe = ProbeHandle::new();

        // create a new input, exchange data, and inspect its output
        worker.dataflow::<usize, _, _>(|scope| {
            input
                .to_stream(scope)
                .unary(
                    Pipeline,
                    "Split",
                    |_cap, _info| {
                        move |input, output| {
                            while let Some((time, data)) = input.next() {
                                let mut session = output.session(&time);
                                for wordcount in data.iter().flat_map(|wordcount| {
                                    wordcount.text.split_whitespace().map(move |text| WordCountReference { text, diff: wordcount.diff })
                                }) {
                                    session.give(wordcount);
                                }
                            }
                        }
                    },
                )
                .container::<Container>()
                .unary_frontier(
                    ExchangeCore::<ColumnBuilder<WordCount>,_>::new_core(|x: &WordCountReference<&str,&i64>| x.text.len() as u64),
                    "WordCount",
                    |_capability, _info| {
                        let mut queues = HashMap::new();
                        let mut counts = HashMap::new();

                        move |input, output| {
                            while let Some((time, data)) = input.next() {
                                queues
                                    .entry(time.retain())
                                    .or_insert(Vec::new())
                                    .push(std::mem::take(data));
                            }

                            for (key, val) in queues.iter_mut() {
                                if !input.frontier().less_equal(key.time()) {
                                    let mut session = output.session(key);
                                    for batch in val.drain(..) {
                                        for wordcount in batch.iter() {
                                            let total =
                                            if let Some(count) = counts.get_mut(wordcount.text) {
                                                *count += wordcount.diff;
                                                *count
                                            }
                                            else {
                                                counts.insert(wordcount.text.to_string(), *wordcount.diff);
                                                *wordcount.diff
                                            };
                                            session.give(WordCountReference { text: wordcount.text, diff: total });
                                        }
                                    }
                                }
                            }

                            queues.retain(|_key, val| !val.is_empty());
                        }
                    },
                )
                .container::<Container>()
                .inspect(|x| println!("seen: {:?}", x))
                .probe_with(&mut probe);
        });

        // introduce data and watch!
        for round in 0..10 {
            input.send(WordCountReference { text: "flat container", diff: 1 });
            input.advance_to(round + 1);
            while probe.less_than(input.time()) {
                worker.step();
            }
        }
    })
    .unwrap();
}


pub use container::Column;
mod container {

    use columnar::Columnar;
    use columnar::Container as FooBozzle;

    use timely_bytes::arc::Bytes;

    /// A container based on a columnar store, encoded in aligned bytes.
    pub enum Column<C: Columnar> {
        /// The typed variant of the container.
        Typed(C::Container),
        /// The binary variant of the container.
        Bytes(Bytes),
        /// Relocated, aligned binary data, if `Bytes` doesn't work for some reason.
        ///
        /// Reasons could include misalignment, cloning of data, or wanting
        /// to release the `Bytes` as a scarce resource.
        Align(Box<[u64]>),
    }

    impl<C: Columnar> Default for Column<C> {
        fn default() -> Self { Self::Typed(Default::default()) }
    }

    impl<C: Columnar> Clone for Column<C> where C::Container: Clone {
        fn clone(&self) -> Self {
            match self {
                Column::Typed(t) => Column::Typed(t.clone()),
                Column::Bytes(b) => {
                    assert!(b.len() % 8 == 0);
                    let mut alloc: Vec<u64> = vec![0; b.len() / 8];
                    bytemuck::cast_slice_mut(&mut alloc[..]).copy_from_slice(&b[..]);
                    Self::Align(alloc.into())
                },
                Column::Align(a) => Column::Align(a.clone()),
            }
        }
    }

    use columnar::{Clear, Len, Index, FromBytes};
    use columnar::bytes::{EncodeDecode, Indexed};
    use columnar::common::IterOwn;

    use timely::Container;
    impl<C: Columnar> Container for Column<C> {
        fn len(&self) -> usize {
            match self {
                Column::Typed(t) => t.len(),
                Column::Bytes(b) => <<C::Container as columnar::Container<C>>::Borrowed<'_> as FromBytes>::from_bytes(&mut Indexed::decode(bytemuck::cast_slice(b))).len(),
                Column::Align(a) => <<C::Container as columnar::Container<C>>::Borrowed<'_> as FromBytes>::from_bytes(&mut Indexed::decode(a)).len(),
            }
        }
        // This sets the `Bytes` variant to be an empty `Typed` variant, appropriate for pushing into.
        fn clear(&mut self) {
            match self {
                Column::Typed(t) => t.clear(),
                Column::Bytes(_) => *self = Column::Typed(Default::default()),
                Column::Align(_) => *self = Column::Typed(Default::default()),
            }
        }

        type ItemRef<'a> = C::Ref<'a>;
        type Iter<'a> = IterOwn<<C::Container as columnar::Container<C>>::Borrowed<'a>>;
        fn iter<'a>(&'a self) -> Self::Iter<'a> {
            match self {
                Column::Typed(t) => t.borrow().into_iter(),
                Column::Bytes(b) => <<C::Container as columnar::Container<C>>::Borrowed<'a> as FromBytes>::from_bytes(&mut Indexed::decode(bytemuck::cast_slice(b))).into_iter(),
                Column::Align(a) => <<C::Container as columnar::Container<C>>::Borrowed<'a> as FromBytes>::from_bytes(&mut Indexed::decode(a)).into_iter(),
            }
        }

        type Item<'a> = C::Ref<'a>;
        type DrainIter<'a> = IterOwn<<C::Container as columnar::Container<C>>::Borrowed<'a>>;
        fn drain<'a>(&'a mut self) -> Self::DrainIter<'a> {
            match self {
                Column::Typed(t) => t.borrow().into_iter(),
                Column::Bytes(b) => <<C::Container as columnar::Container<C>>::Borrowed<'a> as FromBytes>::from_bytes(&mut Indexed::decode(bytemuck::cast_slice(b))).into_iter(),
                Column::Align(a) => <<C::Container as columnar::Container<C>>::Borrowed<'a> as FromBytes>::from_bytes(&mut Indexed::decode(a)).into_iter(),
            }
        }
    }

    use timely::container::SizableContainer;
    impl<C: Columnar> SizableContainer for Column<C> {
        fn at_capacity(&self) -> bool {
            match self {
                Self::Typed(t) => {
                    let length_in_bytes = 8 * Indexed::length_in_words(&t.borrow());
                    length_in_bytes >= (1 << 20)
                },
                Self::Bytes(_) => true,
                Self::Align(_) => true,
            }
        }
        fn ensure_capacity(&mut self, _stash: &mut Option<Self>) { }
    }

    use timely::container::PushInto;
    impl<C: Columnar, T> PushInto<T> for Column<C> where C::Container: columnar::Push<T> {
        #[inline]
        fn push_into(&mut self, item: T) {
            use columnar::Push;
            match self {
                Column::Typed(t) => t.push(item),
                Column::Align(_) | Column::Bytes(_) => {
                    // We really oughtn't be calling this in this case.
                    // We could convert to owned, but need more constraints on `C`.
                    unimplemented!("Pushing into Column::Bytes without first clearing");
                }
            }
        }
    }

    use timely::dataflow::channels::ContainerBytes;
    impl<C: Columnar> ContainerBytes for Column<C> {
        fn from_bytes(bytes: timely::bytes::arc::Bytes) -> Self {
            // Our expectation / hope is that `bytes` is `u64` aligned and sized.
            // If the alignment is borked, we can relocate. IF the size is borked,
            // not sure what we do in that case.
            assert!(bytes.len() % 8 == 0);
            if let Ok(_) = bytemuck::try_cast_slice::<_, u64>(&bytes) {
                Self::Bytes(bytes)
            }
            else {
                println!("Re-locating bytes for alignment reasons");
                let mut alloc: Vec<u64> = vec![0; bytes.len() / 8];
                bytemuck::cast_slice_mut(&mut alloc[..]).copy_from_slice(&bytes[..]);
                Self::Align(alloc.into())
            }
        }

        fn length_in_bytes(&self) -> usize {
            match self {
                // We'll need one u64 for the length, then the length rounded up to a multiple of 8.
                Column::Typed(t) => 8 * Indexed::length_in_words(&t.borrow()),
                Column::Bytes(b) => b.len(),
                Column::Align(a) => 8 * a.len(),
            }
        }

        fn into_bytes<W: ::std::io::Write>(&self, writer: &mut W) {
            match self {
                Column::Typed(t) => { Indexed::write(writer, &t.borrow()).unwrap() },
                Column::Bytes(b) => writer.write_all(b).unwrap(),
                Column::Align(a) => writer.write_all(bytemuck::cast_slice(a)).unwrap(),
            }
        }
    }
}


use builder::ColumnBuilder;
mod builder {

    use std::collections::VecDeque;
    use columnar::{Columnar, Clear, Len, Push};
    use columnar::bytes::{EncodeDecode, Indexed};
    use super::Column;

    /// A container builder for `Column<C>`.
    pub struct ColumnBuilder<C: Columnar> {
        /// Container that we're writing to.
        current: C::Container,
        /// Empty allocation.
        empty: Option<Column<C>>,
        /// Completed containers pending to be sent.
        pending: VecDeque<Column<C>>,
    }

    use timely::container::PushInto;
    impl<C: Columnar, T> PushInto<T> for ColumnBuilder<C> where C::Container: columnar::Push<T> {
        #[inline]
        fn push_into(&mut self, item: T) {
            self.current.push(item);
            // If there is less than 10% slop with 2MB backing allocations, mint a container.
            use columnar::Container;
            let words = Indexed::length_in_words(&self.current.borrow());
            let round = (words + ((1 << 18) - 1)) & !((1 << 18) - 1);
            if round - words < round / 10 {
                let mut alloc = Vec::with_capacity(round);
                Indexed::encode(&mut alloc, &self.current.borrow());
                self.pending.push_back(Column::Align(alloc.into_boxed_slice()));
                self.current.clear();
            }
        }
    }

    impl<C: Columnar> Default for ColumnBuilder<C> {
        fn default() -> Self {
            ColumnBuilder {
                current: Default::default(),
                empty: None,
                pending: Default::default(),
            }
        }
    }

    use timely::container::{ContainerBuilder, LengthPreservingContainerBuilder};
    impl<C: Columnar> ContainerBuilder for ColumnBuilder<C> where C::Container: Clone {
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
                self.pending.push_back(Column::Typed(std::mem::take(&mut self.current)));
            }
            self.empty = self.pending.pop_front();
            self.empty.as_mut()
        }
    }

    impl<C: Columnar> LengthPreservingContainerBuilder for ColumnBuilder<C> where C::Container: Clone { }
}
