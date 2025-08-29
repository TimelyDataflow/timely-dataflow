//! Wordcount based on the `columnar` crate.

use std::collections::HashMap;

use columnar::Index;
use timely::Accountable;
use timely::container::CapacityContainerBuilder;
use timely::dataflow::channels::pact::{ExchangeCore, Pipeline};
use timely::dataflow::InputHandleCore;
use timely::dataflow::operators::{InspectCore, Operator, Probe};
use timely::dataflow::ProbeHandle;

// Creates `WordCountContainer` and `WordCountReference` structs,
// as well as various implementations relating them to `WordCount`.
#[derive(columnar::Columnar)]
struct WordCount {
    text: String,
    diff: i64,
}

fn main() {

    type InnerContainer = <WordCount as columnar::Columnar>::Container;
    type Container = Column<InnerContainer>;

    use columnar::Len;

    let config = timely::Config {
        communication: timely::CommunicationConfig::ProcessBinary(3),
        worker: timely::WorkerConfig::default(),
    };

    // initializes and runs a timely dataflow.
    timely::execute(config, |worker| {
        let mut input = <InputHandleCore<_, CapacityContainerBuilder<Container>>>::new();
        let probe = ProbeHandle::new();

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
                                for wordcount in data.borrow().into_index_iter().flat_map(|wordcount| {
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
                    ExchangeCore::<ColumnBuilder<InnerContainer>,_>::new_core(|x: &WordCountReference<&str,&i64>| x.text.len() as u64),
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
                                        for wordcount in batch.borrow().into_index_iter() {
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
                .inspect_container(|x| {
                    match x {
                        Ok((time, data)) => {
                            println!("seen at: {:?}\t{:?} records", time, data.record_count());
                            for wc in data.borrow().into_index_iter() {
                                println!("  {}: {}", wc.text, wc.diff);
                            }
                        },
                        Err(frontier) => println!("frontier advanced to {:?}", frontier),
                    }
                })
                .probe_with(&probe);
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

    /// A container based on a columnar store, encoded in aligned bytes.
    pub enum Column<C> {
        /// The typed variant of the container.
        Typed(C),
        /// The binary variant of the container.
        Bytes(timely_bytes::arc::Bytes),
        /// Relocated, aligned binary data, if `Bytes` doesn't work for some reason.
        ///
        /// Reasons could include misalignment, cloning of data, or wanting
        /// to release the `Bytes` as a scarce resource.
        Align(Box<[u64]>),
    }

    impl<C: Default> Default for Column<C> {
        fn default() -> Self { Self::Typed(Default::default()) }
    }

    // The clone implementation moves out of the `Bytes` variant into `Align`.
    // This is optional and non-optimal, as the bytes clone is relatively free.
    // But, we don't want to leak the uses of `Bytes`, is why we do this I think.
    impl<C: columnar::Container> Clone for Column<C> where C: Clone {
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
        fn clone_from(&mut self, other: &Self) {
            match (self, other) {
                (Column::Typed(t0), Column::Typed(t1)) => {
                    // Derived `Clone` implementations for e.g. tuples cannot be relied on to call `clone_from`.
                    let t1 = t1.borrow();
                    t0.clear();
                    t0.extend_from_self(t1, 0..t1.len());
                }
                (Column::Align(a0), Column::Align(a1)) => { a0.clone_from(a1); }
                (x, y) => { *x = y.clone(); }
            }
        }
    }

    use columnar::{Len, Index, FromBytes};
    use columnar::bytes::{EncodeDecode, Indexed};
    use columnar::common::IterOwn;

    impl<C: columnar::ContainerBytes> Column<C> {
        /// Borrows the contents no matter their representation.
        #[inline(always)] pub fn borrow(&self) -> C::Borrowed<'_> {
            match self {
                Column::Typed(t) => t.borrow(),
                Column::Bytes(b) => <C::Borrowed<'_> as FromBytes>::from_bytes(&mut Indexed::decode(bytemuck::cast_slice(b))),
                Column::Align(a) => <C::Borrowed<'_> as FromBytes>::from_bytes(&mut Indexed::decode(a)),
            }
        }
    }

    impl<C: columnar::ContainerBytes> timely::Accountable for Column<C> {
        #[inline] fn record_count(&self) -> i64 { i64::try_from(self.borrow().len()).unwrap() }
        #[inline] fn is_empty(&self) -> bool { self.borrow().is_empty() }
    }
    impl<C: columnar::ContainerBytes> timely::container::DrainContainer for Column<C> {
        type Item<'a> = C::Ref<'a>;
        type DrainIter<'a> = IterOwn<C::Borrowed<'a>>;
        fn drain<'a>(&'a mut self) -> Self::DrainIter<'a> { self.borrow().into_index_iter() }
    }

    impl<C: columnar::ContainerBytes> timely::container::SizableContainer for Column<C> {
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

    impl<C: columnar::Container, T> timely::container::PushInto<T> for Column<C> where C: columnar::Push<T> {
        #[inline]
        fn push_into(&mut self, item: T) {
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

    impl<C: columnar::ContainerBytes> timely::dataflow::channels::ContainerBytes for Column<C> {
        fn from_bytes(bytes: timely::bytes::arc::Bytes) -> Self {
            // Our expectation / hope is that `bytes` is `u64` aligned and sized.
            // If the alignment is borked, we can relocate. IF the size is borked,
            // not sure what we do in that case.
            assert!(bytes.len() % 8 == 0);
            if bytemuck::try_cast_slice::<_, u64>(&bytes).is_ok() {
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
    use columnar::bytes::{EncodeDecode, Indexed};
    use super::Column;

    /// A container builder for `Column<C>`.
    #[derive(Default)]
    pub struct ColumnBuilder<C> {
        /// Container that we're writing to.
        current: C,
        /// Empty allocation.
        empty: Option<Column<C>>,
        /// Completed containers pending to be sent.
        pending: VecDeque<Column<C>>,
    }

    impl<C: columnar::ContainerBytes, T> timely::container::PushInto<T> for ColumnBuilder<C> where C: columnar::Push<T> {
        #[inline]
        fn push_into(&mut self, item: T) {
            self.current.push(item);
            // If there is less than 10% slop with 2MB backing allocations, mint a container.
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

    use timely::container::{ContainerBuilder, LengthPreservingContainerBuilder};
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
                self.pending.push_back(Column::Typed(std::mem::take(&mut self.current)));
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

    impl<C: columnar::ContainerBytes> LengthPreservingContainerBuilder for ColumnBuilder<C> { }
}
