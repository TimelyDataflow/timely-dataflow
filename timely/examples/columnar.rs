//! Wordcount based on the `columnar` crate.

use std::collections::HashMap;

use columnar::Index;
use timely::Accountable;
use timely::container::CapacityContainerBuilder;
use timely::dataflow::channels::pact::{ExchangeCore, Pipeline};
use timely::dataflow::InputHandle;
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
        let mut input = <InputHandle<_, CapacityContainerBuilder<Container>>>::new();
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
                            input.for_each_time(|time, data| {
                                let mut session = output.session(&time);
                                for data in data {
                                    for wordcount in data.borrow().into_index_iter().flat_map(|wordcount| {
                                        wordcount.text.split_whitespace().map(move |text| WordCountReference { text, diff: wordcount.diff })
                                    }) {
                                        session.give(wordcount);
                                    }
                                }
                            });
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

                        move |(input, frontier), output| {
                            input.for_each_time(|time, data| {
                                queues
                                    .entry(time.retain(output.output_index()))
                                    .or_insert(Vec::new())
                                    .extend(data.map(std::mem::take));

                            });

                            for (key, val) in queues.iter_mut() {
                                if !frontier.less_equal(key.time()) {
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

    use columnar::bytes::stash::Stash;

    #[derive(Clone, Default)]
    pub struct Column<C> { pub stash: Stash<C, timely_bytes::arc::Bytes> }

    use columnar::{Len, Index};
    use columnar::bytes::{EncodeDecode, Indexed};
    use columnar::common::IterOwn;

    impl<C: columnar::ContainerBytes> Column<C> {
        /// Borrows the contents no matter their representation.
        #[inline(always)] pub fn borrow(&self) -> C::Borrowed<'_> { self.stash.borrow() }
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
            match &self.stash {
                Stash::Typed(t) => {
                    let length_in_bytes = 8 * Indexed::length_in_words(&t.borrow());
                    length_in_bytes >= (1 << 20)
                },
                Stash::Bytes(_) => true,
                Stash::Align(_) => true,
            }
        }
        fn ensure_capacity(&mut self, _stash: &mut Option<Self>) { }
    }

    impl<C: columnar::Container, T> timely::container::PushInto<T> for Column<C> where C: columnar::Push<T> {
        #[inline] fn push_into(&mut self, item: T) { use columnar::Push; self.stash.push(item) }
    }

    impl<C: columnar::ContainerBytes> timely::dataflow::channels::ContainerBytes for Column<C> {
        fn from_bytes(bytes: timely::bytes::arc::Bytes) -> Self { Self { stash: bytes.into() } }
        fn length_in_bytes(&self) -> usize { self.stash.length_in_bytes() }
        fn into_bytes<W: ::std::io::Write>(&self, writer: &mut W) { self.stash.into_bytes(writer) }
    }
}


use builder::ColumnBuilder;
mod builder {

    use std::collections::VecDeque;
    use columnar::bytes::{EncodeDecode, Indexed, stash::Stash};
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
                self.pending.push_back(Column { stash: Stash::Align(alloc.into_boxed_slice()) });
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
                self.pending.push_back(Column { stash: Stash::Typed(std::mem::take(&mut self.current)) });
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
