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

    type Container = Column<<WordCount as columnar::Columnar>::Container>;

    use columnar::Len;

    // initializes and runs a timely dataflow.
    timely::execute_from_args(std::env::args(), |worker| {
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
                    ExchangeCore::new(|x: &WordCountReference<&str,&i64>| x.text.len() as u64),
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

    use timely_bytes::arc::Bytes;

    /// A container based on a columnar store, encoded in aligned bytes.
    pub enum Column<C> {
        /// The typed variant of the container.
        Typed(C),
        /// The binary variant of the container.
        Bytes(Bytes),
    }

    impl<C: Default> Default for Column<C> {
        fn default() -> Self { Self::Typed(C::default()) }
    }

    impl<C: Clone> Clone for Column<C> {
        fn clone(&self) -> Self {
            match self {
                Column::Typed(t) => Column::Typed(t.clone()),
                Column::Bytes(_) => unimplemented!(),
            }
        }
    }

    use columnar::{Clear, Len, Index, bytes::{AsBytes, FromBytes}};
    use columnar::bytes::serialization::decode;
    use columnar::common::IterOwn;

    use timely::Container;
    impl<C: AsBytes + Clear + Len + Clone + Default + 'static> Container for Column<C>
    where
        for<'a> C::Borrowed<'a> : Len + Index,
    {
        fn len(&self) -> usize {
            match self {
                Column::Typed(t) => t.len(),
                Column::Bytes(b) => <C::Borrowed<'_> as FromBytes>::from_bytes(&mut decode(bytemuck::cast_slice(&b[..]))).len(),
            }
        }
        // Perhpas this should be an enum that allows the bytes to be un-set, but .. not sure what this should do.
        fn clear(&mut self) {
            match self {
                Column::Typed(t) => t.clear(),
                Column::Bytes(_) => unimplemented!(),
            }
            // unimplemented!()
        }

        type ItemRef<'a> = <C::Borrowed<'a> as Index>::Ref where Self: 'a;
        type Iter<'a> = IterOwn<C::Borrowed<'a>>;
        fn iter<'a>(&'a self) -> Self::Iter<'a> {
            match self {
                Column::Typed(t) => <C::Borrowed<'a> as FromBytes>::from_bytes(&mut t.as_bytes().map(|(_, x)| x)).into_iter(),
                Column::Bytes(b) => <C::Borrowed<'a> as FromBytes>::from_bytes(&mut decode(bytemuck::cast_slice(&b[..]))).into_iter()
            }
        }

        type Item<'a> = <C::Borrowed<'a> as Index>::Ref where Self: 'a;
        type DrainIter<'a> = IterOwn<C::Borrowed<'a>>;
        fn drain<'a>(&'a mut self) -> Self::DrainIter<'a> {
            match self {
                Column::Typed(t) => <C::Borrowed<'a> as FromBytes>::from_bytes(&mut t.as_bytes().map(|(_, x)| x)).into_iter(),
                Column::Bytes(b) => <C::Borrowed<'a> as FromBytes>::from_bytes(&mut decode(bytemuck::cast_slice(&b[..]))).into_iter()
            }
        }
    }

    use timely::container::SizableContainer;
    impl<C: AsBytes + Clear + Len + Clone + Default + 'static> SizableContainer for Column<C>
    where
        for<'a> C::Borrowed<'a> : Len + Index,
    {
        fn capacity(&self) -> usize { 1024 }
        fn preferred_capacity() -> usize { 1024 }
        fn reserve(&mut self, _additional: usize) { }
    }

    use timely::container::PushInto;
    impl<C: columnar::Push<T>, T> PushInto<T> for Column<C> {
        #[inline]
        fn push_into(&mut self, item: T) {
            match self {
                Column::Typed(t) => t.push(item),
                Column::Bytes(_) => unimplemented!(),
            }
        }
    }

    use timely::dataflow::channels::ContainerBytes;
    impl<C: columnar::bytes::AsBytes> ContainerBytes for Column<C> {
        fn from_bytes(bytes: timely::bytes::arc::Bytes) -> Self {
            Self::Bytes(bytes)
        }

        fn length_in_bytes(&self) -> usize {
            match self {
                // We'll need one u64 for the length, then the length rounded up to a multiple of 8.
                Column::Typed(t) => t.as_bytes().map(|(_, x)| 8 * (1 + (x.len()/8) + if x.len() % 8 == 0 { 0 } else { 1 })).sum(),
                Column::Bytes(b) => b.len(),
            }
        }

        fn into_bytes<W: ::std::io::Write>(&self, writer: &mut W) {
            match self {
                Column::Typed(t) => {
                    for (_align, _bytes) in t.as_bytes() {
                        // Each byte slice is a u64 length in bytes,
                        // followed by bytes padded to a multiple of eight bytes.
                        // writer.write_all(&)
                    }
                },
                Column::Bytes(b) => writer.write_all(&b[..]).unwrap(),
            }
        }
    }
}