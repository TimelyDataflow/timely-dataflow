//! Conversion to the `Stream` type from iterators.

use std::rc::Rc;
use std::cell::RefCell;
use std::default::Default;

use progress::frontier::Antichain;
use progress::{Operate, Timestamp};
use progress::nested::subgraph::Source;
use progress::count_map::CountMap;

use Data;
use dataflow::channels::Content;
use dataflow::channels::pushers::{Tee, Counter};
use dataflow::channels::pushers::buffer::Buffer;

use dataflow::{Stream, Scope};

/// Converts to a timely `Stream`.
pub trait ToStream<T: Timestamp, D: Data> {
    /// Converts to a timely `Stream`.
    ///
    /// #Examples
    /// 
    /// `ToStream` is implemented by any implementor of `IntoIterator` whose
    /// associated type `Item` implements `Data`. This means that iterators, 
    /// vectors, and other collections may be directly converted to timely 
    /// streams.
    ///
    /// ```
    /// use timely::dataflow::operators::{ToStream, Capture};
    /// use timely::dataflow::operators::capture::Extract;
    ///
    /// let (data1, data2) = timely::example(|scope| {
    ///     let data1 = (0..3).to_stream(scope).capture();
    ///     let data2 = vec![0,1,2].to_stream(scope).capture();
    ///     (data1, data2)
    /// });
    /// 
    /// assert_eq!(data1.extract(), data2.extract());
    /// ```
    fn to_stream<S: Scope<Timestamp=T>>(self, scope: &mut S) -> Stream<S, D>;
}

impl<T: Timestamp, I: IntoIterator+'static> ToStream<T, I::Item> for I where I::Item: Data {
    fn to_stream<S: Scope<Timestamp=T>>(self, scope: &mut S) -> Stream<S, I::Item> {

        let (output, registrar) = Tee::<S::Timestamp, I::Item>::new();
        let copies = scope.peers();

        let index = scope.add_operator(Operator {
            iterator: Some(self.into_iter()),
            copies: copies,
            output: Buffer::new(Counter::new(output, Rc::new(RefCell::new(CountMap::new())))),
        });

        Stream::new(Source { index: index, port: 0 }, registrar, scope.clone())
    }
}

struct Operator<T:Timestamp, D: Data, I: Iterator<Item=D>> {
    iterator: Option<I>,
    copies: usize,
    output: Buffer<T, D, Counter<T, D, Tee<T, D>>>,
}

impl<T:Timestamp, D: Data, I: Iterator<Item=D>> Operate<T> for Operator<T, D, I> {
    fn name(&self) -> String { "ToStream".to_owned() }
    fn inputs(&self) -> usize { 0 }
    fn outputs(&self) -> usize { 1 }

    fn get_internal_summary(&mut self) -> (Vec<Vec<Antichain<T::Summary>>>, Vec<CountMap<T>>) {
        let mut map = CountMap::new();
        map.update(&Default::default(), self.copies as i64);
        (Vec::new(), vec![map])
    }

    fn pull_internal_progress(&mut self,_messages_consumed: &mut [CountMap<T>],
                                         frontier_progress: &mut [CountMap<T>],
                                         messages_produced: &mut [CountMap<T>]) -> bool
    {
        if self.iterator.is_some() {

            // if we find that there is a next element, send a bunch.
            if let Some(element) = self.iterator.as_mut().unwrap().next() {

                // send 4096 messages, or whatever the buffer size is.
                let mut session = self.output.session(&Default::default());
                session.give(element);
                for element in self.iterator.as_mut().unwrap().take((256 * Content::<D>::default_length()) - 1) {
                    session.give(element);
                }
            }
            else {

                self.output.cease();
                self.output.inner().pull_progress(&mut messages_produced[0]);
                frontier_progress[0].update(&Default::default(), -1);
                self.iterator = None;
            }
        }

        false
    }

    fn notify_me(&self) -> bool { false }
}
