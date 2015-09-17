//! An operator which captures the streams of records and notifications to play back later.
//!
//! Not yet tested; please be careful using!

use std::rc::Rc;
use std::cell::RefCell;

use ::Data;
use dataflow::{Scope, Stream};
use dataflow::channels::pact::{ParallelizationContract, Pipeline};
use dataflow::channels::pullers::Counter as PullCounter;
use dataflow::channels::pushers::Counter as PushCounter;
use dataflow::channels::pushers::buffer::Buffer as PushBuffer;
use dataflow::channels::pushers::Tee;
use dataflow::channels::message::Content;

use progress::count_map::CountMap;
use progress::nested::subgraph::Source::ChildOutput;
use progress::nested::subgraph::Target::ChildInput;
use progress::{Timestamp, Operate, Antichain};

pub trait Capture<T: Timestamp, D: Data> {
    ///
    ///
    /// #Examples
    /// ```
    /// use timely::dataflow::Scope;
    /// use timely::dataflow::operators::{Capture, ToStream, Inspect};

    /// timely::execute(timely::Configuration::Thread, |computation| {
    ///     let handle = computation.scoped::<u64,_,_>(|builder|
    ///         (0..10).to_stream(builder)
    ///                .capture()
    ///     );
    ///
    ///     computation.scoped(|builder| {
    ///         handle.replay(builder)
    ///               .inspect(|x| println!("replayed: {:?}", x));
    ///     })
    /// });
    /// ```
    fn capture(&self) -> Handle<T, D>;
}

impl<S: Scope, D: Data> Capture<S::Timestamp, D> for Stream<S, D> {
    fn capture(&self) -> Handle<S::Timestamp, D> {

        let events = Rc::new(RefCell::new(EventLink { event: Event::Start, next: None }));
        let handle = Handle { events: events.clone() };

        let mut scope = self.scope();   // clones the scope
        let channel_id = scope.new_identifier();

        let (sender, receiver) = Pipeline.connect(&mut scope, channel_id);
        let operator = CaptureOperator {
            input: PullCounter::new(receiver),
            events: events,
        };

        let index = scope.add_operator(operator);
        self.connect_to(ChildInput(index, 0), sender, channel_id);
        handle
    }
}

/// Possible events that the captured stream may provide.
pub enum Event<T, D> {
    Start,
    /// Progress received via `push_external_progress`.
    Progress(Vec<(T, i64)>),
    /// Messages received via the data stream.
    Messages(T, Content<D>),
}

pub struct EventLink<T, D> {
    event: Event<T, D>,
    next: Option<Rc<RefCell<EventLink<T, D>>>>,
}

// TODO : No notion of streaming garbage collection, which is probably a bug.
// TODO : The whole list will be collected when everyone is done, but perhaps a linked list would
// TODO : be better / have better properties?

/// A handle to the captured data, shared with the capture operator itself.
pub struct Handle<T: Timestamp, D: Data> {
    events: Rc<RefCell<EventLink<T, D>>>,
}

impl<T: Timestamp, D: Data> Handle<T, D> {
    pub fn replay<S: Scope<Timestamp=T>>(&self, scope: &mut S) -> Stream<S, D> {

        let (targets, registrar) = Tee::<S::Timestamp, D>::new();
        let operator = ReplayOperator {
            output: PushBuffer::new(PushCounter::new(targets, Rc::new(RefCell::new(CountMap::new())))),
            events: self.events.clone(),
        };

        let index = scope.add_operator(operator);
        Stream::new(ChildOutput(index, 0), registrar, scope.clone())
    }
}

struct CaptureOperator<T: Timestamp, D: Data> {
    input: PullCounter<T, D>,
    events: Rc<RefCell<EventLink<T, D>>>,
}

impl<T:Timestamp, D: Data> Operate<T> for CaptureOperator<T, D> {
    fn name(&self) -> &str { "Capture" }
    fn inputs(&self) -> usize { 1 }
    fn outputs(&self) -> usize { 0 }

    // we need to set the initial value of the frontier
    fn set_external_summary(&mut self, _: Vec<Vec<Antichain<T::Summary>>>, counts: &mut [CountMap<T>]) {
        self.events.borrow_mut().next = Some(Rc::new(RefCell::new(EventLink { event: Event::Progress(counts[0].updates.clone()), next: None })));
        let next = self.events.borrow().next.as_ref().unwrap().clone();
        self.events = next;
        counts[0].updates.clear();
    }

    // each change to the frontier should be shared
    fn push_external_progress(&mut self, counts: &mut [CountMap<T>]) {
        self.events.borrow_mut().next = Some(Rc::new(RefCell::new(EventLink { event: Event::Progress(counts[0].updates.clone()), next: None })));
        let next = self.events.borrow().next.as_ref().unwrap().clone();
        self.events = next;
        counts[0].updates.clear();
    }

    fn pull_internal_progress(&mut self, _: &mut [CountMap<T>], consumed: &mut [CountMap<T>], _: &mut [CountMap<T>]) -> bool {
        let events = &mut self.events;
        self.input.for_each(|&time, data| {
            events.borrow_mut().next = Some(Rc::new(RefCell::new(EventLink { event: Event::Messages(time, data.clone()), next: None })));
            let next = events.borrow().next.as_ref().unwrap().clone();
            *events = next;
        });
        self.input.pull_progress(&mut consumed[0]);
        false
    }
}

struct ReplayOperator<T:Timestamp, D: Data> {
    events: Rc<RefCell<EventLink<T, D>>>,
    output: PushBuffer<T, D, PushCounter<T, D, Tee<T, D>>>,
}

impl<T:Timestamp, D: Data> Operate<T> for ReplayOperator<T, D> {
    fn name(&self) -> &str { "Replay" }
    fn inputs(&self) -> usize { 0 }
    fn outputs(&self) -> usize { 1 }

    fn get_internal_summary(&mut self) -> (Vec<Vec<Antichain<T::Summary>>>, Vec<CountMap<T>>) {

        // panics if the event link has not been initialized; should only happen if no one has
        // called set_external_summary on the `CaptureOperator`. So, please don't use this in the
        // same graph as the `CaptureOperator`.

        // TODO : use Default::default() as the initial time, and in the first Event we dequeue,
        // TODO : announce that we've moved beyond Default::default().
        let mut result = CountMap::new();
        let next = self.events.borrow().next.as_ref().unwrap().clone();
        self.events = next;
        if let Event::Progress(ref vec) = self.events.borrow().event {
            for &(ref time, delta) in vec {
                result.update(time, delta);
            }
        }
        (vec![], vec![result])
    }

    fn pull_internal_progress(&mut self, internal: &mut [CountMap<T>], _: &mut [CountMap<T>], produced: &mut [CountMap<T>]) -> bool {

        while self.events.borrow().next.is_some() {
            let next = self.events.borrow().next.as_ref().unwrap().clone();
            self.events = next;
            match self.events.borrow().event {
                Event::Start => { },
                Event::Progress(ref vec) => {
                    for &(ref time, delta) in vec {
                        internal[0].update(time, delta);
                    }
                },
                Event::Messages(ref time, ref data) => {
                    let mut data = data.clone();
                    self.output.session(time).give_content(&mut data);
                }
            }
        }

        self.output.cease();
        self.output.inner().pull_progress(&mut produced[0]);

        false
    }
}


#[cfg(test)]
mod tests {

    use ::Configuration;
    use dataflow::*;
    use dataflow::operators::{Capture, ToStream, Inspect};

    #[test]
    fn probe() {

        // initializes and runs a timely dataflow computation
        ::execute(Configuration::Thread, |computation| {
            let handle = computation.scoped::<u64,_,_>(|builder|
                (0..10).to_stream(builder)
                       .capture()
            );

            // computation.step();
            // computation.step();
            // computation.step();

            computation.scoped(|builder| {
                handle.replay(builder)
                      .inspect(|x| println!("replayed: {:?}", x));
            })
        });
    }

}
