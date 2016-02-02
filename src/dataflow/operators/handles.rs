use std::rc::Rc;
use std::cell::RefCell;
use progress::Timestamp;
use progress::count_map::CountMap;
use dataflow::channels::pullers::Counter as PullCounter;
use dataflow::channels::pushers::Counter as PushCounter;
use dataflow::channels::pushers::buffer::{Buffer, Session};
use dataflow::channels::Content;
use timely_communication::Push;

use dataflow::operators::Capability;
use dataflow::operators::capability::mint as mint_capability;

pub struct InputHandle<'a, T: Timestamp, D: 'a> {
    pull_counter: &'a mut PullCounter<T, D>,
    internal: Rc<RefCell<CountMap<T>>>,
}

impl<'a, 'b, T: Timestamp, D> InputHandle<'a, T, D> {
    pub fn new(pull_counter: &'a mut PullCounter<T, D>, internal: Rc<RefCell<CountMap<T>>>) -> InputHandle<'a, T, D> {
        InputHandle {
            pull_counter: pull_counter,
            internal: internal,
        }
    }

    #[inline]
    pub fn next(&mut self) -> Option<(Capability<T>, &mut Content<D>)> {
        let internal = &mut self.internal;
        self.pull_counter.next().map(|(&time, content)| {
            (mint_capability(time, internal.clone()), content)
        })
    }

    #[inline]
    pub fn for_each<F: FnMut(Capability<T>, &mut Content<D>)>(&mut self, mut logic: F) {
        while let Some((cap, data)) = self.next() {
            ::logging::log(&::logging::GUARDED_MESSAGE, true);
            logic(cap, data);
            ::logging::log(&::logging::GUARDED_MESSAGE, false);
        }
    }
}

pub struct OutputHandle<'a, T: Timestamp, D: 'a, P: Push<(T, Content<D>)>+'a> {
    pub push_buffer: &'a mut Buffer<T, D, PushCounter<T, D, P>>,
}

impl<'a, T: Timestamp, D, P: Push<(T, Content<D>)>> OutputHandle<'a, T, D, P> {
    pub fn new(push_buffer: &'a mut Buffer<T, D, PushCounter<T, D, P>>) -> OutputHandle<'a, T, D, P> {
        OutputHandle {
            push_buffer: push_buffer,
        }
    }

    pub fn session<'b>(&'b mut self, cap: &Capability<T>) -> Session<'b, T, D, PushCounter<T, D, P>> where 'a: 'b {
        self.push_buffer.session(cap)
    }
}

