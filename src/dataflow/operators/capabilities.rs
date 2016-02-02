use std::collections::VecDeque;
use std::rc::Rc;
use std::cell::RefCell;
use std::default::Default;
use std::ops::Deref;
use progress::Timestamp;
use progress::count_map::CountMap;
use progress::frontier::MutableAntichain;
use dataflow::channels::pullers::Counter as PullCounter;
use dataflow::channels::pushers::Counter as PushCounter;
use dataflow::channels::pushers::buffer::{Buffer, Session};
use dataflow::channels::Content;
use timely_communication::Push;

pub struct Capability<T: Timestamp> {
    time: T,
    internal: Rc<RefCell<CountMap<T>>>,
}

impl<T: Timestamp> Capability<T> {
    fn new(time: T, internal: Rc<RefCell<CountMap<T>>>) -> Capability<T> {
        internal.borrow_mut().update(&time, 1);
        Capability {
            time: time,
            internal: internal
        }
    }

    #[inline]
    pub fn time(&self) -> T {
        self.time
    }

    #[inline]
    pub fn into_delayed(self, new_time: &T) -> Capability<T> {
        assert!(new_time >= &self.time);
        Capability::<T>::new(new_time, self.internal.clone())
    }
}

impl<T: Timestamp> Drop for Capability<T> {
    fn drop(&mut self) {
        self.internal.borrow_mut().update(&self.time, -1);
    }
}

impl<T: Timestamp> Clone for Capability<T> {
    fn clone(&self) -> Capability<T> {
        Capability::<T>::new(self.time, self.internal.clone())
    }
}

impl<T: Timestamp> Deref for Capability<T> {
    type Target = T;

    fn deref(&self) -> &T {
        &self.time
    }
}

pub struct CapabilityPull<'a, T: Timestamp, D: 'a> {
    pull_counter: &'a mut PullCounter<T, D>,
    internal: Rc<RefCell<CountMap<T>>>,
}

impl<'a, 'b, T: Timestamp, D> CapabilityPull<'a, T, D> {
    pub fn new(pull_counter: &'a mut PullCounter<T, D>, internal: Rc<RefCell<CountMap<T>>>) -> CapabilityPull<'a, T, D> {
        CapabilityPull {
            pull_counter: pull_counter,
            internal: internal,
        }
    }

    #[inline]
    pub fn next(&mut self) -> Option<(Capability<T>, &mut Content<D>)> {
        let internal = &mut self.internal;
        self.pull_counter.next().map(|(&time, content)| {
            (Capability::<T>::new(time, internal.clone()), content)
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

pub struct CapabilityPush<'a, T: Timestamp, D: 'a, P: Push<(T, Content<D>)>+'a> {
    pub push_buffer: &'a mut Buffer<T, D, PushCounter<T, D, P>>,
}

impl<'a, T: Timestamp, D, P: Push<(T, Content<D>)>> CapabilityPush<'a, T, D, P> {
    pub fn new(push_buffer: &'a mut Buffer<T, D, PushCounter<T, D, P>>) -> CapabilityPush<'a, T, D, P> {
        CapabilityPush {
            push_buffer: push_buffer,
        }
    }

    pub fn session<'b>(&'b mut self, cap: &Capability<T>) -> Session<'b, T, D, PushCounter<T, D, P>> where 'a: 'b {
        self.push_buffer.session(cap)
    }
}

pub struct CapabilityNotificator<T: Timestamp> {
    pending: MutableAntichain<T>,
    frontier: Vec<MutableAntichain<T>>,
    available: VecDeque<T>,
    internal_changes: Rc<RefCell<CountMap<T>>>,
}

impl<T: Timestamp> CapabilityNotificator<T> {
    pub fn new(internal_changes: Rc<RefCell<CountMap<T>>>) -> CapabilityNotificator<T> {
        CapabilityNotificator {
            pending: Default::default(),
            frontier: Vec::new(),
            available: VecDeque::new(),
            internal_changes: internal_changes,
        }
    }

    /// Updates the `Notificator`'s frontiers from a `CountMap` per input.
    pub fn update_frontier_from_cm(&mut self, count_map: &mut [CountMap<T>]) {
        while self.frontier.len() < count_map.len() {
            self.frontier.push(MutableAntichain::new());
        }

        for (index, counts) in count_map.iter_mut().enumerate() {
            while let Some((time, delta)) = counts.pop() {
                self.frontier[index].update(&time, delta);
            }
        }
    }

    /// Reveals the elements in the frontier of the indicated input.
    pub fn frontier(&self, input: usize) -> &[T] {
        self.frontier[input].elements()
    }

    /// Requests a notification at `time`.
    #[inline]
    pub fn notify_at(&mut self, time: Capability<T>) {
        self.internal_changes.borrow_mut().update(&time, 1);
        self.pending.update(&time, 1);
    }

    #[inline]
    pub fn for_each<F: FnMut(Capability<T>, i64)>(&mut self, mut logic: F) {
        while let Some((cap, count)) = self.next() {
            ::logging::log(&::logging::GUARDED_PROGRESS, true);
            logic(cap, count);
            ::logging::log(&::logging::GUARDED_PROGRESS, false);
        }
    }

}

impl<T: Timestamp> Iterator for CapabilityNotificator<T> {
    type Item = (Capability<T>, i64);

    fn next(&mut self) -> Option<(Capability<T>, i64)> {

        // if nothing obvious available, scan for options
        if self.available.len() == 0 {
            for pend in self.pending.elements().iter() {
                if !self.frontier.iter().any(|x| x.le(pend) ) {
                    self.available.push_back(pend.clone());
                }
            }
        }

        // return an available notification, after cleaning up
        if let Some(time) = self.available.pop_front() {
            if let Some(delta) = self.pending.count(&time) {
                self.internal_changes.borrow_mut().update(&time, -delta);
                self.pending.update(&time, -delta);
                Some((Capability::new(time, self.internal_changes.clone()), delta))
            }
            else {
                panic!("failed to find available time in pending");
            }
        }
        else { None }
    }
}

pub fn unsafe_mint_capability<T: Timestamp>(time: T, internal: Rc<RefCell<CountMap<T>>>) -> Capability<T> {
    Capability {
        time: time,
        internal: internal,
    }
}

