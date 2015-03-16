use std::collections::HashMap;

use std::rc::Rc;
use std::cell::RefCell;

use progress::frontier::Antichain;
use progress::{Graph, Scope, Timestamp};
use progress::subgraph::Source::ScopeOutput;
use progress::subgraph::Target::ScopeInput;
use progress::count_map::CountMap;

use communication::Communicator;
use communication::Observer;
use communication::channels::{Data, OutputPort};
use example::stream::Stream;

pub trait QueueExtensionTrait {
    fn queue(&mut self) -> Self;
}

impl<G: Graph, D: Data, C: Communicator> QueueExtensionTrait for Stream<G, D, C> {
    fn queue(&mut self) -> Stream<G, D, C> {
        let input = ScopeInputQueue::new_shared();
        let output = OutputPort::<G::Timestamp, D>::new();

        let index = self.graph.add_scope(QueueScope {
            input:      input.clone(),
            output:     output.clone(),
            to_send:    Vec::new(),
            guarantee:  CountMap::new(),
        });

        self.connect_to(ScopeInput(index, 0), input);
        self.clone_with(ScopeOutput(index, 0), output)
    }
}

pub struct ScopeInputQueue<T: Timestamp, D:Data> {
    consumed_messages:  CountMap<T>,
    frontier_progress:  CountMap<T>,
    queues:             HashMap<T, Vec<D>>,
    buffer:             Vec<D>,
}

impl<T: Timestamp, D:Data> Observer for Rc<RefCell<ScopeInputQueue<T, D>>> {
    type Time = T;
    type Data = D;
    fn open(&mut self,_time: &T) { }
    fn push(&mut self, data: &D) {
        // TODO : Fix so not so many borrows ...
        self.borrow_mut().buffer.push(data.clone());
    }

    fn shut(&mut self, time: &T) {
        let mut input = self.borrow_mut();
        let len = input.buffer.len();
        if len > 0 {
            input.consumed_messages.update(time, len as i64);
            if !input.queues.contains_key(time) {
                input.queues.insert(time.clone(), Vec::new());
                input.frontier_progress.update(time, 1);
            }

            let &mut ScopeInputQueue { ref mut buffer, ref mut queues, ..} = &mut *input;

            for elem in buffer.drain() { queues[time.clone()].push(elem); }
            // for elem in buf.drain() { input.queues[time.clone()].push(elem); }
        }
    }
}

impl<T: Timestamp, D:Data> ScopeInputQueue<T, D> {
    pub fn pull_progress(&mut self, consumed: &mut CountMap<T>, progress: &mut CountMap<T>) {
        while let Some((ref key, val)) = self.consumed_messages.pop() { consumed.update(key, val); }
        while let Some((ref key, val)) = self.frontier_progress.pop() { progress.update(key, val); }
    }

    pub fn extract_queue(&mut self, time: &T) -> Option<Vec<D>> {
        self.queues.remove(time)
    }

    pub fn new_shared() -> Rc<RefCell<ScopeInputQueue<T, D>>> {
        Rc::new(RefCell::new(ScopeInputQueue {
            consumed_messages:  CountMap::new(),
            frontier_progress:  CountMap::new(),
            queues:             HashMap::new(),
            buffer:             Vec::new(),
        }))
    }
}

struct QueueScope<T:Timestamp, D:Data> {
    input:      Rc<RefCell<ScopeInputQueue<T, D>>>,
    output:     OutputPort<T, D>,
    to_send:    Vec<(T, Vec<D>)>,
    guarantee:  CountMap<T>,        // TODO : Should probably be a MutableAntichain
}

impl<T:Timestamp, D:Data> Scope<T> for QueueScope<T, D> {
    fn name(&self) -> String { format!("Queue") }
    fn inputs(&self) -> u64 { 1 }
    fn outputs(&self) -> u64 { 1 }

    fn set_external_summary(&mut self, _: Vec<Vec<Antichain<T::Summary>>>, guarantee: &mut Vec<CountMap<T>>) -> () {
        while let Some((ref key, val)) = guarantee[0].pop() {
            self.guarantee.update(key, val);
        }
    }

    fn push_external_progress(&mut self, progress: &mut Vec<CountMap<T>>) -> () {
        while let Some((ref key, val)) = progress[0].pop() { self.guarantee.update(key, val); }
        let mut input = self.input.borrow_mut();
        let mut sendable = Vec::new();
        for key in input.queues.keys() {
            if !self.guarantee.elements().iter().any(|&(ref x, _)| x.le(key)) {
                sendable.push(key.clone());
            }
        }

        for key in sendable.drain() {
            let data = input.extract_queue(&key).unwrap();
            self.to_send.push((key, data));
        }
    }

    fn pull_internal_progress(&mut self, frontier_progress: &mut Vec<CountMap<T>>,
                                         messages_consumed: &mut Vec<CountMap<T>>,
                                         messages_produced: &mut Vec<CountMap<T>>) -> bool
    {
        // ask the input if it has consumed messages and created queues ...
        self.input.borrow_mut().pull_progress(&mut messages_consumed[0], &mut frontier_progress[0]);

        for (ref time, ref data) in self.to_send.drain() {
            messages_produced[0].update(time, data.len() as i64);
            frontier_progress[0].update(time, -1);

            self.output.open(time);
            for datum in data.iter() { self.output.push(datum); }
            self.output.shut(time)
            // for target in self.output.borrow_mut().iter_mut() { target.open(time); }
            // for target in self.output.borrow_mut().iter_mut() {
            //     for datum in data.iter() { target.push(datum); }
            // }
            // for target in self.output.borrow_mut().iter_mut() { target.shut(time); }
        }

        return true;
    }
}
