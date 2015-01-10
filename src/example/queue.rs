use std::collections::HashMap;

use std::rc::Rc;
use std::cell::RefCell;

use progress::frontier::Antichain;
use progress::{Graph, Scope, PathSummary, Timestamp};
use progress::graph::GraphExtension;
use progress::subgraph::Source::ScopeOutput;
use progress::subgraph::Target::ScopeInput;
use progress::count_map::CountMap;

use communication::Observer;
use communication::channels::Data;
use example::stream::Stream;


pub trait QueueExtensionTrait {
    fn queue(&mut self) -> Self;
}

impl<T, S, D> QueueExtensionTrait for Stream<T, S, D>
where T:Timestamp,
      S:PathSummary<T>,
      D:Data,
{
    fn queue(&mut self) -> Stream<T, S, D> {
        let input = ScopeInputQueue::new_shared();
        let output = Rc::new(RefCell::new(Vec::new()));

        let index = self.graph.add_scope(QueueScope {
            input:      input.clone(),
            output:     output.clone(),
            to_send:    Vec::new(),
            guarantee:  Vec::new(),
        });

        self.graph.connect(self.name, ScopeInput(index, 0));
        self.add_observer(input);

        return self.copy_with(ScopeOutput(index, 0), output);
    }
}

pub struct ScopeInputQueue<T: Timestamp, D:Data>
{
    consumed_messages:  Vec<(T, i64)>,
    frontier_progress:  Vec<(T, i64)>,
    queues:             HashMap<T, Vec<D>>,
    buffer:             Vec<D>,
}

impl<T: Timestamp, D:Data> Observer<T, D> for Rc<RefCell<ScopeInputQueue<T, D>>>
{
    fn open(&mut self, time: &T) { }
    fn push(&mut self, data: &D) {
        // TODO : Fix so not so manny borrows ...
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

            let &mut ScopeInputQueue { buffer: ref mut buffer, queues: ref mut queues, ..} = &mut *input;

            for elem in buffer.drain() { queues[time.clone()].push(elem); }
            // for elem in buf.drain() { input.queues[time.clone()].push(elem); }
        }
    }
}

impl<T: Timestamp, D:Data> ScopeInputQueue<T, D>
{
    pub fn pull_progress(&mut self, consumed: &mut Vec<(T, i64)>, progress: &mut Vec<(T, i64)>)
    {
        for &(key, val) in self.consumed_messages.iter() { consumed.push((key, val)); }
        for &(key, val) in self.frontier_progress.iter() { progress.push((key, val)); }

        self.consumed_messages.clear();
        self.frontier_progress.clear();
    }

    pub fn extract_queue(&mut self, time: &T) -> Option<Vec<D>>
    {
        self.queues.remove(time)
    }

    pub fn new_shared() -> Rc<RefCell<ScopeInputQueue<T, D>>>
    {
        Rc::new(RefCell::new(ScopeInputQueue
        {
            consumed_messages:  Vec::new(),
            frontier_progress:  Vec::new(),
            queues:             HashMap::new(),
            buffer:             Vec::new(),
        }))
    }
}

struct QueueScope<T:Timestamp, S: PathSummary<T>, D:Data>
{
    input:      Rc<RefCell<ScopeInputQueue<T, D>>>,
    output:     Rc<RefCell<Vec<Box<Observer<T, D>>>>>,
    to_send:    Vec<(T, Vec<D>)>,
    guarantee:  Vec<(T, i64)>,
}

impl<T:Timestamp, S:PathSummary<T>, D:Data> Scope<T, S> for QueueScope<T, S, D>
{
    fn name(&self) -> String { format!("Queue") }
    fn inputs(&self) -> u64 { 1 }
    fn outputs(&self) -> u64 { 1 }

    fn set_external_summary(&mut self, _: Vec<Vec<Antichain<S>>>, guarantee: &Vec<Vec<(T, i64)>>) -> () {
        for &(key, val) in guarantee[0].iter() {
            self.guarantee.push((key, val));
        }
    }

    fn push_external_progress(&mut self, progress: &Vec<Vec<(T, i64)>>) -> () {
        for &(ref key, val) in progress[0].iter() { self.guarantee.update(key, val); }
        let mut input = self.input.borrow_mut();
        let mut sendable = Vec::new();
        for key in input.queues.keys() {
            if !self.guarantee.iter().any(|&(x,_)| x.le(key)) {
                sendable.push(*key);
            }
        }

        for key in sendable.iter() {
            self.to_send.push((*key, input.extract_queue(key).unwrap()));
        }
    }

    fn pull_internal_progress(&mut self, frontier_progress: &mut Vec<Vec<(T, i64)>>,
                                         messages_consumed: &mut Vec<Vec<(T, i64)>>,
                                         messages_produced: &mut Vec<Vec<(T, i64)>>) -> bool
    {
        // ask the input if it has consumed messages and created queues ...
        self.input.borrow_mut().pull_progress(&mut messages_consumed[0], &mut frontier_progress[0]);

        for (time, data) in self.to_send.drain() {
            messages_produced[0].push((time, data.len() as i64));
            frontier_progress[0].push((time, -1));

            for target in self.output.borrow_mut().iter_mut() { target.open(&time); }
            for target in self.output.borrow_mut().iter_mut() {
                for datum in data.iter() { target.push(datum); }
            }
            for target in self.output.borrow_mut().iter_mut() { target.shut(&time); }
        }

        return true;
    }
}
