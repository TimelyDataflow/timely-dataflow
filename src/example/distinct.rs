use std::collections::{HashMap, HashSet};
use std::collections::hash_map::{Occupied, Vacant};
use std::hash::Hash;
use std::hash;
use core::fmt::Show;

use progress::{Timestamp, PathSummary, Scope};
use communication::channels::{Data};
use communication::exchange::{ExchangeReceiver, exchange_with};
use example::stream::Stream;
use communication::channels::OutputPort;

use progress::subgraph::Source::ScopeOutput;
use progress::subgraph::Target::ScopeInput;

pub trait DistinctExtensionTrait { fn distinct(&mut self) -> Self; }

impl<T: Timestamp, S: PathSummary<T>, D: Data+Hash+Eq+Show> DistinctExtensionTrait for Stream<T, S, D>
{
    fn distinct(&mut self) -> Stream<T, S, D>
    {
        let allocator = &mut (*self.allocator.borrow_mut());
        let (sender, receiver) = exchange_with(allocator, |record| hash::hash(&record) as uint);
        let scope = DistinctScope
        {
            input:      receiver,
            output:     OutputPort::new(),
            elements:   HashMap::new(),
            dispose:    Vec::new(),
        };

        let targets = scope.output.targets.clone();
        let index = self.graph.add_scope(box scope);

        self.graph.connect(self.name, ScopeInput(index, 0));
        self.port.borrow_mut().push(box sender);
    
        return self.copy_with(ScopeOutput(index, 0), targets);
    }
}

pub struct DistinctScope<T: Timestamp, D: Data+Hash+Eq+PartialEq>
{
    input:      ExchangeReceiver<T, D>,
    output:     OutputPort<T, D>,
    elements:   HashMap<T, HashSet<D>>,
    dispose:    Vec<T>,
}

impl<T: Timestamp+Hash+Eq, S: PathSummary<T>, D: Data+Hash+Eq+PartialEq+Show> Scope<T, S> for DistinctScope<T, D>
{
    fn inputs(&self) -> uint { 1 }
    fn outputs(&self) -> uint { 1 }

    fn push_external_progress(&mut self, external: &Vec<Vec<(T, i64)>>) -> ()
    {
        for key in self.elements.keys()
        {
            if external[0].iter().any(|&(time, delta)| delta > 0 && time.gt(key))
            {
                self.dispose.push(*key);
            }
        }

        while let Some(key) = self.dispose.pop() { self.elements.remove(&key); }
    }

    fn pull_internal_progress(&mut self,  internal: &mut Vec<Vec<(T, i64)>>,
                                          consumed: &mut Vec<Vec<(T, i64)>>,
                                          produced: &mut Vec<Vec<(T, i64)>>) -> bool
    {
        for (time, data) in self.input
        {
            let mut output = self.output.buffer_for(&time);
            let set = match self.elements.entry(time)
            {
                Occupied(x) => x.into_mut(),
                Vacant(x)   => x.set(HashSet::new()),
            };

            // send anything new ...
            for &datum in data.iter() { if set.insert(datum) { output.send(datum); } }
        }

        // extract what we know about progress from the input and output adapters.
        self.input.pull_progress(&mut consumed[0], &mut internal[0]);
        self.output.pull_progress(&mut produced[0]);

        return false;
    }

    fn name(&self) -> String { format!("Distinct") }
    fn notify_me(&self) -> bool { true }
}
