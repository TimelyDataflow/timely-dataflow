use std::collections::{HashMap, HashSet};
use std::collections::hash_map::{Occupied, Vacant};
use std::hash::Hash;
use std::hash;
use core::fmt::Show;

use progress::{Timestamp, PathSummary, Scope};
use communication::channels::{Data, ExchangeReceiver, OutputBuffer, exchange_with};
use example::stream::Stream;
use example::ports::TeePort;

use progress::subgraph::Source::ScopeOutput;
use progress::subgraph::Target::ScopeInput;

pub trait DistinctExtensionTrait
{
    fn distinct(&mut self) -> Self;
}

impl<T: Timestamp, S: PathSummary<T>, D: Data+Hash+Eq+Show> DistinctExtensionTrait for Stream<T, S, D>
{
    fn distinct(&mut self) -> Stream<T, S, D>
    {
        // allocate an exchange channel pair? :D
        let alloc = &mut (*self.allocator.borrow_mut());
        let (sender, receiver) = exchange_with(alloc, |record| hash::hash(&record) as uint);

        let scope = DistinctScope
        {
            input:      receiver,
            output:     OutputBuffer { buffers: HashMap::new(), target: TeePort::new() },
            elements:   HashMap::new(),
            dispose:    Vec::new(),
        };

        let targets = scope.output.target.targets.clone();

        let index = self.graph.add_scope(box scope);

        self.graph.connect(self.name, ScopeInput(index, 0));
        self.port.borrow_mut().push(box sender);

        return self.copy_with(ScopeOutput(index, 0), targets);
    }
}

pub struct DistinctScope<T: Timestamp, D: Data+Hash+Eq+PartialEq>
{
    input:      ExchangeReceiver<T, D>,
    output:     OutputBuffer<T, D>,
    elements:   HashMap<T, HashSet<D>>,

    dispose:    Vec<T>,
}

impl<T: Timestamp+Hash+Eq, S: PathSummary<T>, D: Data+Hash+Eq+PartialEq+Show> Scope<T, S> for DistinctScope<T, D>
{
    fn inputs(&self) -> uint { 1 }
    fn outputs(&self) -> uint { 1 }

    fn push_external_progress(&mut self, frontier_progress: &Vec<Vec<(T, i64)>>) -> ()
    {
        // garbage collection
        for key in self.elements.keys()
        {
            // if any frontier additions strictly dominate a time, we can garbage collect it.
            if frontier_progress[0].iter().any(|&(time, delta)| delta > 0 && time.gt(key))
            {
                self.dispose.push(*key);
            }
        }

        while let Some(key) = self.dispose.pop() { self.elements.remove(&key); }
    }

    fn pull_internal_progress(&mut self,  frontier_progress: &mut Vec<Vec<(T, i64)>>,
                                          messages_consumed: &mut Vec<Vec<(T, i64)>>,
                                          messages_produced: &mut Vec<Vec<(T, i64)>>) -> bool
    {
        for (time, data) in self.input
        {
            let set = match self.elements.entry(time)
            {
                Occupied(x) => x.into_mut(),
                Vacant(x)   => x.set(HashSet::new()),
            };

            let mut helper = self.output.buffer_for(&time);

            for &datum in data.iter()
            {
                if set.insert(datum)
                {
                    helper.send(datum);
                }
            }

            helper.flush();
        }

        self.output.flush();

        self.input.pull_progress(&mut messages_consumed[0], &mut frontier_progress[0]);
        self.output.pull_progress(&mut messages_produced[0]);

        return false;
    }

    fn name(&self) -> String { format!("Distinct") }
    fn notify_me(&self) -> bool { true }
}
