use std::rc::Rc;
use std::cell::RefCell;
use std::default::Default;

use progress::count_map::CountMap;
use progress::notificator::Notificator;
use progress::{Timestamp, Scope, Antichain};
use communication::channels::Data;
use communication::exchange::ExchangeReceiver;
use communication::channels::{OutputPort, ObserverHelper};

pub struct UnaryScopeHandle<T: Timestamp, D1: Data, D2: Data> {
    pub input:          ExchangeReceiver<T, D1>,
    pub output:         ObserverHelper<OutputPort<T, D2>>,
    pub notificator:    Notificator<T>,
}

pub struct UnaryScope<T: Timestamp, D1: Data, D2: Data, L: FnMut(&mut UnaryScopeHandle<T, D1, D2>)> {
    handle:         UnaryScopeHandle<T, D1, D2>,
    logic:          L,
}

impl<T: Timestamp, D1: Data, D2: Data, L: FnMut(&mut UnaryScopeHandle<T, D1, D2>)> UnaryScope<T, D1, D2, L> {
    pub fn new(receiver: ExchangeReceiver<T, D1>, targets: OutputPort<T, D2>, logic: L) -> UnaryScope<T, D1, D2, L> {
        UnaryScope {
            handle: UnaryScopeHandle {
                input: receiver,
                output: ObserverHelper::new(targets.clone(), Rc::new(RefCell::new(CountMap::new()))),
                notificator: Default::default(),
            },
            logic: logic,
        }
    }
}

impl<T: Timestamp, D1: Data, D2: Data, L: FnMut(&mut UnaryScopeHandle<T, D1, D2>)> Scope<T> for UnaryScope<T, D1, D2, L> {
    fn inputs(&self) -> u64 { 1 }
    fn outputs(&self) -> u64 { 1 }

    fn set_external_summary(&mut self, _summaries: Vec<Vec<Antichain<T::Summary>>>, frontier: &mut Vec<CountMap<T>>) -> () {
        self.handle.notificator.update_frontier_from_cm(&mut frontier[0]);
        frontier[0].clear();
    }

    fn push_external_progress(&mut self, external: &mut Vec<CountMap<T>>) -> () {
        // println!("unary.pep: {:?}", external);
        self.handle.notificator.update_frontier_from_cm(&mut external[0]);
        external[0].clear();
    }

    fn pull_internal_progress(&mut self, internal: &mut Vec<CountMap<T>>,
                                         consumed: &mut Vec<CountMap<T>>,
                                         produced: &mut Vec<CountMap<T>>) -> bool
    {
        (self.logic)(&mut self.handle);

        // extract what we know about progress from the input and output adapters.
        self.handle.input.pull_progress(&mut consumed[0]);
        self.handle.output.pull_progress(&mut produced[0]);
        self.handle.notificator.pull_progress(&mut internal[0]);

        return false;   // no unannounced internal work
    }

    fn name(&self) -> String { format!("Unary") }
    fn notify_me(&self) -> bool { true }
}
