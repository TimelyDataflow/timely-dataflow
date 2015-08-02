use std::rc::Rc;
use std::cell::RefCell;

use progress::{Timestamp, Scope, Antichain};
use progress::frontier::MutableAntichain;
use progress::nested::Target::ScopeInput;
use progress::count_map::CountMap;

use communication::Data;
use construction::{Stream, GraphBuilder};

pub trait Probe<T: Timestamp> {
    /// Constructs a progress probe which can indicates which timestamps have elapsed at the operator.
    fn probe(&self) -> ProbeHandle<T>;
}

impl<G: GraphBuilder, D: Data> Probe<G::Timestamp> for Stream<G, D> {
    fn probe(&self) -> ProbeHandle<G::Timestamp> {

        // the frontier is shared state; scope updates, handle reads.
        let frontier = Rc::new(RefCell::new(MutableAntichain::new()));

        // we add the scope, acquiring the name of the probe, then add an edge.
        let index = self.builder().add_scope(ProbeScope { frontier: frontier.clone() });
        self.builder().add_edge(*self.name(), ScopeInput(index, 0));

        // the handle is the only result
        ProbeHandle { frontier: frontier }
    }
}

pub struct ProbeHandle<T:Timestamp> {
    frontier: Rc<RefCell<MutableAntichain<T>>>
}

impl<T: Timestamp> ProbeHandle<T> {
    #[inline] pub fn lt(&self, time: &T) -> bool { self.frontier.borrow().lt(time) }
    #[inline] pub fn le(&self, time: &T) -> bool { self.frontier.borrow().le(time) }
}

pub struct ProbeScope<T:Timestamp> {
    frontier: Rc<RefCell<MutableAntichain<T>>>
}

impl<T:Timestamp> Scope<T> for ProbeScope<T> {
    fn name(&self) -> String { format!("Probe") }
    fn inputs(&self) -> u64 { 1 }
    fn outputs(&self) -> u64 { 0 }

    // we need to set the initial value of the frontier
    fn set_external_summary(&mut self, _: Vec<Vec<Antichain<T::Summary>>>, counts: &mut [CountMap<T>]) {
        let mut borrow = self.frontier.borrow_mut();
        while let Some((time, delta)) = counts[0].pop() {
            borrow.update(&time, delta);
        }
    }

    // each change to the frontier should be shared
    fn push_external_progress(&mut self, counts: &mut [CountMap<T>]) {
        let mut borrow = self.frontier.borrow_mut();
        while let Some((time, delta)) = counts[0].pop() {
            borrow.update(&time, delta);
        }
    }

    // the scope does nothing. this is actually a problem, because "reachability" assumes all messages on each edge.
    fn pull_internal_progress(&mut self,_: &mut [CountMap<T>], _: &mut [CountMap<T>], _: &mut [CountMap<T>]) -> bool {
        false
    }

    fn notify_me(&self) -> bool { true }
}
