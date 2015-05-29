use std::rc::Rc;
use std::cell::RefCell;

use std::default::Default;

use progress::{Timestamp, CountMap, Antichain};

pub trait Scope<T: Timestamp> {
    fn inputs(&self) -> u64;               // number of inputs to the vertex.
    fn outputs(&self) -> u64;              // number of outputs from the vertex.

    // Returns (in -> out) summaries using only edges internal to the vertex, and initial capabilities.
    // by default, full connectivity from all inputs to all outputs, and no capabilities reserved.
    fn get_internal_summary(&mut self) -> (Vec<Vec<Antichain<T::Summary>>>, Vec<CountMap<T>>) {
        (vec![vec![Antichain::from_elem(Default::default()); self.outputs() as usize]; self.inputs() as usize],
         vec![CountMap::new(); self.outputs() as usize])
    }

    // Reports (out -> in) summaries for the vertex, and initial frontier information.
    // TODO: Update this to be summaries along paths external to the vertex, as this is strictly more informative.
    fn set_external_summary(&mut self, _summaries: Vec<Vec<Antichain<T::Summary>>>, _frontier: &mut [CountMap<T>]) { }

    // Reports changes to the projection of external work onto each of the scope's inputs.
    // TODO: Update this to be strictly external work, i.e. not work from this vertex.
    // Note: callee is expected to consume the contents of _external to indicate acknowledgement.
    fn push_external_progress(&mut self, _external: &mut [CountMap<T>]) { }

    // Requests changes to the projection of internal work onto each of the scope's outputs, and
    //          changes to the number of messages consumed by each of the scope's inputs, and
    //          changes to the number of messages producen on each of the scope's outputs.
    // Returns a bool indicating if there is any un-reported work remaining (e.g. work that doesn't project on an output).
    fn pull_internal_progress(&mut self,  internal: &mut [CountMap<T>],           // to populate
                                          consumed: &mut [CountMap<T>],           // to populate
                                          produced: &mut [CountMap<T>]) -> bool;  // to populate

    fn name(&self) -> String;               // something descriptive and helpful.
    fn notify_me(&self) -> bool { true }    // override to false if no interest in push_external_progress().
}

// TODO : try_unwrap is unstable; we need this until Rust fixes that.
impl<T: Timestamp, S: Scope<T>> Scope<T> for Rc<RefCell<S>> {
    fn inputs(&self) -> u64 { self.borrow().inputs() }
    fn outputs(&self) -> u64 { self.borrow().outputs() }
    fn get_internal_summary(&mut self) -> (Vec<Vec<Antichain<T::Summary>>>, Vec<CountMap<T>>) { self.borrow_mut().get_internal_summary() }
    fn set_external_summary(&mut self, x: Vec<Vec<Antichain<T::Summary>>>, y: &mut [CountMap<T>]) { self.borrow_mut().set_external_summary(x, y); }
    fn push_external_progress(&mut self, x: &mut [CountMap<T>]) { self.borrow_mut().push_external_progress(x); }
    fn pull_internal_progress(&mut self, internal: &mut [CountMap<T>],           // to populate
                                          consumed: &mut [CountMap<T>],           // to populate
                                          produced: &mut [CountMap<T>]) -> bool
                                  { self.borrow_mut().pull_internal_progress(internal, consumed, produced) }
    fn name(&self) -> String { self.borrow().name() }
    fn notify_me(&self) -> bool { self.borrow().notify_me() }
}
