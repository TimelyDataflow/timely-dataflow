use std::default::Default;

use progress::{Timestamp, CountMap, Antichain};

pub trait Scope<T: Timestamp> {
    fn inputs(&self) -> u64;               // number of inputs to the vertex.
    fn outputs(&self) -> u64;              // number of outputs from the vertex.

    // Returns (in -> out) summaries using only edges internal to the vertex, and initial capabilities.
    // by default, full connectivity from all inputs to all outputs, and no capabilities reserved.
    fn get_internal_summary(&mut self) -> (Vec<Vec<Antichain<T::Summary>>>, Vec<CountMap<T>>) {
        ((0..self.inputs()).map(|_| (0..self.outputs()).map(|_| Antichain::from_elem(Default::default()))
                                                       .collect()).collect(),
         (0..self.outputs()).map(|_| CountMap::new()).collect())
    }

    // Reports (out -> in) summaries for the vertex, and initial frontier information.
    // TODO: Update this to be summaries along paths external to the vertex, as this is strictly more informative.
    fn set_external_summary(&mut self, _summaries: Vec<Vec<Antichain<T::Summary>>>, _frontier: &mut Vec<CountMap<T>>) -> () { }


    // Reports changes to the projection of external work onto each of the scope's inputs.
    // TODO: Update this to be strictly external work, i.e. not work from this vertex.
    // Note: callee is expected to consume the contents of _external to indicate acknowledgement.
    fn push_external_progress(&mut self, _external: &mut Vec<CountMap<T>>) -> () { }


    // Requests changes to the projection of internal work onto each of the scope's outputs, and
    //          changes to the number of messages consumed by each of the scope's inputs, and
    //          changes to the number of messages producen on each of the scope's outputs.
    // Returns a bool indicating if there is any un-reported work remaining (e.g. work that doesn't project on an output).
    fn pull_internal_progress(&mut self,  internal: &mut Vec<CountMap<T>>,           // to populate
                                          consumed: &mut Vec<CountMap<T>>,           // to populate
                                          produced: &mut Vec<CountMap<T>>) -> bool;  // to populate

    fn name(&self) -> String;               // something descriptive and helpful.
    fn notify_me(&self) -> bool { true }    // override to false if no interest in push_external_progress().
}
