use std::default::Default;

use progress::{Timestamp, PathSummary};
use progress::frontier::Antichain;

pub trait Scope<T: Timestamp, S: PathSummary<T>> : 'static
{
    fn inputs(&self) -> uint;               // number of inputs to the vertex.
    fn outputs(&self) -> uint;              // number of outputs from the vertex.

    // Returns (in -> out) summaries using only edges internal to the vertex, and initial capabilities.
    // by default, full connectivity from all inputs to all outputs, and no capabilities reserved.
    fn get_internal_summary(&mut self) -> (Vec<Vec<Antichain<S>>>, Vec<Vec<(T, i64)>>)
    {
        (Vec::from_fn(self.inputs(), |_| Vec::from_fn(self.outputs(), |_| Antichain::from_elem(Default::default()))),
         Vec::from_fn(self.outputs(), |_| Vec::new()))
    }

    // Reports (out -> in) summaries for the vertex, and initial frontier information.
    // TODO: Update this to be summaries along paths external to the vertex, as this is strictly more informative.
    fn set_external_summary(&mut self, _summaries: Vec<Vec<Antichain<S>>>, _frontier: &Vec<Vec<(T, i64)>>) -> () { }


    // Reports changes to the projection of external work onto each of the scope's inputs.
    // TODO: Update this to be strictly external work, i.e. not work from this vertex. 
    fn push_external_progress(&mut self, _frontier_progress: &Vec<Vec<(T, i64)>>) -> () { }


    // Requests changes to the projection of internal work onto each of the scope's outputs, and
    //          changes to the number of messages consumed by each of the scope's inputs, and
    //          changes to the number of messages producen on each of the scope's outputs.
    // Returns a bool indicating if there is any un-reported work remaining (e.g. work that doesn't project on an output).
    fn pull_internal_progress(&mut self,  frontier_progress: &mut Vec<Vec<(T, i64)>>,           // to populate
                                          messages_consumed: &mut Vec<Vec<(T, i64)>>,           // to populate
                                          messages_produced: &mut Vec<Vec<(T, i64)>>) -> bool;  // to populate

    fn name(&self) -> String;               // something descriptive and helpful.
    fn notify_me(&self) -> bool { true }    // override to false if no interest in push_external_progress().
}
