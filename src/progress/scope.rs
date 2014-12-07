use std::default::Default;

use progress::{Timestamp, PathSummary};
use progress::frontier::Antichain;

pub trait Scope<T: Timestamp, S: PathSummary<T>> : 'static
{
    fn inputs(&self) -> uint;               // number of inputs to the vertex.
    fn outputs(&self) -> uint;              // number of outputs from the vertex.

    // produces (in -> out) summaries using only edges internal to the vertex, and initial capabilities.
    // by default, full connectivity from all inputs to all outputs, and no capabilities reserved.
    fn get_internal_summary(&mut self) -> (Vec<Vec<Antichain<S>>>, Vec<Vec<(T, i64)>>)
    {
        (Vec::from_fn(self.inputs(), |_| Vec::from_fn(self.outputs(), |_| Antichain::from_elem(Default::default()))),
         Vec::from_fn(self.outputs(), |_| Vec::new()))
    }

    // receives (out -> in) summaries using only edges external to the vertex, and initial frontier information.
    fn set_external_summary(&mut self, _summaries: Vec<Vec<Antichain<S>>>, _frontier: &Vec<Vec<(T, i64)>>) -> () { }

    // important that: sent message delta << output frontier update << recv message delta.
    // consequently, we'll just do them all at the same time.
    fn push_external_progress(&mut self, _frontier_progress: &Vec<Vec<(T, i64)>>) -> () { }
    fn pull_internal_progress(&mut self,  frontier_progress: &mut Vec<Vec<(T, i64)>>,         // to populate
                                          messages_consumed: &mut Vec<Vec<(T, i64)>>,         // to populate
                                          messages_produced: &mut Vec<Vec<(T, i64)>>) -> ();  // to populate

    fn name(&self) -> String;               // something descriptive and helpful.
    fn notify_me(&self) -> bool { true }    // override to false if no interest in push_external_progress().
}
