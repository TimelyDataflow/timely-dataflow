//! Methods which describe an operators topology, and the progress it makes.

use std::default::Default;

use progress::{Timestamp, CountMap, Antichain};


/// Methods for describing an operators topology, and the progress it makes.
pub trait Operate<T: Timestamp> {

    /// Indicates if the operator is strictly local to this worker.
    ///
    /// A parent scope must understand whether the progress information returned by the worker
    /// reflects only this worker's progress, so that it knows whether to send and receive the
    /// corresponding progress messages to its peers. If the operator is strictly local, it must
    /// exchange this information, whereas if the operator is itself implemented by the same set
    /// of workers, the parent scope understands that progress information already reflects the
    /// aggregate information among the workers.
    ///
    /// This is a coarse approximation to refined worker sets. In a future better world, operators
    /// would explain how their implementations are partitioned, so that a parent scope knows what
    /// progress information to exchange with which peers. Right now the two choices are either
    /// "all" or "none", but it could be more detailed. In the more detailed case, this method
    /// should / could return a pair (index, peers), indicating the group id of the worker out of
    /// how many groups. This becomes complicated, as a full all-to-all exchange would result in
    /// multiple copies of the same progress messages (but aggregated variously) arriving at
    /// arbitrary times.
    fn local(&self) -> bool { true }

    /// The number of inputs.
    fn inputs(&self) -> usize;
    /// The number of outputs.
    fn outputs(&self) -> usize;

    /// Fetches summary information about internal structure of the operator.
    ///
    /// Each operator must summarize its internal structure by a map from pairs `(input, output)`
    /// to an antichain of timestamp summaries, indicating how a timestamp on any of its inputs may
    /// be transformed to timestamps on any of its outputs.
    ///
    /// Each operator must also indicate whether it initially holds any capabilities on any of its
    /// outputs, so that the parent operator can properly initialize its progress information.
    ///
    /// The default behavior is to indicate that timestamps on any input can emerge unchanged on
    /// any output, and no initial capabilities are held.
    fn get_internal_summary(&mut self) -> (Vec<Vec<Antichain<T::Summary>>>, Vec<CountMap<T>>) {
        (vec![vec![Antichain::from_elem(Default::default()); self.outputs()]; self.inputs()],
         vec![CountMap::new(); self.outputs()])
    }

    /// Presents summary information about the external structure around the operator.
    ///
    /// Each operator exists in the context of a parent scope, and the edges and other operators it
    /// hosts represent paths messages may take from this operators outputs back to its inputs. For
    /// an operator to correctly understand the implications of local progress statements, it must
    /// understand how messages it produces may eventually return to its inputs.
    ///
    /// The parent scope must also provide initial capabilities for each of the inputs, reflecting
    /// work elsewhere in the timely computation. Note: it is not clear whether the parent must not
    /// include capabilities expressed by the operator itself. It seems possible to exclude such
    /// capabilities, if it would help the operator, but the operator should not yet rely on any
    /// specific behavior.
    fn set_external_summary(&mut self, _summaries: Vec<Vec<Antichain<T::Summary>>>, _frontier: &mut [CountMap<T>]) { }

    /// Reports a summary of progress statements external to the operator and its peer group.
    ///
    /// This report summarizes *all* of the external world, including updates issued by the operator
    /// itself. This is important, and means that there is an ordering constraint that needs to be
    /// enforced by the operator: before reporting any summarized progress to its parent, a child
    /// must install the non-summarized parts (interal messages, capabilities) locally. Otherwise,
    /// the child may learn about external "progress" corresponding to its own actions, and without
    /// noting the consequences of those actions, will be in a bit of a pickle.
    ///
    /// Note: Callee is expected to consume the contents of _external to indicate acknowledgement.
    fn push_external_progress(&mut self, external: &mut [CountMap<T>]) {
        // default implementation just drains the external updates
        for updates in external.iter_mut() {
            updates.clear();
        }
    }

    /// Retrieves a summary of progress statements internal to the operator.
    ///
    /// Returns a bool indicating if there is any unreported work remaining (e.g. work that doesn't
    /// project on an output).
    ///
    /// Note: not "internal to the operator and its peer group". The operator instance should only
    /// report progress performed by its own instance. The parent scope will figure out what to do
    /// with this information (mostly likely exchange it with its peers). There does seem to be the
    /// opportunity to optimize this, but it may complicate the life of the parent to know which of
    /// its children are reporting partial information and which are complete.
    fn pull_internal_progress(&mut self, consumed: &mut [CountMap<T>],          // to populate
                                         internal: &mut [CountMap<T>],          // to populate
                                         produced: &mut [CountMap<T>]) -> bool; // to populate

    /// A descripitive name for the operator
    fn name(&self) -> String;

    /// Indicates of whether the operator requires `push_external_progress` information or not.
    fn notify_me(&self) -> bool { true }
}