//! Manages pointstamp counts (timestamp, location) within a sub operator.

use progress::Timestamp;
use progress::nested::{Source, Target};
use progress::ChangeBatch;

/// Represents changes to pointstamps before and after transmission along a scope's topology.
#[derive(Default)]
pub struct PointstampCounter<T:Timestamp> {
    /// timestamp updates indexed by (scope, output)
    pub source:  Vec<Vec<ChangeBatch<T>>>,
    /// timestamp updates indexed by (scope, input)
    pub target:  Vec<Vec<ChangeBatch<T>>>,
    /// pushed updates indexed by (scope, input)
    pub pushed:  Vec<Vec<ChangeBatch<T>>>,
}

impl<T:Timestamp> PointstampCounter<T> {
    /// Updates the count for a time at a target.
    pub fn update_target(&mut self, target: Target, time: T, value: i64) {
        self.target[target.index][target.port].update(time, value);
    }
    /// Updates the count for a time at a source.
    pub fn update_source(&mut self, source: Source, time: T, value: i64) {
        self.source[source.index][source.port].update(time, value);
    }
    /// Clears the pointstamp counter.
    pub fn clear(&mut self) {
        for vec in &mut self.source { for map in vec.iter_mut() { map.clear(); } }
        for vec in &mut self.target { for map in vec.iter_mut() { map.clear(); } }
        for vec in &mut self.pushed { for map in vec.iter_mut() { map.clear(); } }
    }
    /// Allocates internal state given an operator's inputs and outputs.
    pub fn allocate_for_operator(&mut self, inputs: usize, outputs: usize) {
        self.pushed.push(vec![ChangeBatch::new(); inputs]);
        self.target.push(vec![ChangeBatch::new(); inputs]);
        self.source.push(vec![ChangeBatch::new(); outputs]);
    }
}
