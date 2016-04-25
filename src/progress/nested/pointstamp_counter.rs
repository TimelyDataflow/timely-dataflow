//! Manages pointstamp counts (timestamp, location) within a sub operator.

use progress::Timestamp;
use progress::nested::{Source, Target};

use progress::count_map::CountMap;

#[derive(Default)]
pub struct PointstampCounter<T:Timestamp> {
    pub source:  Vec<Vec<CountMap<T>>>,    // timestamp updates indexed by (scope, output)
    pub target:  Vec<Vec<CountMap<T>>>,    // timestamp updates indexed by (scope, input)
    pub pushed:  Vec<Vec<CountMap<T>>>,    // pushed updates indexed by (scope, input)
}

impl<T:Timestamp> PointstampCounter<T> {
    pub fn update_target(&mut self, target: Target, time: &T, value: i64) {
        self.target[target.index][target.port].update(time, value);
    }
    pub fn update_source(&mut self, source: Source, time: &T, value: i64) {
        self.source[source.index][source.port].update(time, value);
    }
    pub fn clear(&mut self) {
        for vec in &mut self.source { for map in vec.iter_mut() { map.clear(); } }
        for vec in &mut self.target { for map in vec.iter_mut() { map.clear(); } }
        for vec in &mut self.pushed { for map in vec.iter_mut() { map.clear(); } }
    }

    pub fn allocate_for_operator(&mut self, inputs: usize, outputs: usize) {
        self.pushed.push(vec![Default::default(); inputs]);
        self.target.push(vec![Default::default(); inputs]);
        self.source.push(vec![Default::default(); outputs]);
    }
}
