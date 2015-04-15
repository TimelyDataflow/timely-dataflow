use progress::Timestamp;
use progress::nested::subgraph::Source::{GraphInput, ScopeOutput};
use progress::nested::subgraph::Target::ScopeInput;
use progress::nested::{Source, Target};

use progress::count_map::CountMap;

#[derive(Default)]
pub struct PointstampCounter<T:Timestamp> {
    pub source_counts:  Vec<Vec<CountMap<T>>>,    // timestamp updates indexed by (scope, output)
    pub target_counts:  Vec<Vec<CountMap<T>>>,    // timestamp updates indexed by (scope, input)
    pub input_counts:   Vec<CountMap<T>>,         // timestamp updates indexed by input_port
    pub target_pushed:  Vec<Vec<CountMap<T>>>,    // pushed updates indexed by (scope, input)
    pub output_pushed:  Vec<CountMap<T>>,         // pushed updates indexed by output_port
}

impl<T:Timestamp> PointstampCounter<T> {
    //#[inline(always)]
    pub fn update_target(&mut self, target: Target, time: &T, value: i64) {
        if let ScopeInput(scope, input) = target { self.target_counts[scope as usize][input as usize].update(time, value); }
        else                                     { println!("lolwut?"); } // no graph outputs as pointstamps
    }

    pub fn update_source(&mut self, source: Source, time: &T, value: i64) {
        match source {
            ScopeOutput(scope, output) => { self.source_counts[scope as usize][output as usize].update(time, value); },
            GraphInput(input)          => { self.input_counts[input as usize].update(time, value); },
        }
    }
    pub fn clear_pushed(&mut self) {
        for vec in self.target_pushed.iter_mut() { for map in vec.iter_mut() { map.clear(); } }
        for map in self.output_pushed.iter_mut() { map.clear(); }
    }
}
