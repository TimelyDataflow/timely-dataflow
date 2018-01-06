//! Manages pointstamp counts (timestamp, location) within a sub operator.

use progress::Timestamp;
use progress::nested::{Source, Target};
use progress::ChangeBatch;

use progress::frontier::Antichain;
use progress::timestamp::PathSummary;
use order::PartialOrder;

/// Represents changes to pointstamps before and after transmission along a scope's topology.
#[derive(Default)]
pub struct PointstampCounter<T:Timestamp> {

    /// Buffers of observed changes.
    source:  Vec<Vec<ChangeBatch<T>>>,
    target:  Vec<Vec<ChangeBatch<T>>>,

    /// Buffers of consequent propagated changes.
    pushed:  Vec<Vec<ChangeBatch<T>>>,

    /// Compiled source to target reachability along edges and through internal connections.
    source_target: Vec<Vec<Vec<(Target, Antichain<T::Summary>)>>>,
    /// Compiled target to target reachability along edges and through internal connections.
    target_target: Vec<Vec<Vec<(Target, Antichain<T::Summary>)>>>,
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

    /// Allocate a new `PointstampCounter` using the shape from `summaries`.
    pub fn allocate_from(
        source_target: Vec<Vec<Vec<(Target, Antichain<T::Summary>)>>>,
        target_target: Vec<Vec<Vec<(Target, Antichain<T::Summary>)>>>) -> Self {

        let mut sources = Vec::with_capacity(source_target.len());
        let mut targets = Vec::with_capacity(target_target.len());
        let mut pushed = Vec::with_capacity(source_target.len());

        // Allocate buffer space for each input and input port.
        for source in 0 .. source_target.len() {
            sources.push(vec![ChangeBatch::new(); source_target[source].len()]);
            pushed.push(vec![ChangeBatch::new(); source_target[source].len()]);
        }

        // Allocate buffer space for each output and output port.
        for target in 0 .. target_target.len() {
            targets.push(vec![ChangeBatch::new(); target_target[target].len()]);
        }

        PointstampCounter {
            source: sources,
            target: targets,
            pushed,
            source_target,
            target_target,
        }
    }

    /// Propagate updates made to sources and targets.
    ///
    /// This method processes all updates made through `update_target` and `update_source`,
    /// and moves their consequences to buffers available through `self.pushed_mut(node)`.
    pub fn push_pointstamps(&mut self) {

        // push pointstamps from targets to targets.
        for index in 0..self.target.len() {
            for input in 0..self.target[index].len() {
                for (time, value) in self.target[index][input].drain() {
                    for &(target, ref antichain) in &self.target_target[index][input] {
                        for summary in antichain.elements().iter() {
                            if let Some(new_time) = summary.results_in(&time) {
                                self.pushed[target.index][target.port].update(new_time, value);
                            }
                        }
                    }
                }
            }
        }

        // push pointstamps from sources to targets.
        for index in 0..self.source.len() {
            for output in 0..self.source[index].len() {
                for (time, value) in self.source[index][output].drain() {
                    for &(target, ref antichain) in &self.source_target[index][output] {
                        for summary in antichain.elements().iter() {
                            if let Some(new_time) = summary.results_in(&time) {
                                self.pushed[target.index][target.port].update(new_time, value);
                            }
                        }
                    }
                }
            }
        }
    }

    /// Provides access to pushed changes for a node.
    ///
    /// The caller may read the results or consume the results, as appropriate. The method
    /// itself does not clear the buffer, so pushed values will stay in place until they are
    /// consumed by some caller.
    pub fn pushed_mut(&mut self, node: usize) -> &mut [ChangeBatch<T>] {
        &mut self.pushed[node][..]
    }
}


/// A propagator that compiles all paths and projects all changes along them.
#[derive(Clone, Debug)]
pub struct Builder<T: Timestamp> {
    /// Internal connections within hosted operators.
    ///
    /// Indexed by operator index, then input port, then output port. This is the
    /// same format returned by `get_internal_summary`, as if we simply appended
    /// all of the summaries for the hosted nodes.
    nodes: Vec<Vec<Vec<Antichain<T::Summary>>>>,
    /// Direct connections from sources to targets. 
    ///
    /// Edges do not affect timestamps, so we only need to know the connectivity.
    /// Indexed by operator index then output port.
    edges: Vec<Vec<Vec<Target>>>,
}

impl<T: Timestamp> Builder<T> {

    /// Add links internal to operators.
    ///
    /// This method overwrites any existing summary, instead of anything more sophisticated.
    pub fn add_node(&mut self, index: usize, summary: Vec<Vec<Antichain<T::Summary>>>) {
        while self.nodes.len() <= index { self.nodes.push(Vec::new()); }
        self.nodes[index] = summary;
    }

    /// Add links between operators.
    ///
    /// This method does not check that the associated nodes and ports exist. References to
    /// missing nodes or ports are discovered in `build`.
    pub fn add_edge(&mut self, source: Source, target: Target) {
        while self.edges.len() <= source.index { self.edges.push(Vec::new()); }
        while self.edges[source.index].len() <= source.port { self.edges[source.index].push(Vec::new()); }
        self.edges[source.index][source.port].push(target);
    }

    /// Compiles the current nodes and edges into immutable path summaries.
    ///
    /// This method has the opportunity to perform some error checking that the path summaries
    /// are valid, including references to undefined nodes and ports, as well as self-loops with
    /// default summaries (a serious liveness issue).
    pub fn build(&mut self) -> PointstampCounter<T> {

        // We maintain a list of new ((source, target), path_summary) entries whose implications 
        // have not yet been fully explored. While such entries exist, we consider the next and 
        // explore its implications by considering all incident target-source' connections (from
        // `self.nodes`) followed by all source'-target' connections (from `self.edges`). This may
        // yield ((source, target'), path_summary) entries, and we enqueue any new ones in our list.
        let mut work = ::std::collections::VecDeque::<((Source, Target), T::Summary)>::new();

        // Initialize `work` with all edges in the graph, each with a `Default::default()` summary.
        for index in 0 .. self.edges.len() {
            for port in 0 .. self.edges[index].len() {
                for &target in &self.edges[index][port] {
                    work.push_back(((Source { index: index, port: port}, target), Default::default()));
                }
            }
        }

        // Establish all source-target path summaries by fixed-point computation.
        let mut source_target: Vec<Vec<Vec<(Target, Antichain<T::Summary>)>>> = Vec::new();
        while let Some(((source, target), summary)) = work.pop_front() {
            // try to add the summary, and if it comes back as "novel" we should explore its two-hop connections.
            if try_to_add_summary(&mut source_target[source.index][source.port], target, summary.clone()) {
                for (new_source_port, internal_summaries) in self.nodes[target.index][target.port].iter().enumerate() {
                    for internal_summary in internal_summaries.elements() {
                        if let Some(new_summary) = summary.followed_by(internal_summary) {
                            for &new_target in self.edges[target.index][new_source_port].iter() {
                                work.push_back(((source, new_target), new_summary.clone()));
                            }
                        }
                    }
                }
            }
        }

        // Extend source-target path summaries by one target'-source connection, to yield all 
        // target'-target path summaries.
        let mut target_target: Vec<Vec<Vec<(Target, Antichain<T::Summary>)>>> = Vec::new();
        for index in 0 .. self.nodes.len() {
            for input_port in 0 .. self.nodes[index].len() {
                // install a self-self default path summary, as we want pointstamps changes at
                // this target to be observed here too.
                try_to_add_summary(
                    &mut target_target[index][input_port], 
                    Target { index: index, port: input_port }, 
                    Default::default()
                );

                // for each output port, consider source-target summaries.
                for (output_port, internal_summaries) in self.nodes[index][input_port].iter().enumerate() {
                    for internal_summary in internal_summaries.elements() {
                        for &(target, ref new_summaries) in source_target[index][output_port].iter() {
                            for new_summary in new_summaries.elements() {
                                if let Some(summary) = internal_summary.followed_by(new_summary) {
                                    try_to_add_summary(&mut target_target[index][input_port], target, summary);
                                }
                            }
                        }
                    }
                }
            }
        }

        PointstampCounter::allocate_from(source_target, target_target)
    }
}

fn try_to_add_summary<S: PartialOrder+Eq>(vector: &mut Vec<(Target, Antichain<S>)>, target: Target, summary: S) -> bool {
    for &mut (ref t, ref mut antichain) in vector.iter_mut() {
        // TODO : Do we need to clone here, or should `insert` be smarter?
        if target.eq(t) { return antichain.insert(summary); }
    }
    vector.push((target, Antichain::from_elem(summary)));
    true
}