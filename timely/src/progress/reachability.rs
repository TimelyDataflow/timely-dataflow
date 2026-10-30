//! Manages pointstamp reachability within a timely dataflow graph.
//!
//! Timely dataflow is concerned with understanding and communicating the potential
//! for capabilities to reach nodes in a directed graph, by following paths through
//! the graph (along edges and through nodes). This module contains one abstraction
//! for managing this information.
//!
//! # Examples
//!
//! ```rust
//! use timely::progress::{Location, Port};
//! use timely::progress::frontier::Antichain;
//! use timely::progress::{Source, Target};
//! use timely::progress::reachability::{Builder, Tracker};
//!
//! // allocate a new empty topology builder.
//! let mut builder = Builder::<usize>::new();
//!
//! // Each node with one input connected to one output.
//! builder.add_node(0, 1, 1, vec![[(0, Antichain::from_elem(0))].into_iter().collect()]);
//! builder.add_node(1, 1, 1, vec![[(0, Antichain::from_elem(0))].into_iter().collect()]);
//! builder.add_node(2, 1, 1, vec![[(0, Antichain::from_elem(1))].into_iter().collect()]);
//!
//! // Connect nodes in sequence, looping around to the first from the last.
//! builder.add_edge(Source::new(0, 0), Target::new(1, 0));
//! builder.add_edge(Source::new(1, 0), Target::new(2, 0));
//! builder.add_edge(Source::new(2, 0), Target::new(0, 0));
//!
//! // Construct a reachability tracker.
//! let (mut tracker, _) = builder.build(None);
//!
//! // Introduce a pointstamp at the output of the first node.
//! tracker.update_source(Source::new(0, 0), 17, 1);
//!
//! // Propagate changes; until this call updates are simply buffered.
//! tracker.propagate_all();
//!
//! let mut results =
//! tracker
//!     .pushed()
//!     .0
//!     .drain(..)
//!     .filter(|((location, time), delta)| location.is_target())
//!     .collect::<Vec<_>>();
//!
//! results.sort();
//!
//! println!("{:?}", results);
//!
//! assert_eq!(results.len(), 3);
//! assert_eq!(results[0], ((Location::new_target(0, 0), 18), 1));
//! assert_eq!(results[1], ((Location::new_target(1, 0), 17), 1));
//! assert_eq!(results[2], ((Location::new_target(2, 0), 17), 1));
//!
//! // Introduce a pointstamp at the output of the first node.
//! tracker.update_source(Source::new(0, 0), 17, -1);
//!
//! // Propagate changes; until this call updates are simply buffered.
//! tracker.propagate_all();
//!
//! let mut results =
//! tracker
//!     .pushed()
//!     .0
//!     .drain(..)
//!     .filter(|((location, time), delta)| location.is_target())
//!     .collect::<Vec<_>>();
//!
//! results.sort();
//!
//! assert_eq!(results.len(), 3);
//! assert_eq!(results[0], ((Location::new_target(0, 0), 18), -1));
//! assert_eq!(results[1], ((Location::new_target(1, 0), 17), -1));
//! assert_eq!(results[2], ((Location::new_target(2, 0), 17), -1));
//! ```

use std::collections::BinaryHeap;
use std::cmp::Reverse;

use columnar::{Vecs, Index as ColumnarIndex};

use crate::progress::Timestamp;
use crate::progress::{Source, Target};
use crate::progress::ChangeBatch;
use crate::progress::{Location, Port};
use crate::progress::operate::{Connectivity, PortConnectivity, PortConnectivityBuilder};
use crate::progress::frontier::{Antichain, MutableAntichain};
use crate::progress::timestamp::PathSummary;

/// Build a `Vecs<Vecs<Vec<S>>>` from nested iterators.
///
/// The outer iterator yields nodes, each node yields ports, each port yields data items.
fn build_nested_vecs<S>(nodes: impl Iterator<Item = impl Iterator<Item = impl Iterator<Item = S>>>) -> Vecs<Vecs<Vec<S>>> {
    let mut result: Vecs<Vecs<Vec<S>>> = Default::default();
    for node in nodes {
        for port in node {
            result.values.push_iter(port);
        }
        result.bounds.push(result.values.bounds.len() as u64);
    }
    result
}

/// A topology builder, which can summarize reachability along paths.
///
/// A `Builder` takes descriptions of the nodes and edges in a graph, and compiles
/// a static summary of the minimal actions a timestamp must endure going from any
/// input or output port to a destination input port.
///
/// A graph is provides as (i) several indexed nodes, each with some number of input
/// and output ports, and each with a summary of the internal paths connecting each
/// input to each output, and (ii) a set of edges connecting output ports to input
/// ports. Edges do not adjust timestamps; only nodes do this.
///
/// The resulting summary describes, for each origin port in the graph and destination
/// input port, a set of incomparable path summaries, each describing what happens to
/// a timestamp as it moves along the path. There may be multiple summaries for each
/// part of origin and destination due to the fact that the actions on timestamps may
/// not be totally ordered (e.g., "increment the timestamp" and "take the maximum of
/// the timestamp and seven").
///
/// # Examples
///
/// ```rust
/// use timely::progress::frontier::Antichain;
/// use timely::progress::{Source, Target};
/// use timely::progress::reachability::Builder;
///
/// // allocate a new empty topology builder.
/// let mut builder = Builder::<usize>::new();
///
/// // Each node with one input connected to one output.
/// builder.add_node(0, 1, 1, vec![[(0, Antichain::from_elem(0))].into_iter().collect()]);
/// builder.add_node(1, 1, 1, vec![[(0, Antichain::from_elem(0))].into_iter().collect()]);
/// builder.add_node(2, 1, 1, vec![[(0, Antichain::from_elem(1))].into_iter().collect()]);
///
/// // Connect nodes in sequence, looping around to the first from the last.
/// builder.add_edge(Source::new(0, 0), Target::new(1, 0));
/// builder.add_edge(Source::new(1, 0), Target::new(2, 0));
/// builder.add_edge(Source::new(2, 0), Target::new(0, 0));
///
/// // Summarize reachability information.
/// let (tracker, _) = builder.build(None);
/// ```
#[derive(Clone, Debug)]
pub struct Builder<T: Timestamp> {
    /// Internal connections within hosted operators.
    ///
    /// Indexed by operator index, then input port, then output port. This is the
    /// same format returned by `initialize`, as if we simply appended
    /// all of the summaries for the hosted nodes.
    pub nodes: Vec<Connectivity<T::Summary>>,
    /// Direct connections from sources to targets.
    ///
    /// Edges do not affect timestamps, so we only need to know the connectivity.
    /// Indexed by operator index then output port.
    pub edges: Vec<Vec<Vec<Target>>>,
    /// Numbers of inputs and outputs for each node.
    pub shape: Vec<(usize, usize)>,
}

impl<T: Timestamp> Builder<T> {

    /// Create a new empty topology builder.
    pub fn new() -> Self {
        Builder {
            nodes: Vec::new(),
            edges: Vec::new(),
            shape: Vec::new(),
        }
    }

    /// Add links internal to operators.
    ///
    /// This method overwrites any existing summary, instead of anything more sophisticated.
    pub fn add_node(&mut self, index: usize, inputs: usize, outputs: usize, summary: Connectivity<T::Summary>) {

        // Assert that all summaries exist.
        debug_assert_eq!(inputs, summary.len());
        debug_assert!(summary.iter().all(|os| os.iter_ports().all(|(o,_)| o < outputs)));

        while self.nodes.len() <= index {
            self.nodes.push(Vec::new());
            self.edges.push(Vec::new());
            self.shape.push((0, 0));
        }

        self.nodes[index] = summary;
        if self.edges[index].len() != outputs {
            self.edges[index] = vec![Vec::new(); outputs];
        }
        self.shape[index] = (inputs, outputs);
    }

    /// Add links between operators.
    ///
    /// This method does not check that the associated nodes and ports exist. References to
    /// missing nodes or ports are discovered in `build`.
    pub fn add_edge(&mut self, source: Source, target: Target) {

        // Assert that the edge is between existing ports.
        debug_assert!(source.port < self.shape[source.node].1);
        debug_assert!(target.port < self.shape[target.node].0);

        self.edges[source.node][source.port].push(target);
    }

    /// Compiles the current nodes and edges into immutable path summaries.
    ///
    /// This method has the opportunity to perform some error checking that the path summaries
    /// are valid, including references to undefined nodes and ports, as well as self-loops with
    /// default summaries (a serious liveness issue).
    ///
    /// The optional logger information is baked into the resulting tracker.
    pub fn build(self, logger: Option<logging::TrackerLogger<T>>) -> (Tracker<T>, Connectivity<T::Summary>) {

        if !self.is_acyclic() {
            println!("Cycle detected without timestamp increment");
            println!("{:?}", self);
        }

        Tracker::allocate_from(self, logger)
    }

    /// Tests whether the graph includes a cycle of default path summaries.
    ///
    /// Graphs containing cycles of default path summaries will most likely
    /// not work well with progress tracking, as a timestamp can result in
    /// itself. Such computations can still *run*, but one should not block
    /// on frontier information before yielding results, as you many never
    /// unblock.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use timely::progress::frontier::Antichain;
    /// use timely::progress::{Source, Target};
    /// use timely::progress::reachability::Builder;
    ///
    /// // allocate a new empty topology builder.
    /// let mut builder = Builder::<usize>::new();
    ///
    /// // Each node with one input connected to one output.
    /// builder.add_node(0, 1, 1, vec![[(0, Antichain::from_elem(0))].into_iter().collect()]);
    /// builder.add_node(1, 1, 1, vec![[(0, Antichain::from_elem(0))].into_iter().collect()]);
    /// builder.add_node(2, 1, 1, vec![[(0, Antichain::from_elem(0))].into_iter().collect()]);
    ///
    /// // Connect nodes in sequence, looping around to the first from the last.
    /// builder.add_edge(Source::new(0, 0), Target::new(1, 0));
    /// builder.add_edge(Source::new(1, 0), Target::new(2, 0));
    ///
    /// assert!(builder.is_acyclic());
    ///
    /// builder.add_edge(Source::new(2, 0), Target::new(0, 0));
    ///
    /// assert!(!builder.is_acyclic());
    /// ```
    ///
    /// This test exists because it is possible to describe dataflow graphs that
    /// do not contain non-incrementing cycles, but without feedback nodes that
    /// strictly increment timestamps. For example,
    ///
    /// ```rust
    /// use timely::progress::frontier::Antichain;
    /// use timely::progress::{Source, Target};
    /// use timely::progress::reachability::Builder;
    ///
    /// // allocate a new empty topology builder.
    /// let mut builder = Builder::<usize>::new();
    ///
    /// // Two inputs and outputs, only one of which advances.
    /// builder.add_node(0, 2, 2, vec![
    ///     [(0,Antichain::from_elem(0)),(1,Antichain::new())].into_iter().collect(),
    ///     [(0,Antichain::new()),(1,Antichain::from_elem(1))].into_iter().collect(),
    /// ]);
    ///
    /// // Connect each output to the opposite input.
    /// builder.add_edge(Source::new(0, 0), Target::new(0, 1));
    /// builder.add_edge(Source::new(0, 1), Target::new(0, 0));
    ///
    /// assert!(builder.is_acyclic());
    /// ```
    pub fn is_acyclic(&self) -> bool {

        // Dense per-location in-degree counts, with each node's targets and
        // then sources laid out contiguously at a per-node offset.
        let mut offsets = Vec::with_capacity(self.shape.len());
        let mut locations = 0;
        for (targets, sources) in self.shape.iter() {
            offsets.push(locations);
            locations += targets + sources;
        }
        let index_of = |location: &Location| {
            let (targets, _) = self.shape[location.node];
            match location.port {
                Port::Target(port) => offsets[location.node] + port,
                Port::Source(port) => offsets[location.node] + targets + port,
            }
        };
        let mut in_degree = vec![0usize; locations];

        // Load edges as default summaries.
        for ports in self.edges.iter() {
            for targets in ports.iter() {
                for &target in targets.iter() {
                    in_degree[index_of(&Location::from(target))] += 1;
                }
            }
        }

        // Load default intra-node summaries.
        for (index, summary) in self.nodes.iter().enumerate() {
            for outputs in summary.iter() {
                for (output, summaries) in outputs.iter_ports() {
                    let source = Location::new_source(index, output);
                    for summary in summaries.elements().iter() {
                        if summary == &Default::default() {
                            in_degree[index_of(&source)] += 1;
                        }
                    }
                }
            }
        }

        // A worklist of nodes that cannot be reached from the whole graph.
        // Initially this list contains locations with no incoming edges, but
        // as the algorithm develops we add to it any locations that can only
        // be reached by nodes that have been on this list.
        let mut remaining = in_degree.iter().filter(|count| **count > 0).count();
        let mut worklist = Vec::with_capacity(locations);
        for (node, &(targets, sources)) in self.shape.iter().enumerate() {
            for port in 0 .. targets {
                let location = Location::new_target(node, port);
                if in_degree[index_of(&location)] == 0 { worklist.push(location); }
            }
            for port in 0 .. sources {
                let location = Location::new_source(node, port);
                if in_degree[index_of(&location)] == 0 { worklist.push(location); }
            }
        }

        // Repeatedly remove nodes and update adjacent in-edges.
        while let Some(Location { node, port }) = worklist.pop() {
            match port {
                Port::Source(port) => {
                    for target in self.edges[node][port].iter() {
                        let target = Location::from(*target);
                        let index = index_of(&target);
                        in_degree[index] -= 1;
                        if in_degree[index] == 0 {
                            remaining -= 1;
                            worklist.push(target);
                        }
                    }
                },
                Port::Target(port) => {
                    for (output, summaries) in self.nodes[node][port].iter_ports() {
                        let source = Location::new_source(node, output);
                        let index = index_of(&source);
                        for summary in summaries.elements().iter() {
                            if summary == &Default::default() {
                                in_degree[index] -= 1;
                                if in_degree[index] == 0 {
                                    remaining -= 1;
                                    worklist.push(source);
                                }
                            }
                        }
                    }
                },
            }
        }

        // Acyclic graphs should drain every positive in-degree to zero.
        remaining == 0
    }
}

impl<T: Timestamp> Default for Builder<T> {
    fn default() -> Self {
        Self::new()
    }
}

/// An interactive tracker of propagated reachability information.
///
/// A `Tracker` tracks, for a fixed graph topology, the implications of
/// pointstamp changes at various node input and output ports. These changes may
/// alter the potential pointstamps that could arrive at downstream input ports.
pub struct Tracker<T:Timestamp> {

    /// Internal operator connectivity, columnar form of `Vec<Vec<PortConnectivity<T::Summary>>>`.
    /// Indexed by `(node, input_port)` to yield `(output_port, summary)` pairs.
    nodes: Vecs<Vecs<Vec<(usize, T::Summary)>>>,
    /// Edge connectivity, columnar form of `Vec<Vec<Vec<Target>>>`.
    /// Indexed by `(node, output_port)` to yield target slices.
    edges: Vecs<Vecs<Vec<Target>>>,

    /// Summaries from each target (operator input) to scope outputs.
    /// Indexed by `(node, target_port)` to yield `(scope_output, summary)` pairs.
    target_summaries: Vecs<Vecs<Vec<(usize, T::Summary)>>>,
    /// Summaries from each source (operator output) to scope outputs.
    /// Indexed by `(node, source_port)` to yield `(scope_output, summary)` pairs.
    source_summaries: Vecs<Vecs<Vec<(usize, T::Summary)>>>,

    /// Each source and target has a mutable antichain to ensure that we track their discrete frontiers,
    /// rather than their multiplicities. We separately track the frontiers resulting from propagated
    /// frontiers, to protect them from transient negativity in inbound target updates.
    per_operator: Vec<PerOperator<T>>,

    /// Source and target changes are buffered, which allows us to delay processing until propagation,
    /// and so consolidate updates, but to leap directly to those frontiers that may have changed.
    pending_changes: LocationChanges<T>,

    /// Worklist of changes to implications to perform, by location, with
    /// locations visited in an order consistent with the graph's edges.
    worklist: Worklist<T>,

    /// Buffer of consequent changes: the net change to the frontier at each location.
    pushed_changes: Vec<((Location, T), i64)>,
    /// The changes to the frontiers in the cyclic component being visited, to consolidate.
    cycle_changes: LocationChanges<T>,

    /// Compiled summaries from each internal location (not scope inputs) to each scope output.
    output_changes: Vec<ChangeBatch<T>>,

    /// A non-negative sum of post-filtration input changes.
    ///
    /// This sum should be zero exactly when the accumulated input changes are zero,
    /// indicating that the progress tracker is currently tracking nothing. It should
    /// always be exactly equal to the sum across all operators of the frontier sizes
    /// of the target and source `pointstamps` member.
    total_counts: i64,

    /// Optionally, a unique logging identifier and logging for tracking events.
    logger: Option<logging::TrackerLogger<T>>,
}

/// Target and source information for each operator.
pub struct PerOperator<T: Timestamp> {
    /// Port information for each target.
    pub targets: Vec<PortInformation<T>>,
    /// Port information for each source.
    pub sources: Vec<PortInformation<T>>,
    /// Sum across outputs of capabilities.
    pub cap_counts: i64,
}

impl<T: Timestamp> PerOperator<T> {
    /// A new PerOperator bundle from numbers of input and output ports.
    pub fn new(inputs: usize, outputs: usize) -> Self {
        PerOperator {
            targets: vec![PortInformation::new(); inputs],
            sources: vec![PortInformation::new(); outputs],
            cap_counts: 0,
        }
    }
}

/// Per-port progress-tracking information.
#[derive(Clone)]
pub struct PortInformation<T: Timestamp> {
    /// Current counts of active pointstamps.
    pub pointstamps: MutableAntichain<T>,
    /// Current implications of active pointstamps across the dataflow.
    pub implications: MutableAntichain<T>,
}

impl<T: Timestamp> PortInformation<T> {
    /// Creates empty port information.
    pub fn new() -> Self {
        PortInformation {
            pointstamps: MutableAntichain::new(),
            implications: MutableAntichain::new(),
        }
    }

    /// Returns `true` if updates at this pointstamp uniquely block progress.
    ///
    /// This method returns `true` if the currently maintained pointstamp
    /// counts are such that zeroing out outstanding updates at *this*
    /// pointstamp would change the frontiers at this operator. When the
    /// method returns `false` it means that, temporarily at least, there
    /// are outstanding pointstamp updates that are strictly less than
    /// this pointstamp.
    #[inline]
    pub fn is_global(&self, time: &T) -> bool {
        let dominated = self.implications.frontier().iter().any(|t| t.less_than(time));
        let redundant = self.implications.count_for(time) > 1;
        !dominated && !redundant
    }
}

impl<T: Timestamp> Default for PortInformation<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T:Timestamp> Tracker<T> {

    /// Updates the count for a time at a location.
    #[inline]
    pub fn update(&mut self, location: Location, time: T, value: i64) {
        match location.port {
            Port::Target(port) => self.update_target(Target::new(location.node, port), time, value),
            Port::Source(port) => self.update_source(Source::new(location.node, port), time, value),
        };
    }

    /// Updates the count for a time at a target (operator input, scope output).
    #[inline]
    pub fn update_target(&mut self, target: Target, time: T, value: i64) {
        self.pending_changes.update(Location::from(target), time, value);
    }
    /// Updates the count for a time at a source (operator output, scope input).
    #[inline]
    pub fn update_source(&mut self, source: Source, time: T, value: i64) {
        self.pending_changes.update(Location::from(source), time, value);
    }

    /// Indicates if any pointstamps have positive count.
    pub fn tracking_anything(&mut self) -> bool {
        !self.pending_changes.is_empty() ||
        self.total_counts > 0
    }

    /// Allocate a new `Tracker` using the shape from `summaries`.
    ///
    /// The result is a pair of tracker, and the summaries from each input port to each
    /// output port.
    ///
    /// If the optional logger is provided, it will be used to log various tracker events.
    pub fn allocate_from(builder: Builder<T>, logger: Option<logging::TrackerLogger<T>>) -> (Self, Connectivity<T::Summary>) {

        // Allocate buffer space for each input and input port.
        let per_operator =
        builder
            .shape
            .iter()
            .map(|&(inputs, outputs)| PerOperator::new(inputs, outputs))
            .collect::<Vec<_>>();

        // The order in which to visit locations when propagating changes.
        let worklist = Worklist::new(&builder.shape, &builder.nodes, &builder.edges);

        // Summary of scope inputs to scope outputs.
        let mut builder_summary = vec![PortConnectivity::default(); builder.shape[0].1];

        // Compile summaries from each location to each scope output.
        // Collect into per-node, per-port buckets for flattening.
        let output_summaries = summarize_outputs::<T>(&builder.nodes, &builder.edges);

        // Temporary storage: target_sum[node][port] and source_sum[node][port].
        let mut target_sum: Vec<Vec<PortConnectivity<T::Summary>>> = builder.shape.iter()
            .map(|&(inputs, _)| vec![PortConnectivity::default(); inputs])
            .collect();
        let mut source_sum: Vec<Vec<PortConnectivity<T::Summary>>> = builder.shape.iter()
            .map(|&(_, outputs)| vec![PortConnectivity::default(); outputs])
            .collect();

        for (location, summaries) in output_summaries.into_iter() {
            // Summaries from scope inputs are useful in summarizing the scope.
            if location.node == 0 {
                if let Port::Source(port) = location.port {
                    builder_summary[port] = summaries;
                }
                else {
                    // Ignore (ideally trivial) output to output summaries.
                }
            }
            // Summaries from internal nodes are important for projecting capabilities.
            else {
                match location.port {
                    Port::Target(port) => {
                        target_sum[location.node][port] = summaries;
                    },
                    Port::Source(port) => {
                        source_sum[location.node][port] = summaries;
                    },
                }
            }
        }

        // Build columnar nodes: Vecs<Vecs<Vec<(usize, T::Summary)>>>.
        let nodes = build_nested_vecs(builder.nodes.into_iter().map(|connectivity| {
            connectivity.into_iter().map(|port_conn| {
                port_conn.into_iter().flat_map(|(port, antichain)| {
                    antichain.into_iter().map(move |s| (port, s))
                })
            })
        }));

        // Build columnar edges: Vecs<Vecs<Vec<Target>>>.
        let edges = build_nested_vecs(builder.edges.iter().map(|node_edges| {
            node_edges.iter().map(|port_edges| port_edges.iter().cloned())
        }));

        // Build columnar target and source summaries.
        let target_summaries = build_nested_vecs(target_sum.into_iter().map(|ports| {
            ports.into_iter().map(|port_conn| {
                port_conn.into_iter().flat_map(|(port, antichain)| {
                    antichain.into_iter().map(move |s| (port, s))
                })
            })
        }));
        let source_summaries = build_nested_vecs(source_sum.into_iter().map(|ports| {
            ports.into_iter().map(|port_conn| {
                port_conn.into_iter().flat_map(|(port, antichain)| {
                    antichain.into_iter().map(move |s| (port, s))
                })
            })
        }));

        let scope_outputs = builder.shape[0].0;
        let output_changes = vec![ChangeBatch::new(); scope_outputs];

        let tracker =
        Tracker {
            nodes,
            edges,
            target_summaries,
            source_summaries,
            per_operator,
            pending_changes: LocationChanges::new(&builder.shape),
            worklist,
            pushed_changes: Vec::new(),
            cycle_changes: LocationChanges::new(&builder.shape),
            output_changes,
            total_counts: 0,
            logger,
        };

        (tracker, builder_summary)
    }

    /// Propagates all pending updates.
    ///
    /// The method drains `self.input_changes` and circulates their implications
    /// until we cease deriving new implications.
    pub fn propagate_all(&mut self) {

        // Step 0: If logging is enabled, construct and log inbound changes.
        if let Some(logger) = &mut self.logger {

            let target_changes =
            self.pending_changes
                .iter()
                .filter_map(|(location, time, diff)| match location.port {
                    Port::Target(port) => Some((location.node, port, time, diff)),
                    Port::Source(_) => None,
                });

            logger.log_target_updates(target_changes);

            let source_changes =
            self.pending_changes
                .iter()
                .filter_map(|(location, time, diff)| match location.port {
                    Port::Source(port) => Some((location.node, port, time, diff)),
                    Port::Target(_) => None,
                });

            logger.log_source_updates(source_changes);
        }

        // Step 1: Drain `self.pending_changes` and determine actual frontier changes.
        //
        // Not all pending changes may alter the frontier at a location.
        // By filtering the changes through `self.pointstamps` we react only to discrete
        // changes in the frontier, rather than changes in the pointstamp counts that
        // witness that frontier.
        for (location, updates) in self.pending_changes.drain_locations() {
            let operator = &mut self.per_operator[location.node];
            match location.port {
                Port::Target(port) => {
                    let changes = operator.targets[port].pointstamps.update_iter(updates);
                    for (time, diff) in changes {
                        self.total_counts += diff;
                        for &(output, ref summary) in (&self.target_summaries).get(location.node).get(port).into_index_iter() {
                            if let Some(out_time) = summary.results_in(&time) {
                                self.output_changes[output].update(out_time, diff);
                            }
                        }
                        self.worklist.update(location, time, diff);
                    }
                }
                Port::Source(port) => {
                    let changes = operator.sources[port].pointstamps.update_iter(updates);
                    for (time, diff) in changes {
                        self.total_counts += diff;
                        operator.cap_counts += diff;
                        for &(output, ref summary) in (&self.source_summaries).get(location.node).get(port).into_index_iter() {
                            if let Some(out_time) = summary.results_in(&time) {
                                self.output_changes[output].update(out_time, diff);
                            }
                        }
                        self.worklist.update(location, time, diff);
                    }
                }
            }
        }

        // Step 2: Circulate implications of changes to `self.pointstamps`.
        //
        // Locations are visited by strongly connected component, in a
        // topological order of the components, so that every change that
        // can reach a location has reached it before it is visited. A
        // location outside any cycle is then visited once, and applies all
        // its changes, at whatever times, together: its frontier moves once,
        // directly from the old to the new, and its successors receive the
        // net change. Within a component with a cycle, changes are applied
        // in order of increasing time, as they reach each location.
        //
        // The order affects only the work done, not the result: each
        // location's implications accumulate the same changes, and the
        // changes pushed for it are consolidated, so they are the net change
        // to its frontier.
        while let Some(component) = self.worklist.pop_component() {
            if !self.worklist.cyclic(component) {
                let location = self.worklist.members(component)[0];
                let updates = self.worklist.take(location);
                let implications = match location.port {
                    Port::Target(port) => &mut self.per_operator[location.node].targets[port].implications,
                    Port::Source(port) => &mut self.per_operator[location.node].sources[port].implications,
                };
                // The location is visited once, so these are its net changes.
                for (time, diff) in implications.update_iter(updates) {
                    push_implication(&self.nodes, &self.edges, &mut self.worklist, location, &time, diff, None);
                    self.pushed_changes.push(((location, time), diff));
                }
                continue;
            }

            // A component with a cycle: its changes in order of increasing time.
            self.worklist.load_cycle(component);
            while let Some((at, location, change)) = self.worklist.pop_cycle() {
                let implications = match location.port {
                    Port::Target(port) => &mut self.per_operator[location.node].targets[port].implications,
                    Port::Source(port) => &mut self.per_operator[location.node].sources[port].implications,
                };
                for (time, diff) in implications.update_iter(Some((at, change))) {
                    push_implication(&self.nodes, &self.edges, &mut self.worklist, location, &time, diff, Some(component));
                    self.cycle_changes.update(location, time, diff);
                }
            }
            self.pushed_changes.extend(self.cycle_changes.drain());
        }
        self.worklist.clear();
    }

    /// Implications of maintained capabilities projected to each output.
    pub fn pushed_output(&mut self) -> &mut [ChangeBatch<T>] {
        &mut self.output_changes[..]
    }

    /// A mutable reference to the pushed results of changes.
    pub fn pushed(&mut self) -> (&mut Vec<((Location, T), i64)>, &[PerOperator<T>]) {
        (&mut self.pushed_changes, &self.per_operator)
    }

    /// Reveals per-operator frontier state.
    pub fn node_state(&self, index: usize) -> &PerOperator<T> {
        &self.per_operator[index]
    }

    /// Indicates if pointstamp is in the scope-wide frontier.
    ///
    /// Such a pointstamp would, if removed from `self.pointstamps`, cause a change
    /// to `self.implications`, which is what we track for per operator input frontiers.
    /// If the above do not hold, then its removal either 1. shouldn't be possible,
    /// or 2. will not affect the output of `self.implications`.
    pub fn is_global(&self, location: Location, time: &T) -> bool {
        match location.port {
            Port::Target(port) => self.per_operator[location.node].targets[port].is_global(time),
            Port::Source(port) => self.per_operator[location.node].sources[port].is_global(time),
        }
    }
}

/// Pushes the implications of a change of `diff` to `time` in the frontier
/// at `location` to its successors: across the operator, from an input, or
/// along the edges, from an output. A successor in the cyclic `component`
/// being visited receives it in time order; any other, when its component is
/// visited.
fn push_implication<T: Timestamp>(
    nodes: &Vecs<Vecs<Vec<(usize, T::Summary)>>>,
    edges: &Vecs<Vecs<Vec<Target>>>,
    worklist: &mut Worklist<T>,
    location: Location,
    time: &T,
    diff: i64,
    component: Option<usize>,
) {
    match location.port {
        // Propagate the change forward across the operator.
        Port::Target(port) => {
            for &(output, ref summary) in nodes.get(location.node).get(port).into_index_iter() {
                if let Some(new_time) = summary.results_in(time) {
                    worklist.push(Location::new_source(location.node, output), new_time, diff, component);
                }
            }
        }
        // Propagate the change forward along outgoing edges.
        Port::Source(port) => {
            for target in edges.get(location.node).get(port).into_index_iter() {
                worklist.push(Location::from(*target), time.clone(), diff, component);
            }
        }
    }
}

/// Changes to the implications at locations, waiting to be applied.
///
/// The locations are grouped into the strongly connected components of the
/// graph whose edges lead from each operator input to the outputs it may
/// reach, and from each output to the inputs it is connected to. The
/// components are numbered in a topological order, so a change at a location
/// can reach only locations in the same component or a later one.
struct Worklist<T: Timestamp> {
    /// The changes at each location.
    changes: LocationChanges<T>,
    /// The component of each location, indexed as `changes` indexes them.
    component: Vec<usize>,
    /// The locations of each component, those of component `c` being
    /// `members[starts[c] .. starts[c + 1]]`.
    members: Vec<Location>,
    /// The start in `members` of each component, and the number of locations.
    starts: Vec<usize>,
    /// The components with changes, possibly more than once each.
    components: BinaryHeap<Reverse<usize>>,
    /// The changes in the cyclic component being visited, by time and location.
    cycle: BinaryHeap<Reverse<(T, Location, i64)>>,
}

impl<T: Timestamp> Worklist<T> {
    /// A worklist for a graph with nodes of the `(inputs, outputs)` of
    /// `shape`, the internal connectivity of `nodes`, and the edges of `edges`.
    fn new(shape: &[(usize, usize)], nodes: &[Connectivity<T::Summary>], edges: &[Vec<Vec<Target>>]) -> Self {
        let changes = LocationChanges::new(shape);
        let locations = changes.slot.len();

        // Each location's successors, by index.
        let mut successors = vec![Vec::new(); locations];
        for (node, inputs) in nodes.iter().enumerate() {
            for (input, outputs) in inputs.iter().enumerate() {
                let from = changes.index(Location::new_target(node, input));
                for (output, summaries) in outputs.iter_ports() {
                    if !summaries.elements().is_empty() {
                        successors[from].push(changes.index(Location::new_source(node, output)));
                    }
                }
            }
        }
        for (node, outputs) in edges.iter().enumerate() {
            for (output, targets) in outputs.iter().enumerate() {
                let from = changes.index(Location::new_source(node, output));
                for &target in targets {
                    successors[from].push(changes.index(Location::from(target)));
                }
            }
        }

        // Each location by index.
        let mut location = Vec::with_capacity(locations);
        for (node, &(inputs, outputs)) in shape.iter().enumerate() {
            location.extend((0 .. inputs).map(|port| Location::new_target(node, port)));
            location.extend((0 .. outputs).map(|port| Location::new_source(node, port)));
        }

        // Tarjan's algorithm finds the components in reverse topological order.
        let found = strongly_connected_components(&successors);
        let count = found.len();
        let mut component = vec![0; locations];
        let mut members = Vec::with_capacity(locations);
        let mut starts = Vec::with_capacity(count + 1);
        for (number, scc) in found.iter().rev().enumerate() {
            starts.push(members.len());
            for &index in scc {
                component[index] = number;
                members.push(location[index]);
            }
        }
        starts.push(members.len());

        Worklist { changes, component, members, starts, components: BinaryHeap::new(), cycle: BinaryHeap::new() }
    }

    /// The locations of `component`.
    #[inline]
    fn members(&self, component: usize) -> &[Location] {
        &self.members[self.starts[component] .. self.starts[component + 1]]
    }

    /// Indicates that `component` has a cycle: more than one location, as
    /// no location is its own successor.
    #[inline]
    fn cyclic(&self, component: usize) -> bool {
        self.starts[component + 1] - self.starts[component] > 1
    }

    /// Adds a change of `diff` to `time` at `location`, to be applied in
    /// time order if `location` is in the cyclic `visiting` component, and
    /// otherwise when its component is visited.
    #[inline]
    fn push(&mut self, location: Location, time: T, diff: i64, visiting: Option<usize>) {
        let component = self.component[self.changes.index(location)];
        if Some(component) == visiting {
            self.cycle.push(Reverse((time, location, diff)));
        }
        else {
            self.update(location, time, diff);
        }
    }

    /// Adds a change of `diff` to `time` at `location`, to be applied when
    /// its component is visited.
    #[inline]
    fn update(&mut self, location: Location, time: T, diff: i64) {
        if self.changes.update(location, time, diff) {
            self.components.push(Reverse(self.component[self.changes.index(location)]));
        }
    }

    /// The first component with changes, in topological order.
    fn pop_component(&mut self) -> Option<usize> {
        let Reverse(component) = self.components.pop()?;
        while self.components.peek() == Some(&Reverse(component)) {
            self.components.pop();
        }
        Some(component)
    }

    /// Removes the changes at `location`, consolidated.
    fn take(&mut self, location: Location) -> smallvec::IntoIter<[(T, i64); 4]> {
        self.changes.take(location).into_inner().into_iter()
    }

    /// Moves the changes at the locations of `component` into the time-ordered `cycle`.
    fn load_cycle(&mut self, component: usize) {
        for index in self.starts[component] .. self.starts[component + 1] {
            let location = self.members[index];
            for (time, diff) in self.changes.take(location).into_inner() {
                self.cycle.push(Reverse((time, location, diff)));
            }
        }
    }

    /// The net change at the earliest time and location in the cyclic
    /// component being visited, skipping those with none.
    fn pop_cycle(&mut self) -> Option<(T, Location, i64)> {
        while let Some(Reverse((time, location, mut diff))) = self.cycle.pop() {
            // Drain and accumulate all updates that have the same time and location.
            while self.cycle.peek().is_some_and(|x| (x.0).0 == time && (x.0).1 == location) {
                diff += (self.cycle.pop().unwrap().0).2;
            }
            if diff != 0 {
                return Some((time, location, diff));
            }
        }
        None
    }

    /// Forgets the locations that had changes, once all are applied.
    fn clear(&mut self) {
        self.changes.changes.clear();
    }
}

/// The strongly connected components of the graph with the `successors` of
/// each vertex, in reverse topological order: a component is listed before
/// any component with an edge to it. Tarjan's algorithm, without recursion.
fn strongly_connected_components(successors: &[Vec<usize>]) -> Vec<Vec<usize>> {
    const UNVISITED: usize = usize::MAX;
    let count = successors.len();
    let mut index = vec![UNVISITED; count];
    let mut lowlink = vec![0; count];
    let mut on_stack = vec![false; count];
    let mut stack = Vec::new();
    let mut components = Vec::new();
    let mut next = 0;
    // The depth-first path: each vertex, and how many of its successors it has visited.
    let mut path: Vec<(usize, usize)> = Vec::new();
    for root in 0 .. count {
        if index[root] != UNVISITED {
            continue;
        }
        path.push((root, 0));
        index[root] = next;
        lowlink[root] = next;
        next += 1;
        stack.push(root);
        on_stack[root] = true;
        while let Some(&mut (vertex, ref mut visited)) = path.last_mut() {
            if let Some(&successor) = <[usize]>::get(&successors[vertex], *visited) {
                *visited += 1;
                if index[successor] == UNVISITED {
                    index[successor] = next;
                    lowlink[successor] = next;
                    next += 1;
                    stack.push(successor);
                    on_stack[successor] = true;
                    path.push((successor, 0));
                }
                else if on_stack[successor] {
                    lowlink[vertex] = lowlink[vertex].min(index[successor]);
                }
                continue;
            }
            path.pop();
            if let Some(&(parent, _)) = <[(usize, usize)]>::last(&path) {
                lowlink[parent] = lowlink[parent].min(lowlink[vertex]);
            }
            if lowlink[vertex] == index[vertex] {
                let mut component = Vec::new();
                loop {
                    let member = stack.pop().unwrap();
                    on_stack[member] = false;
                    component.push(member);
                    if member == vertex {
                        break;
                    }
                }
                components.push(component);
            }
        }
    }
    components
}

/// Changes to the counts of times at the locations of a graph.
///
/// The changes are kept per location, each location's in its own
/// `ChangeBatch`, with the locations in the order they first changed.
/// Consolidating them then sorts only each location's few times, rather
/// than all the changes by location and time together; their consumers
/// need each location's changes consolidated, in no particular order.
struct LocationChanges<T> {
    /// For each node, the index in `slot` of its first input, its outputs
    /// following its inputs, and its number of inputs.
    offsets: Vec<(usize, usize)>,
    /// The index in `changes` of each location's changes, valid only while
    /// `changes` holds that location at that index.
    slot: Vec<usize>,
    /// The locations that have changes, in the order first changed, and their changes.
    changes: Vec<(Location, ChangeBatch<T, 4>)>,
}

impl<T: Timestamp> LocationChanges<T> {
    /// Changes for a graph whose nodes have the `(inputs, outputs)` of `shape`.
    fn new(shape: &[(usize, usize)]) -> Self {
        let mut offsets = Vec::with_capacity(shape.len());
        let mut ports = 0;
        for &(inputs, outputs) in shape {
            offsets.push((ports, inputs));
            ports += inputs + outputs;
        }
        LocationChanges { offsets, slot: vec![usize::MAX; ports], changes: Vec::new() }
    }

    /// The index in `slot` of `location`.
    #[inline]
    fn index(&self, location: Location) -> usize {
        let (first, inputs) = self.offsets[location.node];
        match location.port {
            Port::Target(port) => first + port,
            Port::Source(port) => first + inputs + port,
        }
    }

    /// Adds `diff` to the count of `time` at `location`, and indicates
    /// whether `location` had no changes before.
    #[inline]
    fn update(&mut self, location: Location, time: T, diff: i64) -> bool {
        if diff == 0 {
            return false;
        }
        let index = self.index(location);
        let slot = self.slot[index];
        match self.changes.get_mut(slot) {
            Some((at, batch)) if *at == location => {
                let first = batch.unstable_internal_updates().is_empty();
                batch.update(time, diff);
                first
            }
            _ => {
                self.slot[index] = self.changes.len();
                self.changes.push((location, ChangeBatch::new_from(time, diff)));
                true
            }
        }
    }

    /// Removes the changes at `location`.
    #[inline]
    fn take(&mut self, location: Location) -> ChangeBatch<T, 4> {
        let slot = self.slot[self.index(location)];
        match self.changes.get_mut(slot) {
            Some((at, batch)) if *at == location => std::mem::take(batch),
            _ => ChangeBatch::new(),
        }
    }

    /// Indicates that no location has a net change.
    fn is_empty(&mut self) -> bool {
        self.changes.iter_mut().all(|(_, batch)| batch.is_empty())
    }

    /// The net changes, by location, time and change.
    fn iter(&mut self) -> impl Iterator<Item = (Location, &T, i64)> + '_ {
        self.changes.iter_mut().flat_map(|(location, batch)| {
            let location = *location;
            batch.iter().map(move |(time, diff)| (location, time, *diff))
        })
    }

    /// Removes the changes, yielding each location with a net change and
    /// its net changes, consolidated.
    fn drain_locations(&mut self) -> impl Iterator<Item = (Location, smallvec::IntoIter<[(T, i64); 4]>)> + '_ {
        self.changes.drain(..).filter_map(|(location, batch)| {
            let updates = batch.into_inner();
            (!updates.is_empty()).then(|| (location, updates.into_iter()))
        })
    }

    /// Removes the changes, yielding each net change at each location.
    fn drain(&mut self) -> impl Iterator<Item = ((Location, T), i64)> + '_ {
        self.drain_locations().flat_map(|(location, updates)| {
            updates.map(move |(time, diff)| ((location, time), diff))
        })
    }
}

/// A sorted map maintained as a single vector of power-of-two sorted runs.
///
/// The vector's length always reveals the run structure: the binary
/// representation of the length, read from the high bit down, gives the
/// sizes of the sorted runs in order. Adjacent runs may in fact be parts
/// of larger sorted runs, but we make no attempt to claim those wins.
///
/// Keys are distinct across all runs. Novel keys are introduced by
/// re-sorting the suffix whose run structure their addition changes, as
/// in binary addition. Each element is re-sorted at most logarithmically
/// often (so `O(n log^2 n)` comparisons in total, a log factor more than
/// merging would cost, for much less code), and lookups visit at most
/// logarithmically many runs.
struct BinaryRuns<K, V> { entries: Vec<(K, V)> }

impl<K: Ord, V> Default for BinaryRuns<K, V> {
    fn default() -> Self { Self { entries: Vec::new() } }
}

impl<K: Ord, V> BinaryRuns<K, V> {
    /// A mutable reference to the value at `key`, if present.
    fn get_mut(&mut self, key: &K) -> Option<&mut V> {
        let mut position = None;
        let mut offset = 0;
        for bit in (0..usize::BITS).rev() {
            let size = 1usize << bit;
            if self.entries.len() & size != 0 {
                let run = &self.entries[offset .. offset + size];
                if let Ok(index) = run.binary_search_by(|(k, _)| k.cmp(key)) {
                    position = Some(offset + index);
                    break;
                }
                offset += size;
            }
        }
        position.map(|index| &mut self.entries[index].1)
    }

    /// Introduces a batch of keys distinct from each other and from those present.
    fn insert_batch(&mut self, batch: Vec<(K, V)>) {
        if batch.is_empty() { return; }
        let total = self.entries.len() + batch.len();
        // Runs at the leading bits on which the lengths agree are unaffected; mask
        // away the highest differing bit (the xor is non-zero) and below.
        let stable = total & !(usize::MAX >> (self.entries.len() ^ total).leading_zeros());
        self.entries.extend(batch);
        self.entries[stable..].sort_unstable_by(|x, y| x.0.cmp(&y.0));
    }

    /// Merges all runs into one sorted vector.
    fn into_sorted(mut self) -> Vec<(K, V)> {
        self.entries.sort_unstable_by(|x, y| x.0.cmp(&y.0));
        self.entries
    }
}

/// Determines summaries from locations to scope outputs.
///
/// Specifically, for each location whose node identifier is non-zero, we compile
/// the summaries along which they can reach each output.
///
/// Graph locations may be missing from the output, in which case they have no
/// paths to scope outputs. The result is sorted by location.
fn summarize_outputs<T: Timestamp>(
    nodes: &[Connectivity<T::Summary>],
    edges: &[Vec<Vec<Target>>],
    ) -> Vec<(Location, PortConnectivity<T::Summary>)>
{
    // A reverse edge map, to allow us to walk back up the dataflow graph.
    // Sorted by target location; each target should have at most one source.
    let mut reverse_edges = Vec::new();
    for (node, outputs) in edges.iter().enumerate() {
        for (output, targets) in outputs.iter().enumerate() {
            for target in targets.iter() {
                reverse_edges.push((
                    Location::from(*target),
                    Location { node, port: Port::Source(output) }
                ));
            }
        }
    }
    reverse_edges.sort_unstable();
    reverse_edges.dedup();

    // A reverse map from operator outputs to inputs, along their internal summaries.
    // Sorted by source location, so that the entries for a location are contiguous.
    let mut reverse_internal = Vec::new();
    for (node, connectivity) in nodes.iter().enumerate() {
        for (input, outputs) in connectivity.iter().enumerate() {
            for (output, summary) in outputs.iter_ports() {
                reverse_internal.push((Location::new_source(node, output), input, summary));
            }
        }
    }
    reverse_internal.sort_unstable_by(|x, y| (x.0, x.1).cmp(&(y.0, y.1)));

    // Accumulated summaries to scope outputs, keyed by `(location, output)`.
    let mut accumulated: BinaryRuns<(Location, usize), Antichain<T::Summary>> = BinaryRuns::default();

    // Round-based (semi-naive) fixed point. Each round walks reverse edges and reverse
    // internal summaries from the triples that changed last round, and the proposals
    // that improve the accumulated antichains form the next round's work.
    // The scope may have no outputs, in which case we can do no work.
    let mut todo: Vec<(Location, usize, T::Summary)> =
    edges
        .iter()
        .flat_map(|x| x.iter())
        .flat_map(|x| x.iter())
        .filter(|target| target.node == 0)
        .map(|target| (Location::from(*target), target.port, Default::default()))
        .collect();

    let mut proposals: Vec<((Location, usize), T::Summary)> = Vec::new();

    // Loop until we stop discovering novel reachability paths.
    while !todo.is_empty() {

        // Collect proposed summaries from the triples changed last round.
        for (location, output, summary) in todo.drain(..) {
            match location.port {

                // This is an output port of an operator, or a scope input.
                // We want to crawl up the operator, to its inputs.
                Port::Source(_output_port) => {
                    let start = reverse_internal.partition_point(|(source, _, _)| *source < location);
                    let inputs = reverse_internal[start..].iter().take_while(|(source, _, _)| *source == location);
                    for (_, input_port, operator_summary) in inputs {
                        let new_location = Location::new_target(location.node, *input_port);
                        for op_summary in operator_summary.elements().iter() {
                            if let Some(combined) = op_summary.followed_by(&summary) {
                                proposals.push(((new_location, output), combined));
                            }
                        }
                    }
                }

                // This is an input port of an operator, or a scope output.
                // We want to walk back the (unique) edge leading to it.
                Port::Target(_port) => {
                    if let Ok(index) = reverse_edges.binary_search_by_key(&location, |(target, _)| *target) {
                        proposals.push(((reverse_edges[index].1, output), summary));
                    }
                }
            }
        }

        // Merge the batch of proposals into the accumulated summaries. Proposals are
        // first collapsed per key into an antichain, so that only elements novel to
        // the accumulated antichain (in an order-independent sense) seed the next round.
        proposals.sort_unstable_by(|x, y| x.0.cmp(&y.0));
        let mut fresh: Vec<((Location, usize), Antichain<T::Summary>)> = Vec::new();
        let mut batch: Antichain<T::Summary> = Antichain::new();
        let mut iter = proposals.drain(..).peekable();
        while let Some(((location, output), summary)) = iter.next() {
            // Collapse this round's proposals for the key into one antichain.
            batch.insert(summary);
            while iter.peek().map(|(key, _)| *key == (location, output)).unwrap_or(false) {
                batch.insert(iter.next().unwrap().1);
            }
            if let Some(antichain) = accumulated.get_mut(&(location, output)) {
                for summary in batch.drain() {
                    if antichain.insert_ref(&summary) {
                        todo.push((location, output, summary));
                    }
                }
            }
            else {
                todo.extend(batch.elements().iter().map(|summary| (location, output, summary.clone())));
                fresh.push(((location, output), std::mem::take(&mut batch)));
            }
        }

        // Introduce the novel keys.
        accumulated.insert_batch(fresh);
    }

    // Merge all runs into one sorted list, and group it by location.
    let mut results: Vec<(Location, PortConnectivityBuilder<T::Summary>)> = Vec::new();
    for ((location, output), antichain) in accumulated.into_sorted() {
        match results.last_mut() {
            Some((last, connectivity)) if *last == location => { connectivity.add_port(output, antichain); }
            _ => {
                let mut connectivity = PortConnectivityBuilder::default();
                connectivity.add_port(output, antichain);
                results.push((location, connectivity));
            }
        }
    }
    results.into_iter().map(|(location, builder)| (location, builder.freeze())).collect()
}

/// Logging types for reachability tracking events.
pub mod logging {
    use std::time::Duration;

    use timely_container::CapacityContainerBuilder;
    use timely_logging::TypedLogger;
    use crate::logging_core::Logger;

    /// A container builder for tracker events.
    pub type TrackerEventBuilder<T> = CapacityContainerBuilder<Vec<(Duration, TrackerEvent<T>)>>;

    /// A logger with additional identifying information about the tracker.
    pub struct TrackerLogger<T: Clone + 'static> {
        identifier: usize,
        logger: TypedLogger<TrackerEventBuilder<T>, TrackerEvent<T>>,
    }

    impl<T: Clone + 'static> TrackerLogger<T> {
        /// Create a new tracker logger from its fields.
        pub fn new(identifier: usize, logger: Logger<TrackerEventBuilder<T>>) -> Self {
            Self { identifier, logger: logger.into() }
        }

        /// Log source update events with additional identifying information.
        pub fn log_source_updates<'a, I>(&mut self, updates: I)
        where
            I: IntoIterator<Item = (usize, usize, &'a T, i64)>
        {
            let updates: Vec<_> = updates.into_iter().map(|(a,b,c,d)| (a,b,c.clone(),d)).collect();
            if !updates.is_empty() {
                self.logger.log({
                    SourceUpdate {
                        tracker_id: self.identifier,
                        updates
                    }
                });
            }
        }
        /// Log target update events with additional identifying information.
        pub fn log_target_updates<'a, I>(&mut self, updates: I)
        where
            I: IntoIterator<Item = (usize, usize, &'a T, i64)>
        {
            let updates: Vec<_> = updates.into_iter().map(|(a,b,c,d)| (a,b,c.clone(),d)).collect();
            if !updates.is_empty() {
                self.logger.log({
                    TargetUpdate {
                        tracker_id: self.identifier,
                        updates
                    }
                });
            }
        }
    }

    /// Events that the tracker may record.
    #[derive(Debug, Clone)]
    pub enum TrackerEvent<T> {
        /// Updates made at a source of data.
        SourceUpdate(SourceUpdate<T>),
        /// Updates made at a target of data.
        TargetUpdate(TargetUpdate<T>),
    }

    /// An update made at a source of data.
    #[derive(Debug, Clone)]
    pub struct SourceUpdate<T> {
        /// An identifier for the tracker.
        pub tracker_id: usize,
        /// Updates themselves, as `(node, port, time, diff)`.
        pub updates: Vec<(usize, usize, T, i64)>,
    }

    /// An update made at a target of data.
    #[derive(Debug, Clone)]
    pub struct TargetUpdate<T> {
        /// An identifier for the tracker.
        pub tracker_id: usize,
        /// Updates themselves, as `(node, port, time, diff)`.
        pub updates: Vec<(usize, usize, T, i64)>,
    }

    impl<T> From<SourceUpdate<T>> for TrackerEvent<T> {
        fn from(v: SourceUpdate<T>) -> TrackerEvent<T> { TrackerEvent::SourceUpdate(v) }
    }

    impl<T> From<TargetUpdate<T>> for TrackerEvent<T> {
        fn from(v: TargetUpdate<T>) -> TrackerEvent<T> { TrackerEvent::TargetUpdate(v) }
    }
}

// The Drop implementation for `Tracker` makes sure that reachability logging is correct for
// prematurely dropped dataflows. At the moment, this is only possible through `drop_dataflow`,
// because in all other cases the tracker stays alive while it has outstanding work, leaving no
// remaining work for this Drop implementation.
impl<T: Timestamp> Drop for Tracker<T> {
    fn drop(&mut self) {
        let logger = if let Some(logger) = &mut self.logger {
            logger
        } else {
            // No cleanup necessary when there is no logger.
            return;
        };

        // Retract pending data that `propagate_all` would normally log.
        for (index, per_operator) in self.per_operator.iter_mut().enumerate() {
            let target_changes = per_operator.targets
                .iter_mut()
                .enumerate()
                .flat_map(|(port, target)| {
                    target.pointstamps
                        .updates()
                        .map(move |(time, diff)| (index, port, time, -diff))
                });

            logger.log_target_updates(target_changes);

            let source_changes = per_operator.sources
                .iter_mut()
                .enumerate()
                .flat_map(|(port, source)| {
                    source.pointstamps
                        .updates()
                        .map(move |(time, diff)| (index, port, time, -diff))
                });

            logger.log_source_updates(source_changes);
        }
    }
}
