//! Manages pointstamp reachability within a graph.
//!
//! Timely dataflow is concerned with understanding and communicating the potential
//! for capabilites to reach nodes in a directed graph, by following paths through
//! the graph (along edges and through nodes). This module contains one abstraction
//! for managing this information.
//!
//! #Examples
//!
//! ```rust
//! use timely::progress::frontier::Antichain;
//! use timely::progress::nested::subgraph::{Source, Target};
//! use timely::progress::nested::reachability_neu::{Builder, Tracker};
//!
//! // allocate a new empty topology builder.
//! let mut builder = Builder::<usize>::new();
//! 
//! // Each node with one input connected to one output.
//! builder.add_node(0, 1, 1, vec![vec![Antichain::from_elem(0)]]);
//! builder.add_node(1, 1, 1, vec![vec![Antichain::from_elem(0)]]);
//! builder.add_node(2, 1, 1, vec![vec![Antichain::from_elem(1)]]);
//!
//! // Connect nodes in sequence, looping around to the first from the last.
//! builder.add_edge(Source { index: 0, port: 0}, Target { index: 1, port: 0} );
//! builder.add_edge(Source { index: 1, port: 0}, Target { index: 2, port: 0} );
//! builder.add_edge(Source { index: 2, port: 0}, Target { index: 0, port: 0} );
//!
//! // Construct a reachability tracker.
//! let mut tracker = builder.build();
//!
//! // Introduce a pointstamp at the output of the first node.
//! tracker.update_source(Source { index: 0, port: 0}, 17, 1);
//!
//! // Propagate changes; until this call updates are simply buffered.
//! tracker.propagate_all();
//!
//! let mut results = tracker.pushed().drain().collect::<Vec<_>>();
//! results.sort();
//!
//! assert_eq!(results.len(), 3);
//! assert_eq!(results[0], ((Target { index: 0, port: 0 }, 18), 1));
//! assert_eq!(results[1], ((Target { index: 1, port: 0 }, 17), 1));
//! assert_eq!(results[2], ((Target { index: 2, port: 0 }, 17), 1));
//!
//! // Introduce a pointstamp at the output of the first node.
//! tracker.update_source(Source { index: 0, port: 0}, 17, -1);
//!
//! // Propagate changes; until this call updates are simply buffered.
//! tracker.propagate_all();
//!
//! let mut results = tracker.pushed().drain().collect::<Vec<_>>();
//! results.sort();
//!
//! assert_eq!(results.len(), 3);
//! assert_eq!(results[0], ((Target { index: 0, port: 0 }, 18), -1));
//! assert_eq!(results[1], ((Target { index: 1, port: 0 }, 17), -1));
//! assert_eq!(results[2], ((Target { index: 2, port: 0 }, 17), -1));
//! ```

use std::collections::BinaryHeap;

use progress::Timestamp;
use progress::nested::{Source, Target};
use progress::ChangeBatch;

use progress::frontier::{Antichain, MutableAntichain};
use progress::timestamp::PathSummary;


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
/// #Examples
///
/// ```rust
/// use timely::progress::frontier::Antichain;
/// use timely::progress::nested::subgraph::{Source, Target};
/// use timely::progress::nested::reachability_neu::Builder;
///
/// // allocate a new empty topology builder.
/// let mut builder = Builder::<usize>::new();
/// 
/// // Each node with one input connected to one output.
/// builder.add_node(0, 1, 1, vec![vec![Antichain::from_elem(0)]]);
/// builder.add_node(1, 1, 1, vec![vec![Antichain::from_elem(0)]]);
/// builder.add_node(2, 1, 1, vec![vec![Antichain::from_elem(1)]]);
///
/// // Connect nodes in sequence, looping around to the first from the last.
/// builder.add_edge(Source { index: 0, port: 0}, Target { index: 1, port: 0} );
/// builder.add_edge(Source { index: 1, port: 0}, Target { index: 2, port: 0} );
/// builder.add_edge(Source { index: 2, port: 0}, Target { index: 0, port: 0} );
///
/// // Summarize reachability information.
/// let tracker = builder.build();
/// ```

#[derive(Clone, Debug)]
pub struct Builder<T: Timestamp> {
    /// Internal connections within hosted operators.
    ///
    /// Indexed by operator index, then input port, then output port. This is the
    /// same format returned by `get_internal_summary`, as if we simply appended
    /// all of the summaries for the hosted nodes.
    pub nodes: Vec<Vec<Vec<Antichain<T::Summary>>>>,
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
    pub fn add_node(&mut self, index: usize, inputs: usize, outputs: usize, summary: Vec<Vec<Antichain<T::Summary>>>) {
        
        // Assert that all summaries exist.
        debug_assert_eq!(inputs, summary.len());
        for x in summary.iter() { debug_assert_eq!(outputs, x.len()); }

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
        debug_assert!(source.port < self.shape[source.index].1);
        debug_assert!(target.port < self.shape[target.index].0);

        self.edges[source.index][source.port].push(target);
    }

    /// Compiles the current nodes and edges into immutable path summaries.
    ///
    /// This method has the opportunity to perform some error checking that the path summaries
    /// are valid, including references to undefined nodes and ports, as well as self-loops with
    /// default summaries (a serious liveness issue).
    pub fn build(&self) -> Tracker<T> {
        Tracker::allocate_from(self)
    }
}

/// An interactive tracker of propagated reachability information.
///
/// A `Tracker` tracks, for a fixed graph topology, the consequences of
/// pointstamp changes at various node input and output ports. These changes may
/// alter the potential pointstamps that could arrive at downstream input ports.

#[derive(Debug)]
pub struct Tracker<T:Timestamp> {

    // TODO: All of the sizes of these allocations are static (except internal to `ChangeBatch`).
    //       It seems we should be able to flatten most of these so that there are a few allocations
    //       independent of the numbers of nodes and ports and such.
    //
    // TODO: We could also change the internal representation to be a graph of targets, using usize
    //       identifiers for each, so that internally we needn't use multiple levels of indirection.
    //       This may make more sense once we commit to topologically ordering the targets.

    /// Each source and target has a mutable antichain to ensure that we track their discrete frontiers,
    /// rather than their multiplicities. We separately track the frontiers resulting from propagated
    /// frontiers, to protect them from transient negativity in inbound target updates.
    sources: Vec<Vec<MutableAntichain<T>>>,
    targets: Vec<Vec<MutableAntichain<T>>>,
    pusheds: Vec<Vec<MutableAntichain<T>>>,

    /// Source and target changes are buffered, which allows us to delay processing until propagation,
    /// and so consolidate updates, but to leap directly to those frontiers that may have changed.
    source_changes: ChangeBatch<(Source, T)>,
    target_changes: ChangeBatch<(Target, T)>,

    /// Worklist of updates to perform, ordered by increasing timestamp and target.
    target_worklist: BinaryHeap<OrderReversed<(T, Target, i64)>>,

    /// Buffer of consequent changes.
    pushed_changes: ChangeBatch<(Target, T)>,

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

    /// Compiled reach from each target to targets one hop downstream.
    compiled: Vec<Vec<Vec<(Target, T::Summary)>>>,
}

impl<T:Timestamp> Tracker<T> {

    /// Updates the count for a time at a target.
    #[inline]
    pub fn update_target(&mut self, target: Target, time: T, value: i64) {
        self.target_changes.update((target, time), value);
    }
    /// Updates the count for a time at a source.
    #[inline]
    pub fn update_source(&mut self, source: Source, time: T, value: i64) {
        self.source_changes.update((source, time), value);
    }

    /// Allocate a new `Tracker` using the shape from `summaries`.
    pub fn allocate_from(builder: &Builder<T>) -> Self {

        let mut sources = Vec::with_capacity(builder.shape.len());
        let mut targets = Vec::with_capacity(builder.shape.len());
        let mut pusheds = Vec::with_capacity(builder.shape.len());

        let mut compiled = Vec::with_capacity(builder.shape.len());

        // Allocate buffer space for each input and input port.
        for (node, &(inputs, outputs)) in builder.shape.iter().enumerate() {
            sources.push(vec![MutableAntichain::new(); outputs]);
            targets.push(vec![MutableAntichain::new(); inputs]);
            pusheds.push(vec![MutableAntichain::new(); inputs]);

            let mut compiled_node = vec![Vec::new(); inputs];
            for input in 0 .. inputs {
                for output in 0 .. outputs {
                    for summary in builder.nodes[node][input][output].elements().iter() {
                        for &target in builder.edges[node][output].iter() {
                            compiled_node[input].push((target, summary.clone()));
                        }
                    }
                }
            }
            compiled.push(compiled_node);
        }

        Tracker {
            sources,
            targets,
            pusheds,
            source_changes: ChangeBatch::new(),
            target_changes: ChangeBatch::new(),
            target_worklist: BinaryHeap::new(),
            pushed_changes: ChangeBatch::new(),
            nodes: builder.nodes.clone(),
            edges: builder.edges.clone(),
            compiled,
        }
    }

    /// Propagates all pending updates.
    pub fn propagate_all(&mut self) {

        // Filter each target change through `self.targets`.
        for ((target, time), diff) in self.target_changes.drain() {
            let target_worklist = &mut self.target_worklist;
            self.targets[target.index][target.port].update_iter_and(Some((time, diff)), |time, diff| {
                target_worklist.push(OrderReversed::new((time.clone(), target, diff)))
            })
        }

        // Filter each source change through `self.sources` and then along edges.
        for ((source, time), diff) in self.source_changes.drain() {
            let target_worklist = &mut self.target_worklist;
            let edges = &self.edges[source.index][source.port];
            self.sources[source.index][source.port].update_iter_and(Some((time, diff)), |time, diff| {
                for &target in edges.iter() {
                    target_worklist.push(OrderReversed::new((time.clone(), target, diff)))
                }
            })
        }

        while !self.target_worklist.is_empty() {

            // This iteration we will drain all (target, time) work items.
            let (time, target, mut diff) = self.target_worklist.pop().unwrap().element;

            // Drain any other updates that might have the same time; accumulate difference.
            while self.target_worklist.peek().map(|x| (x.element.0 == time) && (x.element.1 == target)).unwrap_or(false) {
                diff += self.target_worklist.pop().unwrap().element.2;
            }

            // Only act if there is a net change, positive or negative.
            if diff != 0 {

                // Borrow various self fields to appease Rust.
                let pushed_changes = &mut self.pushed_changes;
                let target_worklist = &mut self.target_worklist;
                let _edges = &self.edges[target.index];
                let _nodes = &self.nodes[target.index][target.port];
                let _compiled = &self.compiled[target.index][target.port];

                // Although single-element updates may seem wasteful, they are important for termination.
                self.pusheds[target.index][target.port].update_iter_and(Some((time, diff)), |time, diff| {

                    // Identify the change in the out change list.
                    pushed_changes.update((target, time.clone()), diff);

                    // When a target has its frontier change we should communicate the change to downstream
                    // targets as well. This means traversing the operator to its outputs.
                    //
                    // We have two implementations, first traversing `nodes` and `edges`, and second using
                    // a compiled representation that flattens all these lists (but has redundant calls to
                    // `results_in`).

                    // // Version 1.
                    // for (output, summaries) in _nodes.iter().enumerate() {
                    //     for summary in summaries.elements().iter() {
                    //         if let Some(new_time) = summary.results_in(time) {
                    //             for &new_target in _edges[output].iter() {
                    //                 target_worklist.push(OrderReversed::new((new_time.clone(), new_target, diff)));
                    //             }
                    //         }
                    //     }
                    // }

                    // Version 2.
                    for &(new_target, ref summary) in _compiled.iter() {
                        if let Some(new_time) = summary.results_in(time) {
                            target_worklist.push(OrderReversed::new((new_time.clone(), new_target, diff)));
                        }
                    }

                })
            }

        }
    }

    /// A mutable reference to the pushed results of changes.
    pub fn pushed(&mut self) -> &mut ChangeBatch<(Target, T)> {
        &mut self.pushed_changes
    }
}


#[derive(PartialEq, Eq, Debug)]
struct OrderReversed<T> {
    pub element: T,
}

impl<T> OrderReversed<T> {
    fn new(element: T) -> Self { OrderReversed { element } }
}

impl<T: PartialOrd> PartialOrd for OrderReversed<T> {
    fn partial_cmp(&self, other: &Self) -> Option<::std::cmp::Ordering> {
        other.element.partial_cmp(&self.element)
    }
}
impl<T: Ord> Ord for OrderReversed<T> {
    fn cmp(&self, other: &Self) -> ::std::cmp::Ordering {
        other.element.cmp(&self.element)
    }
}
