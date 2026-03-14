//! Pipeline group fusion: detects and fuses groups of pipeline-connected operators.
//!
//! This module implements operator fusion as a `GraphPass`. Groups of operators
//! connected by pipeline (thread-local) channels with identity summaries are
//! fused into a single `GroupScheduler` operator. The group is scheduled as a
//! unit in topological order, hiding intermediate progress from the reachability
//! tracker.

use std::rc::Rc;
use std::cell::RefCell;
use std::collections::HashMap;

use crate::scheduling::Schedule;
use crate::progress::{Source, Target, Timestamp};
use crate::progress::operate::SharedProgress;
use crate::progress::operate::{FrontierInterest, Connectivity, PortConnectivity};

use super::subgraph::PerOperatorState;
use super::graph_pass::GraphPass;

/// A graph pass that fuses groups of pipeline-connected operators.
///
/// Operators are eligible for fusion when they have identity internal summaries,
/// do not require notifications (`notify = false`), and are connected via
/// pipeline (thread-local) channels. Groups of at least `min_length` eligible
/// operators are detected using union-find and fused into a single operator.
pub struct FusionPass {
    /// Minimum group size for fusion.
    min_length: usize,
}

impl FusionPass {
    /// Creates a new fusion pass with the given minimum group size.
    pub fn new(min_length: usize) -> Self {
        FusionPass { min_length }
    }
}

impl<T: Timestamp> GraphPass<T> for FusionPass {
    fn apply(&self, children: &mut Vec<PerOperatorState<T>>, edges: &mut Vec<(Source, Target)>) {
        let groups = detect_groups(children, edges, self.min_length);
        for group in groups {
            fuse_group::<T>(children, edges, &group);
        }
    }
}

/// Returns true if an operator has identity internal summaries on all (input, output) pairs.
/// That is, every connected (input, output) pair has summary `Antichain::from_elem(Default::default())`.
fn has_identity_summary<T: Timestamp>(child: &PerOperatorState<T>) -> bool {
    for input_pc in child.internal_summary.iter() {
        for (_port, ac) in input_pc.iter_ports() {
            if ac.len() != 1 || ac.elements()[0] != Default::default() {
                return false;
            }
        }
    }
    // Must have at least one connection (empty summary means no paths).
    child.internal_summary.iter().any(|pc| pc.iter_ports().next().is_some())
}

/// Returns true if an operator is eligible for group fusion.
fn is_fusible<T: Timestamp>(child: &PerOperatorState<T>) -> bool {
    child.operator.is_some()
        && child.notify.iter().all(|n| *n == FrontierInterest::Never)
        && has_identity_summary(child)
}

/// Detects fusible groups of operators connected by pipeline edges.
///
/// Uses union-find to group operators connected by fusible edges into components.
/// An edge is fusible when both endpoints are fusible operators and the target
/// uses pipeline (thread-local) channels. No fan-in/fan-out or 1-in/1-out restriction.
///
/// Returns groups of at least `min_length` operators, identified by child index.
fn detect_groups<T: Timestamp>(
    children: &[PerOperatorState<T>],
    edge_stash: &[(Source, Target)],
    min_length: usize,
) -> Vec<Vec<usize>> {
    // Mark fusible operators.
    let fusible: Vec<bool> = children.iter().enumerate().map(|(i, child)| {
        i != 0 && is_fusible(child)
    }).collect();

    // Union-Find structure.
    let n = children.len();
    let mut parent: Vec<usize> = (0..n).collect();
    let mut rank: Vec<usize> = vec![0; n];

    fn find(parent: &mut [usize], x: usize) -> usize {
        if parent[x] != x {
            parent[x] = find(parent, parent[x]);
        }
        parent[x]
    }

    fn union(parent: &mut [usize], rank: &mut [usize], a: usize, b: usize) {
        let ra = find(parent, a);
        let rb = find(parent, b);
        if ra == rb { return; }
        if rank[ra] < rank[rb] {
            parent[ra] = rb;
        } else if rank[ra] > rank[rb] {
            parent[rb] = ra;
        } else {
            parent[rb] = ra;
            rank[ra] += 1;
        }
    }

    // For each edge, if both endpoints are fusible and target uses pipeline pact, union them.
    for (source, target) in edge_stash.iter() {
        let src = source.node;
        let tgt = target.node;
        if src == 0 || tgt == 0 { continue; }
        if !fusible[src] || !fusible[tgt] { continue; }
        if !children[tgt].pipeline { continue; }
        union(&mut parent, &mut rank, src, tgt);
    }

    // Collect components.
    let mut components: HashMap<usize, Vec<usize>> = HashMap::new();
    for i in 1..n {
        if fusible[i] {
            let root = find(&mut parent, i);
            components.entry(root).or_default().push(i);
        }
    }

    // Filter by minimum size and sort members for determinism.
    components.into_values()
        .filter(|group| group.len() >= min_length)
        .map(|mut group| { group.sort(); group })
        .collect()
}

/// Topological sort of group members using Kahn's algorithm on internal edges.
fn topological_sort(
    members: &[usize],
    edge_stash: &[(Source, Target)],
) -> Vec<usize> {
    let member_set: std::collections::HashSet<usize> = members.iter().cloned().collect();
    let member_to_pos: HashMap<usize, usize> = members.iter().enumerate().map(|(i, &m)| (m, i)).collect();
    let n = members.len();

    let mut in_degree = vec![0usize; n];
    let mut adj: Vec<Vec<usize>> = vec![Vec::new(); n];

    for (source, target) in edge_stash.iter() {
        if member_set.contains(&source.node) && member_set.contains(&target.node) {
            let from = member_to_pos[&source.node];
            let to = member_to_pos[&target.node];
            // Avoid counting duplicate edges for the same (from, to) pair multiple times
            // for in-degree. We track adjacency; Kahn's handles it correctly.
            adj[from].push(to);
            in_degree[to] += 1;
        }
    }

    let mut queue: std::collections::VecDeque<usize> = std::collections::VecDeque::new();
    for i in 0..n {
        if in_degree[i] == 0 {
            queue.push_back(i);
        }
    }

    let mut order = Vec::with_capacity(n);
    while let Some(pos) = queue.pop_front() {
        order.push(members[pos]);
        for &next in &adj[pos] {
            in_degree[next] -= 1;
            if in_degree[next] == 0 {
                queue.push_back(next);
            }
        }
    }

    assert_eq!(order.len(), n, "group contains a cycle, which should be impossible with identity summaries");
    order
}

/// A member of a fused group, holding the original operator and its progress handle.
struct GroupMember<T: Timestamp> {
    operator: Box<dyn Schedule>,
    shared_progress: Rc<RefCell<SharedProgress<T>>>,
}

/// Schedules a DAG of pipeline-connected operators as a single unit.
///
/// The group presents as a single operator to the subgraph with `input_map.len()` inputs
/// and `output_map.len()` outputs. Members are scheduled in topological order.
/// Intermediate progress is hidden from the reachability tracker.
struct GroupScheduler<T: Timestamp> {
    name: String,
    path: Vec<usize>,
    /// Progress visible to the subgraph.
    group_progress: Rc<RefCell<SharedProgress<T>>>,
    /// Operators in topological order, with their individual SharedProgress handles.
    members: Vec<GroupMember<T>>,
    /// Group input i -> (member index in members vec, member input port)
    input_map: Vec<(usize, usize)>,
    /// Group output j -> (member index in members vec, member output port)
    output_map: Vec<(usize, usize)>,
    /// capability_map[member_idx][output_port] -> list of group output indices
    capability_map: Vec<Vec<Vec<usize>>>,
}

impl<T: Timestamp> Schedule for GroupScheduler<T> {
    fn name(&self) -> &str { &self.name }
    fn path(&self) -> &[usize] { &self.path }

    fn schedule(&mut self) -> bool {
        let n = self.members.len();
        assert!(n > 0);

        // Step 1: Forward group's input frontier changes to the appropriate members.
        {
            let mut group_sp = self.group_progress.borrow_mut();
            for (i, &(member_idx, member_port)) in self.input_map.iter().enumerate() {
                let mut member_sp = self.members[member_idx].shared_progress.borrow_mut();
                for (time, diff) in group_sp.frontiers[i].iter() {
                    member_sp.frontiers[member_port].update(time.clone(), *diff);
                }
            }
        }

        // Step 2: Schedule each member in topological order.
        let mut any_incomplete = false;
        for i in 0..n {
            let incomplete = self.members[i].operator.schedule();
            any_incomplete = any_incomplete || incomplete;
        }

        // Step 3: Aggregate progress.
        {
            let mut group_sp = self.group_progress.borrow_mut();

            // consumeds: for each group input, take from the corresponding member.
            for (i, &(member_idx, member_port)) in self.input_map.iter().enumerate() {
                let mut member_sp = self.members[member_idx].shared_progress.borrow_mut();
                for (time, diff) in member_sp.consumeds[member_port].iter() {
                    group_sp.consumeds[i].update(time.clone(), *diff);
                }
            }

            // produceds: for each group output, take from the corresponding member.
            for (j, &(member_idx, member_port)) in self.output_map.iter().enumerate() {
                let mut member_sp = self.members[member_idx].shared_progress.borrow_mut();
                for (time, diff) in member_sp.produceds[member_port].iter() {
                    group_sp.produceds[j].update(time.clone(), *diff);
                }
            }

            // internals: for each member's output port, report at mapped group outputs.
            for (m, member) in self.members.iter().enumerate() {
                let mut member_sp = member.shared_progress.borrow_mut();
                for (port, internal) in member_sp.internals.iter_mut().enumerate() {
                    for (time, diff) in internal.iter() {
                        for &group_out in &self.capability_map[m][port] {
                            group_sp.internals[group_out].update(time.clone(), *diff);
                        }
                    }
                }
            }
        }

        // Step 4: Clear all members' SharedProgress to prevent accumulation.
        for member in self.members.iter() {
            let mut sp = member.shared_progress.borrow_mut();
            for batch in sp.frontiers.iter_mut() { batch.clear(); }
            for batch in sp.consumeds.iter_mut() { batch.clear(); }
            for batch in sp.internals.iter_mut() { batch.clear(); }
            for batch in sp.produceds.iter_mut() { batch.clear(); }
        }

        // Clear the group's own frontiers (consumed by step 1).
        {
            let mut group_sp = self.group_progress.borrow_mut();
            for batch in group_sp.frontiers.iter_mut() { batch.clear(); }
        }

        any_incomplete
    }
}

/// Computes reachability for all (member, output_port) pairs in a single reverse-topological pass.
///
/// Returns `capability_map[topo_pos][output_port] -> sorted Vec<group_output_index>`.
/// Since all summaries are identity, timestamps don't change along any path.
fn compute_all_reachability(
    topo_order: &[usize],
    children: &[PerOperatorState<impl Timestamp>],
    internal_edges: &HashMap<(usize, usize), Vec<(usize, usize)>>,
    member_summaries: &HashMap<usize, Vec<(usize, usize)>>,
    output_port_to_group_output: &HashMap<(usize, usize), Vec<usize>>,
) -> Vec<Vec<Vec<usize>>> {
    let n = topo_order.len();

    // Build reverse lookup: node -> topo_pos.
    let node_to_topo: HashMap<usize, usize> = topo_order.iter().enumerate()
        .map(|(i, &node)| (node, i))
        .collect();

    // reachable[(node, output_port)] -> set of group output indices
    // Use a flat Vec indexed by (topo_pos, port) for fast access.
    // First, compute a port offset table.
    let mut port_offset = Vec::with_capacity(n);
    let mut total_ports = 0usize;
    for &node in topo_order.iter() {
        port_offset.push(total_ports);
        total_ports += children[node].outputs;
    }

    // Each entry is a sorted Vec<usize> of reachable group outputs.
    let mut reachable: Vec<Vec<usize>> = vec![Vec::new(); total_ports];

    // Seed: output ports that are directly group outputs.
    for (&(node, port), group_outs) in output_port_to_group_output.iter() {
        if let Some(&topo_pos) = node_to_topo.get(&node) {
            let idx = port_offset[topo_pos] + port;
            reachable[idx] = group_outs.clone();
            reachable[idx].sort();
            reachable[idx].dedup();
        }
    }

    // Reverse topological pass: propagate reachability backward through edges.
    for rev_pos in (0..n).rev() {
        let node = topo_order[rev_pos];
        let num_outputs = children[node].outputs;

        // For each output port of this node, follow internal edges forward
        // and union the reachability of the downstream ports.
        for port in 0..num_outputs {
            if let Some(targets) = internal_edges.get(&(node, port)) {
                for &(next_node, next_input_port) in targets {
                    // Use next node's summary to find which output ports are reachable from this input.
                    if let Some(connections) = member_summaries.get(&next_node) {
                        if let Some(&next_topo) = node_to_topo.get(&next_node) {
                            for &(inp, outp) in connections.iter() {
                                if inp == next_input_port {
                                    // Merge reachable[next_topo][outp] into reachable[rev_pos][port].
                                    let src_idx = port_offset[next_topo] + outp;
                                    let dst_idx = port_offset[rev_pos] + port;
                                    if src_idx != dst_idx {
                                        // Clone to avoid double borrow.
                                        let to_add = reachable[src_idx].clone();
                                        let dst = &mut reachable[dst_idx];
                                        dst.extend_from_slice(&to_add);
                                    }
                                }
                            }
                        }
                    }
                }
            }
            // Deduplicate after merging all edges for this port.
            let idx = port_offset[rev_pos] + port;
            reachable[idx].sort();
            reachable[idx].dedup();
        }
    }

    // Reshape into capability_map[topo_pos][output_port].
    let mut capability_map = Vec::with_capacity(n);
    for (topo_pos, &node) in topo_order.iter().enumerate() {
        let num_outputs = children[node].outputs;
        let mut port_map = Vec::with_capacity(num_outputs);
        for port in 0..num_outputs {
            let idx = port_offset[topo_pos] + port;
            port_map.push(std::mem::take(&mut reachable[idx]));
        }
        capability_map.push(port_map);
    }

    capability_map
}

/// Fuses a detected group into a single operator within `children`, rewriting `edge_stash`.
///
/// The representative (lowest index in group) retains its slot and becomes the fused operator.
/// All other group members become tombstones: their operator is removed, inputs/outputs set
/// to zero, and `forward_to` is set to the representative for activation forwarding.
fn fuse_group<T: Timestamp>(
    children: &mut [PerOperatorState<T>],
    edge_stash: &mut Vec<(Source, Target)>,
    group: &[usize],
) {
    assert!(group.len() >= 2);
    let group_set: std::collections::HashSet<usize> = group.iter().cloned().collect();
    let representative = *group.iter().min().unwrap();

    // Step 1: Compute topological order.
    let topo_order = topological_sort(group, edge_stash);
    let node_to_topo: HashMap<usize, usize> = topo_order.iter().enumerate().map(|(i, &n)| (n, i)).collect();

    // Step 2: Compute input_map and output_map by scanning edges.
    // Group inputs: (member_node, input_port) pairs where at least one incoming edge originates outside the group.
    // Group outputs: (member_node, output_port) pairs where at least one outgoing edge targets outside the group,
    //                OR the port has no outgoing edges at all within the edge_stash.
    let mut group_input_set: std::collections::HashSet<(usize, usize)> = std::collections::HashSet::new();
    let mut group_output_set: std::collections::HashSet<(usize, usize)> = std::collections::HashSet::new();
    let mut has_outgoing: std::collections::HashSet<(usize, usize)> = std::collections::HashSet::new();

    // Collect all output ports of group members.
    let mut all_output_ports: std::collections::HashSet<(usize, usize)> = std::collections::HashSet::new();
    for &node in group.iter() {
        for port in 0..children[node].outputs {
            all_output_ports.insert((node, port));
        }
    }

    // Build internal edges map: (src_node, src_port) -> [(tgt_node, tgt_port)]
    let mut internal_edges: HashMap<(usize, usize), Vec<(usize, usize)>> = HashMap::new();

    for (source, target) in edge_stash.iter() {
        let src_in = group_set.contains(&source.node);
        let tgt_in = group_set.contains(&target.node);

        if src_in {
            has_outgoing.insert((source.node, source.port));
        }

        if src_in && tgt_in {
            // Internal edge
            internal_edges.entry((source.node, source.port))
                .or_default()
                .push((target.node, target.port));
        } else if !src_in && tgt_in {
            // Incoming edge from outside
            group_input_set.insert((target.node, target.port));
        } else if src_in && !tgt_in {
            // Outgoing edge to outside
            group_output_set.insert((source.node, source.port));
        }
    }

    // Output ports with no outgoing edges at all are also group outputs.
    for &(node, port) in &all_output_ports {
        if !has_outgoing.contains(&(node, port)) {
            group_output_set.insert((node, port));
        }
    }

    // Sort and assign indices for determinism.
    let mut input_map: Vec<(usize, usize)> = group_input_set.into_iter()
        .map(|(node, port)| (node_to_topo[&node], port))
        .collect();
    input_map.sort();
    // Convert back: input_map elements are (topo_position, port)

    let mut output_map: Vec<(usize, usize)> = group_output_set.into_iter()
        .map(|(node, port)| (node_to_topo[&node], port))
        .collect();
    output_map.sort();

    // Build reverse lookups.
    // (node, input_port) -> group input index
    let input_port_to_group_input: HashMap<(usize, usize), usize> = input_map.iter().enumerate()
        .map(|(i, &(topo_pos, port))| ((topo_order[topo_pos], port), i))
        .collect();

    // (node, output_port) -> group output indices
    let mut output_port_to_group_output: HashMap<(usize, usize), Vec<usize>> = HashMap::new();
    for (j, &(topo_pos, port)) in output_map.iter().enumerate() {
        output_port_to_group_output.entry((topo_order[topo_pos], port))
            .or_default()
            .push(j);
    }

    // Step 3: Compute member summaries (input_port, output_port) connections for each node.
    let mut member_summaries: HashMap<usize, Vec<(usize, usize)>> = HashMap::new();
    for &node in group.iter() {
        let mut connections = Vec::new();
        for (inp_idx, pc) in children[node].internal_summary.iter().enumerate() {
            for (out_port, _ac) in pc.iter_ports() {
                connections.push((inp_idx, out_port));
            }
        }
        member_summaries.insert(node, connections);
    }

    // Step 4: Compute capability_map via reachability (single reverse-topological pass).
    // capability_map[topo_pos][output_port] -> list of group output indices
    let capability_map = compute_all_reachability(
        &topo_order, children, &internal_edges, &member_summaries, &output_port_to_group_output,
    );

    // Step 5: Compute composed summary for the group.
    // For each (group_input_i, group_output_j): if there's a reachability path, set identity summary.
    let num_inputs = input_map.len();
    let num_outputs = output_map.len();

    let mut composed_summary: Connectivity<T::Summary> = Vec::with_capacity(num_inputs);
    for &(topo_pos, port) in input_map.iter() {
        let node = topo_order[topo_pos];
        let mut pc = PortConnectivity::default();

        // Find which group outputs are reachable from this input.
        // Use the node's summary to find output ports reachable from this input port,
        // then use capability_map for those output ports.
        if let Some(connections) = member_summaries.get(&node) {
            for &(inp, outp) in connections.iter() {
                if inp == port {
                    for &group_out in &capability_map[topo_pos][outp] {
                        pc.insert(group_out, Default::default());
                    }
                }
            }
        }
        composed_summary.push(pc);
    }

    // Step 6: Extract members in topological order.
    let mut members = Vec::with_capacity(topo_order.len());
    let mut group_name_parts = Vec::new();
    let mut representative_path = Vec::new();

    for &node in topo_order.iter() {
        let child = &mut children[node];
        let operator = child.operator.take().expect("group member must have an operator");
        let shared_progress = Rc::clone(&child.shared_progress);

        if node == representative {
            representative_path = operator.path().to_vec();
        }
        group_name_parts.push(child.name.clone());

        members.push(GroupMember {
            operator,
            shared_progress,
        });
    }

    let group_name = format!("Group[{}]", group_name_parts.join(", "));

    // Step 7: Create the group's SharedProgress.
    let group_progress = Rc::new(RefCell::new(SharedProgress::new(num_inputs, num_outputs)));

    // Transfer initial internal capabilities from ALL members to group_progress,
    // mapped through capability_map.
    {
        let mut group_sp = group_progress.borrow_mut();
        for (topo_pos, member) in members.iter().enumerate() {
            let mut member_sp = member.shared_progress.borrow_mut();
            for (port, internal) in member_sp.internals.iter_mut().enumerate() {
                for (time, diff) in internal.iter() {
                    for &group_out in &capability_map[topo_pos][port] {
                        group_sp.internals[group_out].update(time.clone(), *diff);
                    }
                }
            }
        }
    }

    // Clear all members' internals to prevent double-counting during initialize().
    for member in members.iter() {
        let mut sp = member.shared_progress.borrow_mut();
        for batch in sp.internals.iter_mut() { batch.clear(); }
    }

    let group_scheduler: Box<dyn Schedule> = Box::new(GroupScheduler {
        name: group_name.clone(),
        path: representative_path,
        group_progress: Rc::clone(&group_progress),
        members,
        input_map: input_map.clone(),
        output_map: output_map.clone(),
        capability_map,
    });

    // Step 8: Install the fused operator at the representative slot.
    // Edges are left empty here; the build method populates them from edge_stash.
    let head = &mut children[representative];
    head.name = group_name;
    head.operator = Some(group_scheduler);
    head.shared_progress = group_progress;
    head.internal_summary = composed_summary;
    head.notify = vec![FrontierInterest::Never; num_inputs];
    head.inputs = num_inputs;
    head.outputs = num_outputs;
    head.edges = vec![Vec::new(); num_outputs];

    // Step 9: Tombstone all other group members, forwarding activations to representative.
    for &node in group.iter() {
        if node == representative { continue; }
        let child = &mut children[node];
        child.name = format!("Tombstone({})", child.name);
        child.operator = None;
        child.shared_progress = Rc::new(RefCell::new(SharedProgress::new(0, 0)));
        child.edges = Vec::new();
        child.inputs = 0;
        child.outputs = 0;
        child.internal_summary = Vec::new();
        child.forward_to = Some(representative);
    }

    // Step 10: Rewrite edge_stash.
    // Remove edges where both endpoints are in the group.
    // Rewrite edges incoming to group members: target.node = representative, target.port = group_input_index.
    // Rewrite edges outgoing from group members: source.node = representative, source.port = group_output_index.
    let mut new_edge_stash: Vec<(Source, Target)> = Vec::new();

    for (source, target) in edge_stash.iter() {
        let src_in = group_set.contains(&source.node);
        let tgt_in = group_set.contains(&target.node);

        if src_in && tgt_in {
            // Internal edge: remove.
            continue;
        } else if !src_in && tgt_in {
            // Incoming edge: rewrite target.
            if let Some(&group_input) = input_port_to_group_input.get(&(target.node, target.port)) {
                new_edge_stash.push((
                    *source,
                    Target::new(representative, group_input),
                ));
            }
        } else if src_in && !tgt_in {
            // Outgoing edge: rewrite source.
            let topo_pos = node_to_topo[&source.node];
            if let Some(group_outs) = output_port_to_group_output.get(&(source.node, source.port)) {
                for &group_out in group_outs {
                    if output_map[group_out] == (topo_pos, source.port) {
                        new_edge_stash.push((
                            Source::new(representative, group_out),
                            *target,
                        ));
                    }
                }
            }
        } else {
            // Neither endpoint in group: keep as-is.
            new_edge_stash.push((*source, *target));
        }
    }

    *edge_stash = new_edge_stash;
}
