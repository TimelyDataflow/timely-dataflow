//! Methods which describe an operators topology, and the progress it makes.

use std::rc::Rc;
use std::cell::RefCell;

use crate::scheduling::Schedule;
use crate::progress::{Timestamp, ChangeBatch, Antichain};

/// A dataflow operator that progress with a specific timestamp type.
///
/// This trait describes the methods necessary to present as a dataflow operator.
/// This trait is a "builder" for operators, in that it reveals the structure of the operator
/// and its requirements, but then (through `initialize`) consumes itself to produce a boxed
/// schedulable object. At the moment of initialization, the values of the other methods are
/// captured and frozen.
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

    /// Initializes the operator, converting the operator builder to a schedulable object.
    ///
    /// In addition, initialization produces internal connectivity, and a shared progress conduit
    /// which must contain any initial output capabilities the operator would like to hold.
    ///
    /// The internal connectivity summarizes the operator by a map from pairs `(input, output)`
    /// to an antichain of timestamp summaries, indicating how a timestamp on any of its inputs may
    /// be transformed to timestamps on any of its outputs. The conservative and most common result
    /// is full connectivity between all inputs and outputs, each with the identity summary.
    ///
    /// The shared progress object allows information to move between the host and the schedulable.
    /// Importantly, it also indicates the initial internal capabilities for all of its outputs.
    /// This must happen at this moment, as it is the only moment where an operator is allowed to
    /// safely "create" capabilities without basing them on other, prior capabilities.
    fn initialize(self: Box<Self>) -> (Connectivity<T::Summary>, Rc<RefCell<SharedProgress<T>>>, Box<dyn Schedule>);

    /// Indicates for each input whether the operator should be invoked when that input's frontier changes.
    ///
    /// Returns a `Vec<FrontierInterest>` with one entry per input. Each entry describes whether
    /// frontier changes on that input should cause the operator to be scheduled. The conservative
    /// default is `Always` for each input.
    fn notify_me(&self) -> &[FrontierInterest];// { &vec![FrontierInterest::Always; self.inputs()] }
}

/// The ways in which an operator can express interest in activation when an input frontier changes.
#[derive(Ord, PartialOrd, Eq, PartialEq, Copy, Clone, Debug)]
pub enum FrontierInterest {
    /// Never interested in frontier changes, as for example the `map()` and `filter()` operators.
    Never,
    /// Interested when the operator holds capabilities.
    IfCapability,
    /// Always interested in frontier changes, as for example the `probe()` and `capture()` operators.
    Always,
}

/// Operator internal connectivity, from inputs to outputs.
pub type Connectivity<TS> = Vec<PortConnectivity<TS>>;

/// Tagged view of the summary attached to a single output port.
///
/// `Default` indicates the identity summary (`TS::default()`) is implied for that
/// port, with no element stored. `Specific` borrows the materialized antichain for
/// summaries that contain any non-default element.
#[derive(Debug)]
pub enum PortEntry<'a, TS> {
    /// Identity (default) summary, stored implicitly.
    Default,
    /// A non-default antichain of summaries.
    Specific(&'a Antichain<TS>),
}

/// Internal connectivity from one port to any number of opposing ports.
///
/// Default summaries are represented implicitly via a bitset, avoiding per-edge
/// heap allocation in the dominant case. Non-default summaries live in a sparse
/// map. Invariant: a port appears in `defaults` xor `specifics`, never both, and
/// `specifics` never holds an antichain that is exactly a single default element
/// (such antichains are demoted into `defaults`).
#[derive(serde::Serialize, serde::Deserialize, columnar::Columnar, Debug, Clone, Eq, PartialEq)]
pub struct PortConnectivity<TS> {
    /// Bitset of output ports with default (identity) summary. Bit `p` of
    /// `defaults[p / 64]` set iff port `p` has the identity summary.
    defaults: smallvec::SmallVec<[u64; 2]>,
    /// Non-default antichains, keyed by output port. Disjoint from `defaults`.
    specifics: std::collections::BTreeMap<usize, Antichain<TS>>,
}

impl<TS> Default for PortConnectivity<TS> {
    fn default() -> Self {
        Self {
            defaults: smallvec::SmallVec::new(),
            specifics: std::collections::BTreeMap::new(),
        }
    }
}

impl<TS> PortConnectivity<TS> {
    fn default_bit(&self, port: usize) -> bool {
        let (word, bit) = (port / 64, port % 64);
        self.defaults.get(word).is_some_and(|w| (w >> bit) & 1 == 1)
    }

    fn set_default_bit(&mut self, port: usize) {
        let (word, bit) = (port / 64, port % 64);
        if self.defaults.len() <= word {
            self.defaults.resize(word + 1, 0);
        }
        self.defaults[word] |= 1u64 << bit;
    }

    fn clear_default_bit(&mut self, port: usize) {
        let (word, bit) = (port / 64, port % 64);
        if let Some(w) = self.defaults.get_mut(word) {
            *w &= !(1u64 << bit);
        }
    }

    /// True if `port` has any connection (default or specific summary).
    pub fn contains(&self, port: usize) -> bool {
        self.default_bit(port) || self.specifics.contains_key(&port)
    }

    /// True if `port` has the identity (default) summary, and only the default.
    pub fn is_default(&self, port: usize) -> bool {
        self.default_bit(port)
    }

    /// Returns the non-default antichain at `port`, if any.
    pub fn specific(&self, port: usize) -> Option<&Antichain<TS>> {
        self.specifics.get(&port)
    }

    /// Returns the entry for `port`, if any.
    pub fn get(&self, port: usize) -> Option<PortEntry<'_, TS>> {
        if self.default_bit(port) {
            Some(PortEntry::Default)
        } else {
            self.specifics.get(&port).map(PortEntry::Specific)
        }
    }

    /// Iterates port indices that hold the identity summary, in ascending order.
    pub fn iter_defaults(&self) -> impl Iterator<Item = usize> + '_ {
        self.defaults
            .iter()
            .enumerate()
            .flat_map(|(word_idx, &word)| BitIter { word, base: word_idx * 64 })
    }

    /// Iterates port indices with non-default summaries and their antichains.
    pub fn iter_specifics(&self) -> impl Iterator<Item = (usize, &Antichain<TS>)> {
        self.specifics.iter().map(|(p, ac)| (*p, ac))
    }

    /// Iterates `(port, entry)` for all connected ports. Defaults precede specifics.
    pub fn iter_ports(&self) -> impl Iterator<Item = (usize, PortEntry<'_, TS>)> {
        self.iter_defaults()
            .map(|p| (p, PortEntry::Default))
            .chain(self.iter_specifics().map(|(p, ac)| (p, PortEntry::Specific(ac))))
    }

    /// Iterates `(port, summary)` flattening default-port entries to `TS::default()`
    /// and each element of every specific antichain. No sorting guarantee on `port`.
    pub fn iter_summaries_owned(&self) -> impl Iterator<Item = (usize, TS)> + '_
    where
        TS: Default + Clone,
    {
        self.iter_defaults()
            .map(|p| (p, TS::default()))
            .chain(self.iter_specifics().flat_map(|(p, ac)| {
                ac.elements().iter().map(move |s| (p, s.clone()))
            }))
    }

    /// Invokes `pred` for each summary attached to `port`, returning `true` on the
    /// first hit. Materializes a `TS::default()` for the default-bit case.
    pub fn any_summary<F>(&self, port: usize, mut pred: F) -> bool
    where
        TS: Default,
        F: FnMut(&TS) -> bool,
    {
        if self.default_bit(port) {
            return pred(&TS::default());
        }
        if let Some(ac) = self.specifics.get(&port) {
            return ac.elements().iter().any(pred);
        }
        false
    }

    /// Restore canonical form at `port`: if `specifics[port]` holds exactly the
    /// default summary as a single element, demote it into the `defaults` bitset.
    fn canonicalize(&mut self, port: usize)
    where
        TS: Default + Eq,
    {
        let demote = self
            .specifics
            .get(&port)
            .is_some_and(|ac| ac.elements().len() == 1 && ac.elements()[0] == TS::default());
        if demote {
            self.specifics.remove(&port);
            self.set_default_bit(port);
        }
    }

    /// Inserts `element` into the antichain at `port`, returning `true` if the
    /// antichain changed.
    pub fn insert(&mut self, port: usize, element: TS) -> bool
    where
        TS: crate::PartialOrder + Default + Eq + Clone,
    {
        use std::collections::btree_map::Entry;

        if self.default_bit(port) {
            if element == TS::default() {
                return false;
            }
            // Promote the existing default into `specifics` so the mixed
            // antichain has both the default and the new element.
            self.clear_default_bit(port);
            let mut ac = Antichain::new();
            ac.insert(TS::default());
            let prior = self.specifics.insert(port, ac);
            debug_assert!(prior.is_none());
        }
        let changed = match self.specifics.entry(port) {
            Entry::Vacant(e) => {
                if element == TS::default() {
                    self.set_default_bit(port);
                } else {
                    let mut ac = Antichain::new();
                    ac.insert(element);
                    e.insert(ac);
                }
                return true;
            }
            Entry::Occupied(mut e) => e.get_mut().insert(element),
        };
        self.canonicalize(port);
        changed
    }

    /// As [`insert`], but takes the element by reference.
    pub fn insert_ref(&mut self, port: usize, element: &TS) -> bool
    where
        TS: crate::PartialOrder + Default + Eq + Clone,
    {
        use std::collections::btree_map::Entry;

        if self.default_bit(port) {
            if element == &TS::default() {
                return false;
            }
            self.clear_default_bit(port);
            let mut ac = Antichain::new();
            ac.insert(TS::default());
            let prior = self.specifics.insert(port, ac);
            debug_assert!(prior.is_none());
        }
        let changed = match self.specifics.entry(port) {
            Entry::Vacant(e) => {
                if element == &TS::default() {
                    self.set_default_bit(port);
                } else {
                    let mut ac = Antichain::new();
                    ac.insert_ref(element);
                    e.insert(ac);
                }
                return true;
            }
            Entry::Occupied(mut e) => e.get_mut().insert_ref(element),
        };
        self.canonicalize(port);
        changed
    }

    /// Introduces a summary for `port`. Panics if a summary already exists.
    pub fn add_port(&mut self, port: usize, summary: Antichain<TS>)
    where
        TS: Default + Eq,
    {
        if summary.is_empty() {
            assert!(
                !self.default_bit(port) && !self.specifics.contains_key(&port),
                "add_port with empty summary on already-connected port"
            );
            return;
        }
        let is_default_only =
            summary.elements().len() == 1 && summary.elements()[0] == TS::default();
        if is_default_only {
            assert!(
                !self.default_bit(port) && !self.specifics.contains_key(&port),
                "add_port called on port that already has a summary"
            );
            self.set_default_bit(port);
        } else {
            assert!(
                !self.default_bit(port),
                "add_port called on port that already has a default summary"
            );
            let prior = self.specifics.insert(port, summary);
            assert!(prior.is_none(), "add_port called on port that already has a summary");
        }
    }
}

impl<TS> FromIterator<(usize, Antichain<TS>)> for PortConnectivity<TS>
where
    TS: Default + Eq,
{
    fn from_iter<T>(iter: T) -> Self
    where
        T: IntoIterator<Item = (usize, Antichain<TS>)>,
    {
        let mut out = Self::default();
        for (port, ac) in iter {
            if ac.is_empty() {
                continue;
            }
            let is_default_only =
                ac.elements().len() == 1 && ac.elements()[0] == TS::default();
            if is_default_only {
                out.set_default_bit(port);
            } else {
                out.specifics.insert(port, ac);
            }
        }
        out
    }
}

/// Iterates over the set bits of a single 64-bit word, yielding bit indices
/// (offset by `base`) in ascending order.
struct BitIter {
    word: u64,
    base: usize,
}

impl Iterator for BitIter {
    type Item = usize;
    fn next(&mut self) -> Option<usize> {
        if self.word == 0 {
            None
        } else {
            let bit = self.word.trailing_zeros() as usize;
            self.word &= self.word - 1;
            Some(self.base + bit)
        }
    }
}

/// Progress information shared between parent and child.
#[derive(Debug)]
pub struct SharedProgress<T: Timestamp> {
    /// Frontier capability changes reported by the parent scope.
    pub frontiers: Vec<ChangeBatch<T>>,
    /// Consumed message changes reported by the child operator.
    pub consumeds: Vec<ChangeBatch<T>>,
    /// Internal capability changes reported by the child operator.
    pub internals: Vec<ChangeBatch<T>>,
    /// Produced message changes reported by the child operator.
    pub produceds: Vec<ChangeBatch<T>>,
}

impl<T: Timestamp> SharedProgress<T> {
    /// Allocates a new shared progress structure.
    pub fn new(inputs: usize, outputs: usize) -> Self {
        SharedProgress {
            frontiers: vec![ChangeBatch::new(); inputs],
            consumeds: vec![ChangeBatch::new(); inputs],
            internals: vec![ChangeBatch::new(); outputs],
            produceds: vec![ChangeBatch::new(); outputs],
        }
    }
}
