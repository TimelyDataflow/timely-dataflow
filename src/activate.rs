//! Parking and unparking timely fibers.

use std::collections::BTreeSet;
use std::collections::btree_set::Range;

/// Tracks a set of active paths.
///
/// This struct is currently based on a BTreeSet<Vec<usize>>,
/// whereas it could be more efficiently packed with fewer
/// allocations, especially in its methods which require some
/// allocations.
pub struct Activations {
    active: BTreeSet<Vec<usize>>,
}

impl Activations {
    /// Allocates a new activation tracker.
    pub fn new() -> Self {
        Self { active: BTreeSet::new() }
    }
    /// Mark a path as inactive.
    pub fn park(&mut self, path: &[usize]) {
        self.active.remove(path);
    }
    /// Mark a path as active.
    pub fn unpark(&mut self, path: &[usize]) {
        self.active.insert(path.to_vec());
    }
    /// Return active paths in an interval.
    pub fn range(&self, lower: &[usize], upper: &[usize]) -> Range<Vec<usize>> {
        let lower = lower.to_vec();
        let upper = upper.to_vec();
        self.active.range(lower .. upper)
    }
}

use std::rc::Rc;
use std::cell::RefCell;

/// A handle to activate a specific path.
pub struct ActivationHandle {
    path: Vec<usize>,
    queue: Rc<RefCell<Vec<Vec<usize>>>>,
}

impl ActivationHandle {
    /// Creates a new activation handle
    pub fn new(path: &[usize], queue: Rc<RefCell<Vec<Vec<usize>>>>) -> Self {
        Self {
            path: path.to_vec(),
            queue,
        }
    }
    /// Activates the associated path.
    pub fn activate(&self) {
        self.queue.borrow_mut().push(self.path.clone());
    }
}
