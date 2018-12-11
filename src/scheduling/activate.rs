//! Parking and unparking timely fibers.

use std::collections::BTreeSet;
use std::collections::btree_set::Range;

use std::rc::Rc;
use std::cell::RefCell;

/// Tracks a set of active paths.
///
/// This struct is currently based on a BTreeSet<Vec<usize>>,
/// whereas it could be more efficiently packed with fewer
/// allocations, especially in its methods which require some
/// allocations.
pub struct Activations {
    /// Active paths.
    pub active: BTreeSet<Vec<usize>>,
    /// Queued unparking requests.
    queued: Rc<RefCell<Vec<Vec<usize>>>>,
}

impl Activations {
    /// Allocates a new activation tracker.
    pub fn new() -> Self {
        Self {
            active: BTreeSet::new(),
            queued: Rc::new(RefCell::new(Vec::new())),
        }
    }
    /// Mark a path as inactive.
    pub fn park(&mut self, path: &[usize]) {
        // println!("Parking: {:?}", path);
        self.active.remove(path);
    }
    /// Mark a path as active.
    pub fn unpark(&mut self, path: &[usize]) {
        // println!("Unparking: {:?}", path);
        self.active.insert(path.to_vec());
    }
    /// Return active paths in an interval.
    pub fn range(&self, lower: &[usize], upper: &[usize]) -> Range<Vec<usize>> {
        let lower = lower.to_vec();
        let upper = upper.to_vec();
        self.active.range(lower .. upper)
    }
    /// Creates a capability to activate `path`.
    pub fn activator_for(&self, path: &[usize]) -> ActivationHandle {
        ActivationHandle::new(path, self.queued.clone())
    }

    /// Processes queued activations.
    pub fn drain_queued(&mut self) {
        let cloned = self.queued.clone();
        let mut borrow = cloned.borrow_mut();
        for path in borrow.drain(..) {
            self.unpark(&path[..]);
        }
    }
}

/// A capability to activate a specific path.
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
        // println!("ActivationHandle::activate() for path: {:?}", self.path);
        self.queue
            .borrow_mut()
            .push(self.path.clone());
    }
}
