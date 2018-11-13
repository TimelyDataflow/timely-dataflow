//! Parking and unparking timely fibers.

use progress::ChangeBatch;

/// A tree-structured representation of active tasks.
pub struct Activations {
    /// Indicates that this specific location is active.
    active: bool,
    /// Activity counts for immediate children.
    active_children: ChangeBatch<usize>,
    /// All activation states for immediate children.
    child_activations: Vec<Activations>,
}

impl Activations {

    /// Parks the task described by `path`.

    pub fn park(&mut self, path: &[usize]) {
        if self.is_active(path) {
            self.change_path(path, -1);
        }
    }

    /// Unparks the task described by `path`.
    pub fn unpark(&mut self, path: &[usize]) {
        if !self.is_active(path) {
            self.change_path(path, 1);
        }
    }

    /// Enumerates the active children of the task described by `path`.
    pub fn active_children<'a>(&'a mut self, path: &[usize]) -> impl Iterator<Item=usize>+'a {
        self.ensure(path)
            .active_children
            .iter()
            .cloned()
            .map(|(child,_diff)| child)
    }

    /// Allocates a new empty `Activations`.
    pub fn new() -> Self {
        Activations {
            active: false,
            active_children: ChangeBatch::new(),
            child_activations: Vec::new(),
        }
    }

    /// Ensures that a child path exists,
    pub fn is_active(&mut self, path: &[usize]) -> bool {
        self.ensure(path).active
    }

    /// Applies a change to the active counts along a path.
    fn change_path(&mut self, path: &[usize], delta: i64) -> bool {
        let mut node = self;
        for &child in path.iter() {
            node.active_children.update(child, delta);
            let temp = node;    // borrow to explain to Rust.
            node = &mut temp.child_activations[child];
        }
        node.active
    }

    /// Ensures, returns a reference to the state at a path.
    fn ensure(&mut self, path: &[usize]) -> &mut Activations {
        let mut node = self;
        for &child in path.iter() {
            while node.child_activations.len() <= child {
                node.child_activations.push(Activations::new());
            }
            let temp = node;    // borrow to explain to Rust.
            node = &mut temp.child_activations[child];
        }
        node
    }

}
