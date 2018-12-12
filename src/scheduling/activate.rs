//! Parking and unparking timely fibers.

use std::rc::Rc;
use std::cell::RefCell;

/// A capability to activate a specific path.
pub struct ActivationHandle {
    path: Vec<usize>,
    queue: Rc<RefCell<Activations>>,
}

impl ActivationHandle {
    /// Creates a new activation handle
    pub fn new(path: &[usize], queue: Rc<RefCell<Activations>>) -> Self {
        Self {
            path: path.to_vec(),
            queue,
        }
    }
    /// Activates the associated path.
    pub fn activate(&self) {
        self.queue
            .borrow_mut()
            .unpark(&self.path[..]);
    }
}

/// A wrapper that unparks on drop.
pub struct UnparkOnDrop<'a, T>  {
    wrapped: T,
    address: &'a [usize],
    activator: Rc<RefCell<Activations>>,
}

use std::ops::{Deref, DerefMut};

impl<'a, T> UnparkOnDrop<'a, T> {
    /// Wraps an element so that it is unparked on drop.
    pub fn new(wrapped: T, address: &'a [usize], activator: Rc<RefCell<Activations>>) -> Self {
        Self { wrapped, address, activator }
    }
}

impl<'a, T> Deref for UnparkOnDrop<'a, T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        &self.wrapped
    }
}

impl<'a, T> DerefMut for UnparkOnDrop<'a, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.wrapped
    }
}

impl<'a, T> Drop for UnparkOnDrop<'a, T> {
    fn drop(&mut self) {
        self.activator.borrow_mut().unpark(self.address);
    }
}

/// Allocation-free activation tracker.
///
/// This activity tracker accepts `park` and `unpark` commands, but they only
/// become visible after a call to `tidy`, which puts the commands in sorted
/// order by path, and retains only those paths whose last command was unpark.
pub struct Activations {
    clean: usize,
    /// `(offset, length, unpark)`
    bounds: Vec<(usize, usize, bool)>,
    slices: Vec<usize>,
    buffer: Vec<usize>,
}

impl Activations {

    /// Creates a new activation tracker.
    pub fn new() -> Self {
        Self {
            clean: 0,
            bounds: Vec::new(),
            slices: Vec::new(),
            buffer: Vec::new(),
        }
    }

    /// Parks task addressed by `path`.
    pub fn park(&mut self, path: &[usize]) {
        self.bounds.push((self.slices.len(), path.len(), false));
        self.slices.extend(path);
    }
    /// Unparks task addressed by `path`.
    pub fn unpark(&mut self, path: &[usize]) {
        self.bounds.push((self.slices.len(), path.len(), true));
        self.slices.extend(path);
    }

    /// Removes duplicate
    pub fn tidy(&mut self) {

        {   // Scoped, to allow borrow to drop.
            let slices = &self.slices[..];
            // Sort slices, retaining park/unpark order.
            self.bounds.sort_by_key(|x| &slices[x.0 .. (x.0 + x.1)]);
            // Discard all but the most recent state.
            self.bounds.reverse();
            self.bounds.dedup_by_key(|x| &slices[x.0 .. (x.0 + x.1)]);
            self.bounds.reverse();
            // Retain unpark events.
            self.bounds.retain(|x| x.2);
        }

        // Compact the slices.
        self.buffer.clear();
        for (offset, length, _unpark) in self.bounds.iter_mut() {
            self.buffer.extend(&self.slices[*offset .. (*offset + *length)]);
            *offset = self.buffer.len() - *length;
        }
        ::std::mem::swap(&mut self.buffer, &mut self.slices);

        self.clean = self.bounds.len();
    }

    /// Sets as active any symbols that follow `path`.
    pub fn extensions(&self, path: &[usize], active: &mut Vec<usize>) {

        let position =
        self.bounds[..self.clean]
            .binary_search_by_key(&path, |x| &self.slices[x.0 .. (x.0 + x.1)]);
        let position = match position {
            Ok(x) => x,
            Err(x) => x,
        };

        self.bounds
            .iter()
            .cloned()
            .skip(position)
            .map(|(offset, length, _)| &self.slices[offset .. (offset + length)])
            .take_while(|x| x.starts_with(path))
            .for_each(|x| {
                // push non-empty, non-duplicate extensions.
                if let Some(extension) = x.get(path.len()) {
                    if active.last() != Some(extension) {
                        active.push(*extension);
                    }
                }
            });

        active.dedup();
    }
}