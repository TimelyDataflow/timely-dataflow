//! Parking and unparking timely fibers.

use std::rc::Rc;
use std::cell::RefCell;

/// Allocation-free activation tracker.
pub struct Activations {
    clean: usize,
    /// `(offset, length)`
    bounds: Vec<(usize, usize)>,
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

    /// Unparks task addressed by `path`.
    pub fn activate(&mut self, path: &[usize]) {
        self.bounds.push((self.slices.len(), path.len()));
        self.slices.extend(path);
    }

    /// Discards the current active set and presents the next active set.
    pub fn advance(&mut self) {

        self.bounds.drain(.. self.clean);

        {   // Scoped, to allow borrow to drop.
            let slices = &self.slices[..];
            self.bounds.sort_by_key(|x| &slices[x.0 .. (x.0 + x.1)]);
            self.bounds.dedup_by_key(|x| &slices[x.0 .. (x.0 + x.1)]);
        }

        // Compact the slices.
        self.buffer.clear();
        for (offset, length) in self.bounds.iter_mut() {
            self.buffer.extend(&self.slices[*offset .. (*offset + *length)]);
            *offset = self.buffer.len() - *length;
        }
        ::std::mem::swap(&mut self.buffer, &mut self.slices);

        self.clean = self.bounds.len();
    }

    ///
    pub fn map_active(&self, logic: impl Fn(&[usize])) {
        for (offset, length) in self.bounds.iter() {
            logic(&self.slices[*offset .. (*offset + *length)]);
        }
    }

    /// Sets as active any symbols that follow `path`.
    pub fn for_extensions(&self, path: &[usize], mut action: impl FnMut(usize)) {

        let position =
        self.bounds[..self.clean]
            .binary_search_by_key(&path, |x| &self.slices[x.0 .. (x.0 + x.1)]);
        let position = match position {
            Ok(x) => x,
            Err(x) => x,
        };

        let mut previous = None;
        self.bounds
            .iter()
            .cloned()
            .skip(position)
            .map(|(offset, length)| &self.slices[offset .. (offset + length)])
            .take_while(|x| x.starts_with(path))
            .for_each(|x| {
                // push non-empty, non-duplicate extensions.
                if let Some(extension) = x.get(path.len()) {
                    if previous != Some(*extension) {
                        action(*extension);
                        previous = Some(*extension);
                    }
                }
            });
    }
}

/// A capability to activate a specific path.
pub struct Activator {
    path: Vec<usize>,
    queue: Rc<RefCell<Activations>>,
}

impl Activator {
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
            .activate(&self.path[..]);
    }
}

/// A wrapper that unparks on drop.
pub struct ActivateOnDrop<T>  {
    wrapped: T,
    address: Rc<Vec<usize>>,
    activator: Rc<RefCell<Activations>>,
}

use std::ops::{Deref, DerefMut};

impl<T> ActivateOnDrop<T> {
    /// Wraps an element so that it is unparked on drop.
    pub fn new(wrapped: T, address: Rc<Vec<usize>>, activator: Rc<RefCell<Activations>>) -> Self {
        Self { wrapped, address, activator }
    }
}

impl<T> Deref for ActivateOnDrop<T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        &self.wrapped
    }
}

impl<T> DerefMut for ActivateOnDrop<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.wrapped
    }
}

impl<T> Drop for ActivateOnDrop<T> {
    fn drop(&mut self) {
        self.activator.borrow_mut().activate(&self.address[..]);
    }
}