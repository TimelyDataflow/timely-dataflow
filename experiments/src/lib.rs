//! Utilities for experiments

use itertools::Itertools;
use timely::communication::Push;
use timely::dataflow::channels::Bundle;
use timely::Data;

/// A pusher that drops data pushed at it.
#[derive(Default, Debug)]
pub struct DropPusher {}

impl<T: Eq + Data, D: Data> Push<Bundle<T, D>> for DropPusher {
    fn push(&mut self, element: &mut Option<Bundle<T, D>>) {
        element.take();
    }
}

/// A pusher that consumes the data by clearing the message's contents, but leaves the allocation.
#[derive(Default, Debug)]
pub struct ReturnPusher {}

impl<T: Eq + Data, D: Data> Push<Bundle<T, D>> for ReturnPusher {
    fn push(&mut self, element: &mut Option<Bundle<T, D>>) {
        if let Some(message) = element.as_mut().and_then(|e| e.if_mut()) {
            message.data.clear();
        }
    }
}

/// Construct experiment data of size `batch`.
///
/// Returns a `Vec<Vec<u64>>` where the inner vector contains up to [`default_capacity`] elements,
/// and all inner vectors together container `batch` many elements.
///
/// [`default_capacity`]: timely::container::buffer::default_capacity
pub fn construct_data(batch: u64) -> Vec<Vec<u64>> {
    (0..batch)
        .map(|x| x)
        .chunks(timely::container::buffer::default_capacity::<u64>())
        .into_iter()
        .map(|chunk| chunk.collect())
        .collect()
}
