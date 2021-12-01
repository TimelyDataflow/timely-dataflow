//! Utilities for experiments

use timely::communication::Push;
use timely::dataflow::channels::Bundle;
use timely::Data;

#[derive(Default, Debug)]
pub struct DropPusher {}

impl<T: Eq + Data, D: Data> Push<Bundle<T, D>> for DropPusher {
    fn push(&mut self, element: &mut Option<Bundle<T, D>>) {
        element.take();
    }
}

#[derive(Default, Debug)]
pub struct ReturnPusher {}

impl<T: Eq + Data, D: Data> Push<Bundle<T, D>> for ReturnPusher {
    fn push(&mut self, element: &mut Option<Bundle<T, D>>) {
        if let Some(message) = element.as_mut().and_then(|e| e.if_mut()) {
            message.data.clear();
        }
    }
}
