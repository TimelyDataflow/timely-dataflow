//! The exchange pattern distributes pushed data between many target pushees.

use crate::communication::Push;
use crate::container::{ContainerBuilder, PushInto};
use crate::dataflow::channels::Message;
use crate::{Container, Data};

// TODO : Software write combining
/// Distributes records among target pushees according to a distribution function.
pub struct Exchange<T, CB, P, H>
where
    CB: ContainerBuilder<Container: Container>,
    P: Push<Message<T, CB::Container>>,
    for<'a> H: FnMut(&<CB::Container as Container>::Item<'a>) -> u64
{
    pushers: Vec<P>,
    builders: Vec<CB>,
    current: Option<T>,
    hash_func: H,
}

impl<T: Clone, CB, P, H>  Exchange<T, CB, P, H>
where
    CB: ContainerBuilder<Container: Container>,
    P: Push<Message<T, CB::Container>>,
    for<'a> H: FnMut(&<CB::Container as Container>::Item<'a>) -> u64
{
    /// Allocates a new `Exchange` from a supplied set of pushers and a distribution function.
    pub fn new(pushers: Vec<P>, key: H) -> Exchange<T, CB, P, H> {
        let mut builders = vec![];
        for _ in 0..pushers.len() {
            builders.push(Default::default());
        }
        Exchange {
            pushers,
            hash_func: key,
            builders,
            current: None,
        }
    }
    #[inline]
    fn flush(&mut self, index: usize) {
        while let Some(container) = self.builders[index].finish() {
            if let Some(ref time) = self.current {
                Message::push_at(container, time.clone(), &mut self.pushers[index]);
            }
        }
    }
}

impl<T: Eq+Data, CB, P, H> Push<Message<T, CB::Container>> for Exchange<T, CB, P, H>
where
    CB: ContainerBuilder<Container: Container>,
    CB: for<'a> PushInto<<CB::Container as Container>::Item<'a>>,
    P: Push<Message<T, CB::Container>>,
    for<'a> H: FnMut(&<CB::Container as Container>::Item<'a>) -> u64
{
    #[inline(never)]
    fn push(&mut self, message: &mut Option<Message<T, CB::Container>>) {
        // if only one pusher, no exchange
        if self.pushers.len() == 1 {
            self.pushers[0].push(message);
        }
        else if let Some(message) = message {

            let time = &message.time;
            let data = &mut message.data;

            // if the time isn't right, flush everything.
            if self.current.as_ref().is_some_and(|x| x != time) {
                for index in 0..self.pushers.len() {
                    self.flush(index);
                }
            }
            self.current = Some(time.clone());

            let hash_func = &mut self.hash_func;

            // if the number of pushers is a power of two, use a mask
            if self.pushers.len().is_power_of_two() {
                let mask = (self.pushers.len() - 1) as u64;
                CB::partition(data, &mut self.builders, |datum| ((hash_func)(datum) & mask) as usize);
            }
            // as a last resort, use mod (%)
            else {
                let num_pushers = self.pushers.len() as u64;
                CB::partition(data, &mut self.builders, |datum| ((hash_func)(datum) % num_pushers) as usize);
            }
            for (buffer, pusher) in self.builders.iter_mut().zip(self.pushers.iter_mut()) {
                while let Some(container) = buffer.extract() {
                    Message::push_at(container, time.clone(), pusher);
                }
            }
        }
        else {
            // flush
            for index in 0..self.pushers.len() {
                self.flush(index);
                self.builders[index].relax();
                self.pushers[index].push(&mut None);
            }
        }
    }
}
