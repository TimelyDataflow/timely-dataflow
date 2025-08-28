//! The exchange pattern distributes pushed data between many target pushees.

use crate::communication::Push;
use crate::container::{ContainerBuilder, PushInto};
use crate::dataflow::channels::Message;
use crate::Container;

/// Distribute containers to several pushers.
///
/// A distributor sits behind an exchange pusher, and partitions containers at a given time
/// into several pushers. It can use [`Message::push_at`] to push its outputs at the desired
/// pusher.
///
/// It needs to uphold progress tracking requirements. The count of the input container
/// must be preserved across the output containers, from the first call to `partition` until the
/// call to `flush` for a specific time stamp.
pub trait Distributor<C> {
    /// Partition the contents of `container` at `time` into the `pushers`.
    fn partition<T: Clone, P: Push<Message<T, C>>>(&mut self, container: &mut C, time: &T, pushers: &mut [P]);
    /// Flush any remaining contents into the `pushers` at time `time`.
    fn flush<T: Clone, P: Push<Message<T, C>>>(&mut self, time: &T, pushers: &mut [P]);
    /// Optionally release resources, such as memory.
    fn relax(&mut self);
}

/// A distributor creating containers from a drainable container based
/// on a hash function of the container's item.
pub struct DrainContainerDistributor<CB, H> {
    builders: Vec<CB>,
    hash_func: H,
}

impl<CB: Default, H> DrainContainerDistributor<CB, H> {
    /// Constructs a new `DrainContainerDistributor` with the given hash function.
    pub fn new(hash_func: H) -> Self {
        Self {
            builders: Vec::new(),
            hash_func,
        }
    }
}

impl<CB, H> Distributor<CB::Container> for DrainContainerDistributor<CB, H>
where
    CB: ContainerBuilder + for<'a> PushInto<<CB::Container as Container>::Item<'a>>,
    for<'a> H: FnMut(&<CB::Container as Container>::Item<'a>) -> u64,
{
    fn partition<T: Clone, P: Push<Message<T, CB::Container>>>(&mut self, container: &mut CB::Container, time: &T, pushers: &mut [P]) {
        if self.builders.len() <= pushers.len() {
            self.builders.resize_with(pushers.len(), Default::default);
        }
        else {
            debug_assert_eq!(self.builders.len(), pushers.len());
        }
        if pushers.len().is_power_of_two() {
            let mask = (pushers.len() - 1) as u64;
            for datum in container.drain() {
                let index = ((self.hash_func)(&datum) & mask) as usize;
                self.builders[index].push_into(datum);
                while let Some(produced) = self.builders[index].extract() {
                    Message::push_at(produced, time.clone(), &mut pushers[index]);
                }
            }
        }
        else {
            let num_pushers = pushers.len() as u64;
            for datum in container.drain() {
                let index = ((self.hash_func)(&datum) % num_pushers) as usize;
                self.builders[index].push_into(datum);
                while let Some(produced) = self.builders[index].extract() {
                    Message::push_at(produced, time.clone(), &mut pushers[index]);
                }
            }
        }
    }

    fn flush<T: Clone, P: Push<Message<T, CB::Container>>>(&mut self, time: &T, pushers: &mut [P]) {
        for (builder, pusher) in self.builders.iter_mut().zip(pushers.iter_mut()) {
            while let Some(container) = builder.finish() {
                Message::push_at(container, time.clone(), pusher);
            }
        }
    }

    fn relax(&mut self) {
        for builder in &mut self.builders {
            builder.relax();
        }
    }
}

// TODO : Software write combining
/// Distributes records among target pushees according to a distributor.
pub struct Exchange<T, P, D> {
    pushers: Vec<P>,
    current: Option<T>,
    distributor: D,
}

impl<T: Clone, P, D>  Exchange<T, P, D> {
    /// Allocates a new `Exchange` from a supplied set of pushers and a distributor.
    pub fn new(pushers: Vec<P>, distributor: D) -> Exchange<T, P, D> {
        Exchange {
            pushers,
            current: None,
            distributor,
        }
    }
}

impl<T: Eq+Clone, C, P, D> Push<Message<T, C>> for Exchange<T, P, D>
where
    P: Push<Message<T, C>>,
    D: Distributor<C>,
{
    #[inline(never)]
    fn push(&mut self, message: &mut Option<Message<T, C>>) {
        // if only one pusher, no exchange
        if self.pushers.len() == 1 {
            self.pushers[0].push(message);
        }
        else if let Some(message) = message {

            let time = &message.time;
            let data = &mut message.data;

            // if the time isn't right, flush everything.
            match self.current.as_ref() {
                // We have a current time, and it is different from the new time.
                Some(current_time) if current_time != time => {
                    self.distributor.flush(current_time, &mut self.pushers);
                    self.current = Some(time.clone());
                }
                // We had no time before, or flushed.
                None => self.current = Some(time.clone()),
                // Time didn't change since last call.
                _ => {}
            }

            self.distributor.partition(data, time, &mut self.pushers);
        }
        else {
            // flush
            if let Some(time) = self.current.take() {
                self.distributor.flush(&time, &mut self.pushers);
            }
            self.distributor.relax();
            for index in 0..self.pushers.len() {
                self.pushers[index].push(&mut None);
            }
        }
    }
}
