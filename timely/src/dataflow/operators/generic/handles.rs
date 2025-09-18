//! Handles to an operator's input and output streams.
//!
//! These handles are used by the generic operator interfaces to allow user closures to interact as
//! the operator would with its input and output streams.

use std::rc::Rc;
use std::cell::RefCell;
use std::collections::VecDeque;

use crate::progress::Timestamp;
use crate::progress::ChangeBatch;
use crate::progress::operate::PortConnectivity;
use crate::dataflow::channels::pullers::Counter as PullCounter;
use crate::dataflow::channels::Message;
use crate::communication::Pull;
use crate::{Container, Accountable};
use crate::container::{ContainerBuilder, CapacityContainerBuilder, PushInto};

use crate::dataflow::operators::InputCapability;
use crate::dataflow::operators::capability::CapabilityTrait;

#[must_use]
pub struct InputSession<'a, T: Timestamp, C, P: Pull<Message<T, C>>> {
    input: &'a mut InputHandleCore<T, C, P>,
}

impl<'a, T: Timestamp, C: Accountable, P: Pull<Message<T, C>>> InputSession<'a, T, C, P> {
    /// Iterates through distinct capabilities and the lists of containers associated with each.
    pub fn for_each_time<F>(self, logic: F) where F: FnMut(InputCapability<T>, std::slice::IterMut::<C>), C: Default {
        self.input.for_each_time(logic)
    }
    /// Iterates through pairs of capability and container.
    ///
    /// The `for_each_time` method is equivalent, but groups containers by capability and is preferred,
    /// in that it often leads to grouping work by capability, including the creation of output sessions.
    pub fn for_each<F>(self, logic: F) where F: FnMut(InputCapability<T>, &mut C) {
        self.input.for_each(logic)
    }
}

/// Handle to an operator's input stream.
pub struct InputHandleCore<T: Timestamp, C, P: Pull<Message<T, C>>> {
    pull_counter: PullCounter<T, C, P>,
    internal: Rc<RefCell<Vec<Rc<RefCell<ChangeBatch<T>>>>>>,
    /// Timestamp summaries from this input to each output.
    ///
    /// Each timestamp received through this input may only produce output timestamps
    /// greater or equal to the input timestamp subjected to at least one of these summaries.
    summaries: Rc<RefCell<PortConnectivity<T::Summary>>>,
    /// Staged capabilities and containers.
    staging: VecDeque<(InputCapability<T>, C)>,
    staged: Vec<C>,
}

impl<T: Timestamp, C: Accountable, P: Pull<Message<T, C>>> InputHandleCore<T, C, P> {

    /// Activates an input handle with a session that reorders inputs and must be drained.
    pub fn activate(&mut self) -> InputSession<'_, T, C, P> { InputSession { input: self } }

    /// Reads the next input buffer (at some timestamp `t`) and a corresponding capability for `t`.
    /// The timestamp `t` of the input buffer can be retrieved by invoking `.time()` on the capability.
    /// Returns `None` when there's no more data available.
    #[inline]
    pub fn next(&mut self) -> Option<(InputCapability<T>, &mut C)> {
        let internal = &self.internal;
        let summaries = &self.summaries;
        self.pull_counter.next_guarded().map(|(guard, bundle)| {
            (InputCapability::new(Rc::clone(internal), Rc::clone(summaries), guard), &mut bundle.data)
        })
    }
    /// Iterates through pairs of capability and container.
    ///
    /// The `for_each_time` method is equivalent, but groups containers by capability and is preferred,
    /// in that it often leads to grouping work by capability, including the creation of output sessions.
    pub fn for_each<F>(&mut self, mut logic: F) where F: FnMut(InputCapability<T>, &mut C) {
        while let Some((cap, data)) = self.next() { logic(cap, data); }
    }
    /// Iterates through distinct capabilities and the lists of containers associated with each.
    pub fn for_each_time<F>(&mut self, mut logic: F) where F: FnMut(InputCapability<T>, std::slice::IterMut::<C>), C: Default {
        while let Some((cap, data)) = self.next() {
            let data = std::mem::take(data);
            self.staging.push_back((cap, data));
        }
        self.staging.make_contiguous().sort_by(|x,y| x.0.time().cmp(&y.0.time()));

        while let Some((cap, data)) = self.staging.pop_front() {
            self.staged.push(data);
            let more = self.staging.iter().take_while(|(c,_)| c.time() == cap.time()).count();
            self.staged.extend(self.staging.drain(..more).map(|(_,d)| d));
            logic(cap, self.staged.iter_mut());
            // Could return these back to the input ..
            self.staged.clear();
        }
    }
}

pub fn _access_pull_counter<T: Timestamp, C: Accountable, P: Pull<Message<T, C>>>(input: &mut InputHandleCore<T, C, P>) -> &mut PullCounter<T, C, P> {
    &mut input.pull_counter
}

/// Constructs an input handle.
/// Declared separately so that it can be kept private when `InputHandle` is re-exported.
pub fn new_input_handle<T: Timestamp, C: Accountable, P: Pull<Message<T, C>>>(
    pull_counter: PullCounter<T, C, P>,
    internal: Rc<RefCell<Vec<Rc<RefCell<ChangeBatch<T>>>>>>,
    summaries: Rc<RefCell<PortConnectivity<T::Summary>>>,
) -> InputHandleCore<T, C, P> {
    InputHandleCore {
        pull_counter,
        internal,
        summaries,
        staging: Default::default(),
        staged: Default::default(),
    }
}

/// An owning pair of output pusher and container builder.
pub struct OutputBuilder<T: Timestamp, CB: ContainerBuilder> {
    output: crate::dataflow::channels::pushers::Output<T, CB::Container>,
    builder: CB,
}

impl<T: Timestamp, CB: ContainerBuilder> OutputBuilder<T, CB> {
    /// Constructs an output builder from an output and a default container builder.
    pub fn from(output: crate::dataflow::channels::pushers::Output<T, CB::Container>) -> Self {
        Self { output, builder: CB::default() }
    }
    /// An activated output buffer for building containers.
    pub fn activate<'a>(&'a mut self) -> OutputBuffer<'a, T, CB> {
        OutputBuffer {
            session: self.output.activate(),
            builder: &mut self.builder,
        }
    }
}

/// A wrapper around a live output session, with a container builder to buffer.
pub struct OutputBuffer<'a, T: Timestamp, CB: ContainerBuilder> {
    session: crate::dataflow::channels::pushers::OutputSession<'a, T, CB::Container>,
    builder: &'a mut CB,
}

impl<'a, T: Timestamp, CB: ContainerBuilder> OutputBuffer<'a, T, CB> {
    /// A container-building session associated with a capability.
    ///
    /// This method is the prefered way of sending records that must be accumulated into a container,
    /// as it avoid the recurring overhead of capability validation.
    pub fn session_with_builder<'b, CT: CapabilityTrait<T>>(&'b mut self, capability: &'b CT) -> Session<'a, 'b, T, CB, CT> where 'a: 'b {
        debug_assert!(self.session.valid(capability));
        Session {
            buffer: self,
            capability,
        }
    }
}

impl<'a, T: Timestamp, C: Container> OutputBuffer<'a, T, CapacityContainerBuilder<C>> {
    /// A container-building session associated with a capability.
    ///
    /// This method is the prefered way of sending records that must be accumulated into a container,
    /// as it avoid the recurring overhead of capability validation.
    pub fn session<'b, CT: CapabilityTrait<T>>(&'b mut self, capability: &'b CT) -> Session<'a, 'b, T, CapacityContainerBuilder<C>, CT> where 'a: 'b {
        debug_assert!(self.session.valid(capability));
        Session {
            buffer: self,
            capability,
        }
    }
}

/// An active output building session, which accepts items and builds containers.
pub struct Session<'a: 'b, 'b, T: Timestamp, CB: ContainerBuilder, CT: CapabilityTrait<T>> {
    buffer: &'b mut OutputBuffer<'a, T, CB>,
    capability: &'b CT,
}

impl<'a: 'b, 'b, T: Timestamp, CB: ContainerBuilder, CT: CapabilityTrait<T>> Session<'a, 'b, T, CB, CT> {

    /// Provides one record at the time specified by the `Session`.
    #[inline]
    pub fn give<D>(&mut self, data: D) where CB: PushInto<D> {
        self.buffer.builder.push_into(data);
        self.extract_and_send();
    }

    /// Provides an iterator of records at the time specified by the `Session`.
    #[inline]
    pub fn give_iterator<I>(&mut self, iter: I)
    where
        I: Iterator,
        CB: PushInto<I::Item>,
    {
        for item in iter { self.buffer.builder.push_into(item); }
        self.extract_and_send();
    }
    /// Provide a container at the time specified by the [Session].
    pub fn give_container(&mut self, container: &mut CB::Container) {
        self.buffer.session.give(&self.capability, container);
    }
    /// Provide multiple containers at the time specifid by the [Session].
    pub fn give_containers<'c>(&mut self, containers: impl Iterator<Item = &'c mut CB::Container>) {
        for container in containers { self.buffer.session.give(&self.capability, container); }
    }

    /// Extracts built containers and sends them.
    pub fn extract_and_send(&mut self) {
        while let Some(container) = self.buffer.builder.extract() {
            self.buffer.session.give(&self.capability, container);
        }
    }
    /// Finalizes containers and sends them.
    pub fn flush(&mut self) {
        while let Some(container) = self.buffer.builder.finish() {
            self.buffer.session.give(&self.capability, container);
        }
    }
}

impl<'a: 'b, 'b, T: Timestamp, CB: ContainerBuilder, CT: CapabilityTrait<T>> Drop for Session<'a, 'b, T, CB, CT> {
    fn drop(&mut self) { self.flush() }
}
