//! Handles to an operator's input and output streams.
//!
//! These handles are used by the generic operator interfaces to allow user closures to interact as 
//! the operator would with its input and output streams.

use std::rc::Rc;
use std::cell::RefCell;
use progress::Timestamp;
use progress::ChangeBatch;
use progress::frontier::MutableAntichain;
use dataflow::channels::pullers::Counter as PullCounter;
use dataflow::channels::pushers::Counter as PushCounter;
use dataflow::channels::pushers::buffer::{Buffer, Session};
use dataflow::channels::Content;
use timely_communication::{Push, Pull};
use logging::Logger;

use dataflow::operators::Capability;
use dataflow::operators::capability::mint as mint_capability;

/// Handle to an operator's input stream.
pub struct InputHandle<T: Timestamp, D, P: Pull<(T, Content<D>)>> {
    pull_counter: PullCounter<T, D, P>,
    internal: Rc<RefCell<ChangeBatch<T>>>,
    logging: Logger,
}

/// Handle to an operator's input stream and frontier.
pub struct FrontieredInputHandle<'a, T: Timestamp, D: 'a, P: Pull<(T, Content<D>)>+'a> {
    /// The underlying input handle.
    pub handle: &'a mut InputHandle<T, D, P>,
    /// The frontier as reported by timely progress tracking.
    pub frontier: &'a MutableAntichain<T>,
}

impl<'a, T: Timestamp, D, P: Pull<(T, Content<D>)>> InputHandle<T, D, P> {

    /// Reads the next input buffer (at some timestamp `t`) and a corresponding capability for `t`.
    /// The timestamp `t` of the input buffer can be retrieved by invoking `.time()` on the capability.
    /// Returns `None` when there's no more data available.
    #[inline(always)]
    pub fn next(&mut self) -> Option<(Capability<T>, &mut Content<D>)> {
        let internal = &mut self.internal;
        self.pull_counter.next().map(|(time, content)| {
            (mint_capability(time.clone(), internal.clone()), content)
        })
    }

    /// Repeatedly calls `logic` till exhaustion of the available input data.
    /// `logic` receives a capability and an input buffer.
    ///
    /// #Examples
    /// ```
    /// use timely::dataflow::operators::ToStream;
    /// use timely::dataflow::operators::generic::unary::Unary;
    /// use timely::dataflow::channels::pact::Pipeline;
    ///
    /// timely::example(|scope| {
    ///     (0..10).to_stream(scope)
    ///            .unary_stream(Pipeline, "example", |input, output| {
    ///                input.for_each(|cap, data| {
    ///                    output.session(&cap).give_content(data);
    ///                });
    ///            });
    /// });
    /// ```
    #[inline]
    pub fn for_each<F: FnMut(Capability<T>, &mut Content<D>)>(&mut self, mut logic: F) {
        let logging = self.logging.clone();
        while let Some((cap, data)) = self.next() {
            logging.log(::timely_logging::Event::GuardedMessage(
                    ::timely_logging::GuardedMessageEvent { is_start: true }));
            logic(cap, data);
            logging.log(::timely_logging::Event::GuardedMessage(
                    ::timely_logging::GuardedMessageEvent { is_start: false }));
        }
    }

}

impl<'a, T: Timestamp, D, P: Pull<(T, Content<D>)>+'a> FrontieredInputHandle<'a, T, D, P> {
    /// Reads the next input buffer (at some timestamp `t`) and a corresponding capability for `t`.
    /// The timestamp `t` of the input buffer can be retrieved by invoking `.time()` on the capability.
    /// Returns `None` when there's no more data available.
    #[inline(always)]
    pub fn next(&mut self) -> Option<(Capability<T>, &mut Content<D>)> {
        self.handle.next()
    }

    /// Repeatedly calls `logic` till exhaustion of the available input data.
    /// `logic` receives a capability and an input buffer.
    ///
    /// #Examples
    /// ```
    /// use timely::dataflow::operators::ToStream;
    /// use timely::dataflow::operators::generic::unary::Unary;
    /// use timely::dataflow::channels::pact::Pipeline;
    ///
    /// timely::example(|scope| {
    ///     (0..10).to_stream(scope)
    ///            .unary_stream(Pipeline, "example", |input, output| {
    ///                input.for_each(|cap, data| {
    ///                    output.session(&cap).give_content(data);
    ///                });
    ///            });
    /// });
    /// ```
    #[inline]
    pub fn for_each<F: FnMut(Capability<T>, &mut Content<D>)>(&mut self, logic: F) {
        self.handle.for_each(logic)
    }

    /// Inspect the frontier associated with this input.
    #[inline(always)]
    pub fn frontier(&self) -> &'a MutableAntichain<T> {
        self.frontier
    }
}

pub fn _access_pull_counter<T: Timestamp, D, P: Pull<(T, Content<D>)>>(input: &mut InputHandle<T, D, P>) -> &mut PullCounter<T, D, P> {
    &mut input.pull_counter
}

/// Constructs an input handle.
/// Declared separately so that it can be kept private when `InputHandle` is re-exported.
pub fn new_input_handle<T: Timestamp, D, P: Pull<(T, Content<D>)>>(pull_counter: PullCounter<T, D, P>, internal: Rc<RefCell<ChangeBatch<T>>>, logging: Logger) -> InputHandle<T, D, P> {
    InputHandle {
        pull_counter: pull_counter,
        internal: internal,
        logging: logging,
    }
}

pub fn new_frontier_input_handle<'a, T: Timestamp, D: 'a, P: Pull<(T, Content<D>)>+'a>(input_handle: &'a mut InputHandle<T, D, P>, frontier: &'a MutableAntichain<T>) -> FrontieredInputHandle<'a, T, D, P> {
    FrontieredInputHandle {
        handle: input_handle,
        frontier: frontier,
    }
}

/// Handle to an operator's output stream.
pub struct OutputHandle<'a, T: Timestamp, D: 'a, P: Push<(T, Content<D>)>+'a> {
    push_buffer: &'a mut Buffer<T, D, PushCounter<T, D, P>>,
}

impl<'a, T: Timestamp, D, P: Push<(T, Content<D>)>> OutputHandle<'a, T, D, P> {
    /// Obtains a session that can send data at the timestamp associated with capability `cap`.
    ///
    /// In order to send data at a future timestamp, obtain a capability for the new timestamp
    /// first, as show in the example.
    ///
    /// #Examples
    /// ```
    /// use timely::dataflow::operators::ToStream;
    /// use timely::dataflow::operators::generic::unary::Unary;
    /// use timely::dataflow::channels::pact::Pipeline;
    ///
    /// timely::example(|scope| {
    ///     (0..10).to_stream(scope)
    ///            .unary_stream(Pipeline, "example", |input, output| {
    ///                input.for_each(|cap, data| {
    ///                    let mut time = cap.time().clone();
    ///                    time.inner += 1;
    ///                    output.session(&cap.delayed(&time)).give_content(data);
    ///                });
    ///            });
    /// });
    /// ```
    pub fn session<'b>(&'b mut self, cap: &'b Capability<T>) -> Session<'b, T, D, PushCounter<T, D, P>> where 'a: 'b {
        self.push_buffer.session(cap)
    }
}

impl<'a, T: Timestamp, D, P: Push<(T, Content<D>)>> Drop for OutputHandle<'a, T, D, P> {
    fn drop(&mut self) {
        self.push_buffer.cease();
    }
}

/// Constructs an output handle.
/// Declared separately so that it can be kept private when `OutputHandle` is re-exported.
pub fn new_output_handle<'a, T: Timestamp, D, P: Push<(T, Content<D>)>>(push_buffer: &'a mut Buffer<T, D, PushCounter<T, D, P>>) -> OutputHandle<'a, T, D, P> {
    OutputHandle {
        push_buffer: push_buffer,
    }
}


