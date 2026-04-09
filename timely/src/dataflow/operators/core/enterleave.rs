//! Extension traits to move a `Stream` between an outer `Scope` and inner `Scope`.
//!
//! Each `Stream` indicates its containing `Scope` as part of its type signature. To create a new
//! stream with the same contents in another scope, one must explicit use the methods `enter` and
//! `leave`, to clearly indicate the transition to the timely dataflow progress tracking logic.
//!
//! # Examples
//! ```
//! use timely::dataflow::scopes::Scope;
//! use timely::dataflow::operators::{Enter, Leave, ToStream, Inspect};
//!
//! timely::example(|outer| {
//!     let stream = (0..9).to_stream(outer).container::<Vec<_>>();
//!     let output = outer.region(|inner| {
//!         stream.enter(inner)
//!               .inspect(|x| println!("in nested scope: {:?}", x))
//!               .leave(outer)
//!     });
//! });
//! ```

use std::marker::PhantomData;
use std::rc::Rc;

use crate::logging::{TimelyLogger, MessagesEvent};
use crate::progress::Timestamp;
use crate::progress::timestamp::Refines;
use crate::progress::{Source, Target};
use crate::{Accountable, Container};
use crate::communication::Push;
use crate::dataflow::channels::pushers::{Counter, Tee};
use crate::dataflow::channels::Message;
use crate::worker::AsWorker;
use crate::dataflow::{Stream, Scope};

/// Extension trait to move a `Stream` into a child of its current `Scope`.
pub trait Enter<TOuter: Timestamp, TInner: Timestamp+Refines<TOuter>, C> {
    /// Moves the `Stream` argument into a child of its current `Scope`.
    ///
    /// The destination scope must be a child of the stream's scope.
    /// The method checks this property at runtime, and will panic if not respected.
    ///
    /// # Examples
    /// ```
    /// use timely::dataflow::operators::{Enter, Leave, ToStream};
    ///
    /// timely::example(|outer| {
    ///     let stream = (0..9).to_stream(outer).container::<Vec<_>>();
    ///     let output = outer.region(|inner| {
    ///         stream.enter(inner).leave(outer)
    ///     });
    /// });
    /// ```
    fn enter(self, inner: &Scope<TInner>) -> Stream<TInner, C>;
}

impl<TOuter, TInner, C> Enter<TOuter, TInner, C> for Stream<TOuter, C>
where
    TOuter: Timestamp,
    TInner: Timestamp + Refines<TOuter>,
    C: Container,
{
    fn enter(self, inner: &Scope<TInner>) -> Stream<TInner, C> {

        use crate::scheduling::Scheduler;

        // Validate that `inner` is a child of `self`'s scope.
        let inner_addr = inner.addr();
        let outer_addr = self.scope().addr();
        assert!(
            inner_addr.len() == outer_addr.len() + 1
                && inner_addr[..outer_addr.len()] == outer_addr[..],
            "Enter::enter: `inner` is not a child of the stream's scope \
            (inner addr: {:?}, outer addr: {:?})",
            inner_addr,
            outer_addr,
        );

        let (targets, registrar) = Tee::<TInner, C>::new();
        let ingress = IngressNub {
            targets: Counter::new(targets),
            phantom: PhantomData,
            activator: inner.activator_for(inner_addr),
            active: false,
        };
        let produced = Rc::clone(ingress.targets.produced());
        let input = inner.subgraph.borrow_mut().new_input(produced);
        let channel_id = inner.clone().new_identifier();

        if let Some(logger) = inner.logging() {
            let pusher = LogPusher::new(ingress, channel_id, inner.index(), logger);
            self.connect_to(input, pusher, channel_id);
        } else {
            self.connect_to(input, ingress, channel_id);
        }

        Stream::new(
            Source::new(0, input.port),
            registrar,
            inner.clone(),
        )
    }
}

/// Extension trait to move a `Stream` to the parent of its current `Scope`.
pub trait Leave<TOuter: Timestamp, C> {
    /// Moves a `Stream` to the parent of its current `Scope`.
    ///
    /// The parent scope must be supplied as an argument.
    ///
    /// The destination scope must be the parent of the stream's scope.
    /// The method checks this property at runtime, and will panic if not respected.
    ///
    /// # Examples
    /// ```
    /// use timely::dataflow::operators::{Enter, Leave, ToStream};
    ///
    /// timely::example(|outer| {
    ///     let stream = (0..9).to_stream(outer).container::<Vec<_>>();
    ///     let output = outer.region(|inner| {
    ///         stream.enter(inner).leave(outer)
    ///     });
    /// });
    /// ```
    fn leave(self, outer: &Scope<TOuter>) -> Stream<TOuter, C>;
}

impl<TOuter, TInner, C> Leave<TOuter, C> for Stream<TInner, C>
where
    TOuter: Timestamp,
    TInner: Timestamp + Refines<TOuter>,
    C: Container,
{
    fn leave(self, outer: &Scope<TOuter>) -> Stream<TOuter, C> {

        let scope = self.scope();

        // Validate that `self`'s scope is a child of `outer`.
        let inner_addr = scope.addr();
        let outer_addr = outer.addr();
        assert!(
            inner_addr.len() == outer_addr.len() + 1
                && inner_addr[..outer_addr.len()] == outer_addr[..],
            "Leave::leave: `outer` is not the parent of the stream's scope \
            (stream addr: {:?}, outer addr: {:?})",
            inner_addr,
            outer_addr,
        );

        let output = scope.subgraph.borrow_mut().new_output();
        let target = Target::new(0, output.port);
        let (targets, registrar) = Tee::<TOuter, C>::new();
        let egress = EgressNub { targets, phantom: PhantomData };
        let channel_id = scope.clone().new_identifier();

        if let Some(logger) = scope.logging() {
            let pusher = LogPusher::new(egress, channel_id, scope.index(), logger);
            self.connect_to(target, pusher, channel_id);
        } else {
            self.connect_to(target, egress, channel_id);
        }

        Stream::new(
            output,
            registrar,
            outer.clone(),
        )
    }
}


struct IngressNub<TOuter: Timestamp, TInner: Timestamp+Refines<TOuter>, TContainer: Container> {
    targets: Counter<TInner, Tee<TInner, TContainer>>,
    phantom: PhantomData<TOuter>,
    activator: crate::scheduling::Activator,
    active: bool,
}

impl<TOuter: Timestamp, TInner: Timestamp+Refines<TOuter>, TContainer: Container> Push<Message<TOuter, TContainer>> for IngressNub<TOuter, TInner, TContainer> {
    fn push(&mut self, element: &mut Option<Message<TOuter, TContainer>>) {
        if let Some(outer_message) = element {
            let data = ::std::mem::take(&mut outer_message.data);
            let mut inner_message = Some(Message::new(TInner::to_inner(outer_message.time.clone()), data));
            self.targets.push(&mut inner_message);
            if let Some(inner_message) = inner_message {
                outer_message.data = inner_message.data;
            }
            self.active = true;
        }
        else {
            if self.active {
                self.activator.activate();
                self.active = false;
            }
            self.targets.done();
        }
    }
}


struct EgressNub<TOuter: Timestamp, TInner: Timestamp+Refines<TOuter>, TContainer> {
    targets: Tee<TOuter, TContainer>,
    phantom: PhantomData<TInner>,
}

impl<TOuter, TInner, TContainer: Container> Push<Message<TInner, TContainer>> for EgressNub<TOuter, TInner, TContainer>
where TOuter: Timestamp, TInner: Timestamp+Refines<TOuter>, {
    fn push(&mut self, message: &mut Option<Message<TInner, TContainer>>) {
        if let Some(inner_message) = message {
            let data = ::std::mem::take(&mut inner_message.data);
            let mut outer_message = Some(Message::new(inner_message.time.clone().to_outer(), data));
            self.targets.push(&mut outer_message);
            if let Some(outer_message) = outer_message {
                inner_message.data = outer_message.data;
            }
        }
        else { self.targets.done(); }
    }
}

/// A pusher that logs messages passing through it.
///
/// This type performs the same function as the `LogPusher` and `LogPuller` types in
/// `timely::dataflow::channels::pact`. We need a special implementation for `enter`/`leave`
/// channels because those don't have a puller connected. Thus, this pusher needs to log both the
/// send and the receive `MessageEvent`.
struct LogPusher<P> {
    pusher: P,
    channel: usize,
    counter: usize,
    index: usize,
    logger: TimelyLogger,
}

impl<P> LogPusher<P> {
    fn new(pusher: P, channel: usize, index: usize, logger: TimelyLogger) -> Self {
        Self {
            pusher,
            channel,
            counter: 0,
            index,
            logger,
        }
    }
}

impl<T, C, P> Push<Message<T, C>> for LogPusher<P>
where
    C: Accountable,
    P: Push<Message<T, C>>,
{
    fn push(&mut self, element: &mut Option<Message<T, C>>) {
        if let Some(bundle) = element {
            let send_event = MessagesEvent {
                is_send: true,
                channel: self.channel,
                source: self.index,
                target: self.index,
                seq_no: self.counter,
                record_count: bundle.data.record_count(),
            };
            let recv_event = MessagesEvent {
                is_send: false,
                ..send_event
            };

            self.logger.log(send_event);
            self.logger.log(recv_event);
            self.counter += 1;
        }

        self.pusher.push(element);
    }
}

#[cfg(test)]
mod test {
    /// Test that nested scopes with pass-through edges (no operators) correctly communicate progress.
    ///
    /// This is for issue: https://github.com/TimelyDataflow/timely-dataflow/issues/377
    #[test]
    fn test_nested() {

        use crate::dataflow::{InputHandle, ProbeHandle};
        use crate::dataflow::operators::{vec::Input, Inspect, Probe};

        use crate::dataflow::operators::{Enter, Leave};

        // initializes and runs a timely dataflow.
        crate::execute(crate::Config::process(3), |worker| {

            let index = worker.index();
            let mut input = InputHandle::new();
            let probe = ProbeHandle::new();

            // create a new input, exchange data, and inspect its output
            worker.dataflow(|scope| {
                let data = scope.input_from(&mut input);

                scope.region(|inner| {

                    let data = data.enter(inner);
                    inner.region(|inner2| data.enter(inner2).leave(inner)).leave(scope)
                })
                    .inspect(move |x| println!("worker {}:\thello {}", index, x))
                    .probe_with(&probe);
            });

            // introduce data and watch!
            input.advance_to(0);
            for round in 0..10 {
                if index == 0 {
                    input.send(round);
                }
                input.advance_to(round + 1);
                while probe.less_than(input.time()) {
                    worker.step_or_park(None);
                }
            }
        }).unwrap();
    }

}
