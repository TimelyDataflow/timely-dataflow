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
//!     let stream = (0..9).to_stream(outer);
//!     let output = outer.region(|inner| {
//!         stream.enter(inner)
//!               .inspect(|x| println!("in nested scope: {:?}", x))
//!               .leave()
//!     });
//! });
//! ```

use std::marker::PhantomData;

use crate::logging::{TimelyLogger, MessagesEvent};
use crate::progress::Timestamp;
use crate::progress::timestamp::Refines;
use crate::progress::{Source, Target};
use crate::{Container, Data};
use crate::communication::Push;
use crate::dataflow::channels::pushers::{Counter, Tee};
use crate::dataflow::channels::{Bundle, Message};

use crate::worker::AsWorker;
use crate::dataflow::{StreamCore, Scope};
use crate::dataflow::scopes::Child;

/// Extension trait to move a `Stream` into a child of its current `Scope`.
pub trait Enter<G: Scope, T: Timestamp+Refines<G::Timestamp>, C: Container> {
    /// Moves the `Stream` argument into a child of its current `Scope`.
    ///
    /// # Examples
    /// ```
    /// use timely::dataflow::scopes::Scope;
    /// use timely::dataflow::operators::{Enter, Leave, ToStream};
    ///
    /// timely::example(|outer| {
    ///     let stream = (0..9).to_stream(outer);
    ///     let output = outer.region(|inner| {
    ///         stream.enter(inner).leave()
    ///     });
    /// });
    /// ```
    fn enter<'a>(&self, _: &Child<'a, G, T>) -> StreamCore<Child<'a, G, T>, C>;
}

impl<G: Scope, T: Timestamp+Refines<G::Timestamp>, C: Data+Container> Enter<G, T, C> for StreamCore<G, C> {
    fn enter<'a>(&self, scope: &Child<'a, G, T>) -> StreamCore<Child<'a, G, T>, C> {

        use crate::scheduling::Scheduler;

        let (targets, registrar) = Tee::<T, C>::new();
        let ingress = IngressNub {
            targets: Counter::new(targets),
            phantom: PhantomData,
            activator: scope.activator_for(&scope.addr()),
            active: false,
        };
        let produced = ingress.targets.produced().clone();
        let input = scope.subgraph.borrow_mut().new_input(produced);
        let channel_id = scope.clone().new_identifier();

        if let Some(logger) = scope.logging() {
            let pusher = LogPusher::new(ingress, channel_id, scope.index(), logger);
            self.connect_to(input, pusher, channel_id);
        } else {
            self.connect_to(input, ingress, channel_id);
        }

        StreamCore::new(
            Source::new(0, input.port),
            registrar,
            scope.clone(),
        )
    }
}

/// Extension trait to move a `Stream` to the parent of its current `Scope`.
pub trait Leave<G: Scope, C: Container> {
    /// Moves a `Stream` to the parent of its current `Scope`.
    ///
    /// # Examples
    /// ```
    /// use timely::dataflow::scopes::Scope;
    /// use timely::dataflow::operators::{Enter, Leave, ToStream};
    ///
    /// timely::example(|outer| {
    ///     let stream = (0..9).to_stream(outer);
    ///     let output = outer.region(|inner| {
    ///         stream.enter(inner).leave()
    ///     });
    /// });
    /// ```
    fn leave(&self) -> StreamCore<G, C>;
}

impl<'a, G: Scope, C: Clone+Container, T: Timestamp+Refines<G::Timestamp>> Leave<G, C> for StreamCore<Child<'a, G, T>, C> {
    fn leave(&self) -> StreamCore<G, C> {

        let scope = self.scope();

        let output = scope.subgraph.borrow_mut().new_output();
        let target = Target::new(0, output.port);
        let (targets, registrar) = Tee::<G::Timestamp, C>::new();
        let egress = EgressNub { targets, phantom: PhantomData };
        let channel_id = scope.clone().new_identifier();

        if let Some(logger) = scope.logging() {
            let pusher = LogPusher::new(egress, channel_id, scope.index(), logger);
            self.connect_to(target, pusher, channel_id);
        } else {
            self.connect_to(target, egress, channel_id);
        }

        StreamCore::new(
            output,
            registrar,
            scope.parent,
        )
    }
}


struct IngressNub<TOuter: Timestamp, TInner: Timestamp+Refines<TOuter>, TContainer: Container> {
    targets: Counter<TInner, TContainer, Tee<TInner, TContainer>>,
    phantom: ::std::marker::PhantomData<TOuter>,
    activator: crate::scheduling::Activator,
    active: bool,
}

impl<TOuter: Timestamp, TInner: Timestamp+Refines<TOuter>, TContainer: Container> Push<Bundle<TOuter, TContainer>> for IngressNub<TOuter, TInner, TContainer> {
    fn push(&mut self, element: &mut Option<Bundle<TOuter, TContainer>>) {
        if let Some(message) = element {
            let outer_message = message.as_mut();
            let data = ::std::mem::take(&mut outer_message.data);
            let mut inner_message = Some(Bundle::from_typed(Message::new(TInner::to_inner(outer_message.time.clone()), data, 0, 0)));
            self.targets.push(&mut inner_message);
            if let Some(inner_message) = inner_message {
                if let Some(inner_message) = inner_message.if_typed() {
                    outer_message.data = inner_message.data;
                }
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


struct EgressNub<TOuter: Timestamp, TInner: Timestamp+Refines<TOuter>, TContainer: Data> {
    targets: Tee<TOuter, TContainer>,
    phantom: PhantomData<TInner>,
}

impl<TOuter, TInner, TContainer: Container> Push<Bundle<TInner, TContainer>> for EgressNub<TOuter, TInner, TContainer>
where TOuter: Timestamp, TInner: Timestamp+Refines<TOuter>, TContainer: Data {
    fn push(&mut self, message: &mut Option<Bundle<TInner, TContainer>>) {
        if let Some(message) = message {
            let inner_message = message.as_mut();
            let data = ::std::mem::take(&mut inner_message.data);
            let mut outer_message = Some(Bundle::from_typed(Message::new(inner_message.time.clone().to_outer(), data, 0, 0)));
            self.targets.push(&mut outer_message);
            if let Some(outer_message) = outer_message {
                if let Some(outer_message) = outer_message.if_typed() {
                    inner_message.data = outer_message.data;
                }
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

impl<T, C, P> Push<Bundle<T, C>> for LogPusher<P>
where
    C: Container,
    P: Push<Bundle<T, C>>,
{
    fn push(&mut self, element: &mut Option<Bundle<T, C>>) {
        if let Some(bundle) = element {
            let send_event = MessagesEvent {
                is_send: true,
                channel: self.channel,
                source: self.index,
                target: self.index,
                seq_no: self.counter,
                length: bundle.data.len(),
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
        use crate::dataflow::operators::{Input, Inspect, Probe};

        use crate::dataflow::Scope;
        use crate::dataflow::operators::{Enter, Leave};

        // initializes and runs a timely dataflow.
        crate::execute(crate::Config::process(3), |worker| {

            let index = worker.index();
            let mut input = InputHandle::new();
            let mut probe = ProbeHandle::new();

            // create a new input, exchange data, and inspect its output
            worker.dataflow(|scope| {
                let data = scope.input_from(&mut input);

                scope.region(|inner| {

                    let data = data.enter(inner);
                    inner.region(|inner2| data.enter(inner2).leave()).leave()
                })
                    .inspect(move |x| println!("worker {}:\thello {}", index, x))
                    .probe_with(&mut probe);
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
