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
//!     let output = outer.scoped::<u32,_,_>(|inner| {
//!         stream.enter(inner)
//!               .inspect(|x| println!("in nested scope: {:?}", x))
//!               .leave()
//!     });
//! });
//! ```

use std::default::Default;

use std::marker::PhantomData;

use progress::Timestamp;
use progress::nested::subgraph::{Source, Target};
use progress::nested::product::Product;
use Data;
use communication::Push;
use dataflow::channels::pushers::{Counter, Tee};
use dataflow::channels::{Bundle, Message};

use worker::AsWorker;
use dataflow::{Stream, Scope};
use dataflow::scopes::Child;
use dataflow::operators::delay::Delay;

/// Extension trait to move a `Stream` into a child of its current `Scope`.
pub trait Enter<G: Scope, T: Timestamp, D: Data> {
    /// Moves the `Stream` argument into a child of its current `Scope`.
    ///
    /// # Examples
    /// ```
    /// use timely::dataflow::scopes::Scope;
    /// use timely::dataflow::operators::{Enter, Leave, ToStream};
    ///
    /// timely::example(|outer| {
    ///     let stream = (0..9).to_stream(outer);
    ///     let output = outer.scoped::<u32,_,_>(|inner| {
    ///         stream.enter(inner).leave()
    ///     });
    /// });
    /// ```
    fn enter<'a>(&self, &Child<'a, G, T>) -> Stream<Child<'a, G, T>, D>;
}

/// Extension trait to move a `Stream` into a child of its current `Scope` setting the timestamp for each element.
pub trait EnterAt<G: Scope, T: Timestamp, D: Data> {
    /// Moves the `Stream` argument into a child of its current `Scope` setting the timestamp for each element by `initial`.
    ///
    /// # Examples
    /// ```
    /// use timely::dataflow::scopes::Scope;
    /// use timely::dataflow::operators::{EnterAt, Leave, ToStream};
    ///
    /// timely::example(|outer| {
    ///     let stream = (0..9).to_stream(outer);
    ///     let output = outer.scoped::<u32,_,_>(|inner| {
    ///         stream.enter_at(inner, |x| *x).leave()
    ///     });
    /// });
    /// ```
    fn enter_at<'a, F:Fn(&D)->T+'static>(&self, scope: &Child<'a, G, T>, initial: F) -> Stream<Child<'a, G, T>, D> ;
}

impl<G: Scope, T: Timestamp, D: Data, E: Enter<G, T, D>> EnterAt<G, T, D> for E {
    fn enter_at<'a, F:Fn(&D)->T+'static>(&self, scope: &Child<'a, G, T>, initial: F) ->
        Stream<Child<'a, G, T>, D> {
            self.enter(scope).delay(move |datum, time| Product::new(time.outer.clone(), initial(datum)))
    }
}

impl<T: Timestamp, G: Scope, D: Data> Enter<G, T, D> for Stream<G, D> {
    fn enter<'a>(&self, scope: &Child<'a, G, T>) -> Stream<Child<'a, G, T>, D> {

        let (targets, registrar) = Tee::<Product<G::Timestamp, T>, D>::new();
        let ingress = IngressNub { targets: Counter::new(targets) };
        let produced = ingress.targets.produced().clone();

        let input = scope.subgraph.borrow_mut().new_input(produced);

        let channel_id = scope.clone().new_identifier();
        self.connect_to(input, ingress, channel_id);
        Stream::new(Source { index: 0, port: input.port }, registrar, scope.clone())
    }
}

/// Extension trait to move a `Stream` to the parent of its current `Scope`.
pub trait Leave<G: Scope, D: Data> {
    /// Moves a `Stream` to the parent of its current `Scope`.
    ///
    /// # Examples
    /// ```
    /// use timely::dataflow::scopes::Scope;
    /// use timely::dataflow::operators::{Enter, Leave, ToStream};
    ///
    /// timely::example(|outer| {
    ///     let stream = (0..9).to_stream(outer);
    ///     let output = outer.scoped::<u32,_,_>(|inner| {
    ///         stream.enter(inner).leave()
    ///     });
    /// });
    /// ```
    fn leave(&self) -> Stream<G, D>;
}

impl<'a, G: Scope, D: Data, T: Timestamp> Leave<G, D> for Stream<Child<'a, G, T>, D> {
    fn leave(&self) -> Stream<G, D> {

        let scope = self.scope();

        let output = scope.subgraph.borrow_mut().new_output();
        let (targets, registrar) = Tee::<G::Timestamp, D>::new();
        let channel_id = scope.clone().new_identifier();
        self.connect_to(Target { index: 0, port: output.port }, EgressNub { targets, phantom: PhantomData }, channel_id);

        Stream::new(
            output,
            registrar,
            scope.parent.clone()
        )
    }
}


struct IngressNub<TOuter: Timestamp, TInner: Timestamp, TData: Data> {
    targets: Counter<Product<TOuter, TInner>, TData, Tee<Product<TOuter, TInner>, TData>>,
}

impl<TOuter: Timestamp, TInner: Timestamp, TData: Data> Push<Bundle<TOuter, TData>> for IngressNub<TOuter, TInner, TData> {
    fn push(&mut self, message: &mut Option<Bundle<TOuter, TData>>) {
        if let Some(message) = message {
            let outer_message = message.as_mut();
            let data = ::std::mem::replace(&mut outer_message.data, Vec::new());
            let mut inner_message = Some(Bundle::from_typed(Message::new(Product::new(outer_message.time.clone(), Default::default()), data, 0, 0)));
            self.targets.push(&mut inner_message);
            if let Some(inner_message) = inner_message {
                if let Some(inner_message) = inner_message.if_typed() {
                    outer_message.data = inner_message.data;
                }
            }
        }
        else { self.targets.done(); }
    }
}


struct EgressNub<TOuter: Timestamp, TInner: Timestamp, TData: Data> {
    targets: Tee<TOuter, TData>,
    phantom: PhantomData<TInner>,
}

impl<TOuter, TInner, TData> Push<Bundle<Product<TOuter, TInner>, TData>> for EgressNub<TOuter, TInner, TData>
where TOuter: Timestamp, TInner: Timestamp, TData: Data {
    fn push(&mut self, message: &mut Option<Bundle<Product<TOuter, TInner>, TData>>) {
        if let Some(message) = message {
            let inner_message = message.as_mut();
            let data = ::std::mem::replace(&mut inner_message.data, Vec::new());
            let mut outer_message = Some(Bundle::from_typed(Message::new(inner_message.time.outer.clone(), data, 0, 0)));
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
