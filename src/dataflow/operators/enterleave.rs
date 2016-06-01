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

use std::hash::Hash;
use std::default::Default;

use std::rc::Rc;
use std::cell::RefCell;
use std::marker::PhantomData;

use progress::{Timestamp, CountMap};
use progress::nested::subgraph::{Source, Target};
use progress::nested::product::Product;
use {Data, Push};
use dataflow::channels::pushers::{Counter, Tee};
use dataflow::channels::Content;

use dataflow::{Stream, Scope};
use dataflow::scopes::Child;
use dataflow::operators::delay::*;

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
pub trait EnterAt<G: Scope, T: Timestamp, D: Data>  where  G::Timestamp: Hash, T: Hash {
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

impl<G: Scope, T: Timestamp, D: Data, E: Enter<G, T, D>> EnterAt<G, T, D> for E
where G::Timestamp: Hash, T: Hash {
    fn enter_at<'a, F:Fn(&D)->T+'static>(&self, scope: &Child<'a, G, T>, initial: F) ->
        Stream<Child<'a, G, T>, D> {
            self.enter(scope).delay(move |datum, time| Product::new(time.outer, initial(datum)))
    }
}

impl<T: Timestamp, G: Scope, D: Data> Enter<G, T, D> for Stream<G, D> {
    fn enter<'a>(&self, scope: &Child<'a, G, T>) -> Stream<Child<'a, G, T>, D> {

        let (targets, registrar) = Tee::<Product<G::Timestamp, T>, D>::new();
        let produced = Rc::new(RefCell::new(CountMap::new()));
        let ingress = IngressNub { targets: Counter::new(targets, produced.clone()) };

        let scope_index = scope.subgraph.borrow().index;
        let input_index = scope.subgraph.borrow_mut().new_input(produced);

        let channel_id = scope.clone().new_identifier();
        self.connect_to(Target { index: scope_index, port: input_index }, ingress, channel_id);
        Stream::new(Source { index: 0, port: input_index }, registrar, scope.clone())
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

        let output_index = scope.subgraph.borrow_mut().new_output();
        let (targets, registrar) = Tee::<G::Timestamp, D>::new();
        let channel_id = scope.clone().new_identifier();
        self.connect_to(Target { index: 0, port: output_index }, EgressNub { targets: targets, phantom: PhantomData }, channel_id);
        let subgraph_index = scope.subgraph.borrow().index;
        
        Stream::new(
            Source { index: subgraph_index, port: output_index },
            registrar,
            scope.parent.clone()
        )
    }
}


struct IngressNub<TOuter: Timestamp, TInner: Timestamp, TData: Data> {
    targets: Counter<Product<TOuter, TInner>, TData, Tee<Product<TOuter, TInner>, TData>>,
}

impl<TOuter: Timestamp, TInner: Timestamp, TData: Data> Push<(TOuter, Content<TData>)> for IngressNub<TOuter, TInner, TData> {
    fn push(&mut self, message: &mut Option<(TOuter, Content<TData>)>) {
        if let Some((ref time, ref mut data)) = *message {
            let content = ::std::mem::replace(data, Content::Typed(Vec::new()));
            let mut message = Some((Product::new(*time, Default::default()), content));
            self.targets.push(&mut message);
            if let Some((_, content)) = message {
                *data = content;
            }
        }
        else { self.targets.done(); }
    }
}


struct EgressNub<TOuter: Timestamp, TInner: Timestamp, TData: Data> {
    targets: Tee<TOuter, TData>,
    phantom: PhantomData<TInner>,
}

impl<TOuter, TInner, TData> Push<(Product<TOuter, TInner>, Content<TData>)> for EgressNub<TOuter, TInner, TData>
where TOuter: Timestamp, TInner: Timestamp, TData: Data {
    fn push(&mut self, message: &mut Option<(Product<TOuter, TInner>, Content<TData>)>) {
        if let Some((ref time, ref mut data)) = *message {
            let content = ::std::mem::replace(data, Content::Typed(Vec::new()));
            let mut message = Some((time.outer, content));
            self.targets.push(&mut message);
            if let Some((_, content)) = message {
                *data = content;
            }
        }
        else { self.targets.done(); }
    }
}
