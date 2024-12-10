//! Extension trait and implementation for observing and action on streamed data.

use crate::Container;
use crate::dataflow::channels::pact::Pipeline;
use crate::dataflow::{Scope, OwnedStream, StreamLike};
use crate::dataflow::operators::generic::Operator;

/// Methods to inspect records and batches of records on a stream.
pub trait Inspect<G: Scope, C: Container>: InspectCore<G, C> + Sized {
    /// Runs a supplied closure on each observed data element.
    ///
    /// # Examples
    /// ```
    /// use timely::dataflow::operators::{ToStream, Map, Inspect};
    ///
    /// timely::example(|scope| {
    ///     (0..10).to_stream(scope)
    ///            .inspect(|x| println!("seen: {:?}", x));
    /// });
    /// ```
    fn inspect<F>(self, mut func: F) -> OwnedStream<G, C>
    where
        F: for<'a> FnMut(C::ItemRef<'a>) + 'static,
    {
        self.inspect_batch(move |_, data| {
            for datum in data.iter() { func(datum); }
        })
    }

    /// Runs a supplied closure on each observed data element and associated time.
    ///
    /// # Examples
    /// ```
    /// use timely::dataflow::operators::{ToStream, Map, Inspect};
    ///
    /// timely::example(|scope| {
    ///     (0..10).to_stream(scope)
    ///            .inspect_time(|t, x| println!("seen at: {:?}\t{:?}", t, x));
    /// });
    /// ```
    fn inspect_time<F>(self, mut func: F) -> OwnedStream<G, C>
    where
        F: for<'a> FnMut(&G::Timestamp, C::ItemRef<'a>) + 'static,
    {
        self.inspect_batch(move |time, data| {
            for datum in data.iter() {
                func(time, datum);
            }
        })
    }

    /// Runs a supplied closure on each observed data batch (time and data slice).
    ///
    /// # Examples
    /// ```
    /// use timely::dataflow::operators::{ToStream, Map, Inspect};
    ///
    /// timely::example(|scope| {
    ///     (0..10).to_stream(scope)
    ///            .inspect_batch(|t,xs| println!("seen at: {:?}\t{:?} records", t, xs.len()));
    /// });
    /// ```
    fn inspect_batch(self, mut func: impl FnMut(&G::Timestamp, &C)+'static) -> OwnedStream<G, C> {
        self.inspect_core(move |event| {
            if let Ok((time, data)) = event {
                func(time, data);
            }
        })
    }

    /// Runs a supplied closure on each observed data batch, and each frontier advancement.
    ///
    /// Rust's `Result` type is used to distinguish the events, with `Ok` for time and data,
    /// and `Err` for frontiers. Frontiers are only presented when they change.
    ///
    /// # Examples
    /// ```
    /// use timely::dataflow::operators::{ToStream, Map, Inspect};
    ///
    /// timely::example(|scope| {
    ///     (0..10).to_stream(scope)
    ///            .inspect_core(|event| {
    ///                match event {
    ///                    Ok((time, data)) => println!("seen at: {:?}\t{:?} records", time, data.len()),
    ///                    Err(frontier) => println!("frontier advanced to {:?}", frontier),
    ///                }
    ///             });
    /// });
    /// ```
    fn inspect_core<F>(self, func: F) -> OwnedStream<G, C> where F: FnMut(Result<(&G::Timestamp, &C), &[G::Timestamp]>)+'static;
}

impl<G: Scope, C: Container + 'static, S: StreamLike<G, C>> Inspect<G, C> for S {
    fn inspect_core<F>(self, func: F) -> OwnedStream<G, C> where F: FnMut(Result<(&G::Timestamp, &C), &[G::Timestamp]>) + 'static {
        self.inspect_container(func)
    }
}

/// Inspect containers
pub trait InspectCore<G: Scope, C: Container> {
    /// Runs a supplied closure on each observed container, and each frontier advancement.
    ///
    /// Rust's `Result` type is used to distinguish the events, with `Ok` for time and data,
    /// and `Err` for frontiers. Frontiers are only presented when they change.
    ///
    /// # Examples
    /// ```
    /// use timely::dataflow::operators::{ToStream, Map, InspectCore};
    ///
    /// timely::example(|scope| {
    ///     (0..10).to_stream(scope)
    ///            .inspect_container(|event| {
    ///                match event {
    ///                    Ok((time, data)) => println!("seen at: {:?}\t{:?} records", time, data.len()),
    ///                    Err(frontier) => println!("frontier advanced to {:?}", frontier),
    ///                }
    ///             });
    /// });
    /// ```
    fn inspect_container<F>(self, func: F) -> OwnedStream<G, C> where F: FnMut(Result<(&G::Timestamp, &C), &[G::Timestamp]>)+'static;
}

impl<G: Scope, C: Container + 'static, S: StreamLike<G, C>> InspectCore<G, C> for S {

    fn inspect_container<F>(self, mut func: F) -> OwnedStream<G, C>
        where F: FnMut(Result<(&G::Timestamp, &C), &[G::Timestamp]>)+'static
    {
        use crate::progress::timestamp::Timestamp;
        let mut frontier = crate::progress::Antichain::from_elem(G::Timestamp::minimum());
        self.unary_frontier(Pipeline, "InspectBatch", move |_,_| move |input, output| {
            if input.frontier.frontier() != frontier.borrow() {
                frontier.clear();
                frontier.extend(input.frontier.frontier().iter().cloned());
                func(Err(frontier.elements()));
            }
            input.for_each(|time, data| {
                func(Ok((&time, &*data)));
                output.session(&time).give_container(data);
            });
        })
    }
}
