//! Extension trait and implementation for observing and action on streamed data.

use crate::Container;
use crate::progress::{Stamp, Timestamp};
use crate::dataflow::channels::pact::Pipeline;
use crate::dataflow::Stream;
use crate::dataflow::operators::generic::Operator;

/// Methods to inspect records and container and frontier events on a stream.
pub trait Inspect<T: Timestamp, C>: Sized {
    /// Runs a supplied closure on each observed container, and each frontier advancement.
    ///
    /// Rust's `Result` type is used to distinguish the events, with `Ok` for a container and
    /// the stamp under which it travels, and `Err` for frontiers. Frontiers are only presented
    /// when they change. The stamp is the set of timestamps under which the container travels:
    /// a singleton unless an upstream operator has stamped its messages with several times, or
    /// with none.
    ///
    /// # Examples
    /// ```
    /// use timely::dataflow::operators::{ToStream, Inspect};
    ///
    /// timely::example(|scope| {
    ///     (0..10).to_stream(scope)
    ///            .container::<Vec<_>>()
    ///            .inspect_core(|event| {
    ///                match event {
    ///                    Ok((stamp, data)) => println!("seen under {:?}: {:?} records", stamp, data.len()),
    ///                    Err(frontier) => println!("frontier advanced to {:?}", frontier),
    ///                }
    ///             });
    /// });
    /// ```
    fn inspect_core<F>(self, func: F) -> Self where F: FnMut(Result<(&Stamp<T>, &C), &[T]>)+'static;

    /// Runs a supplied closure on each observed record.
    ///
    /// # Examples
    /// ```
    /// use timely::dataflow::operators::{ToStream, Inspect};
    ///
    /// timely::example(|scope| {
    ///     (0..10).to_stream(scope)
    ///            .container::<Vec<_>>()
    ///            .inspect(|x| println!("seen: {:?}", x));
    /// });
    /// ```
    fn inspect<F>(self, mut func: F) -> Self
    where
        for<'a> &'a C: IntoIterator,
        F: for<'a> FnMut(<&'a C as IntoIterator>::Item) + 'static,
    {
        self.inspect_core(move |event| {
            if let Ok((_, data)) = event {
                for datum in data.into_iter() { func(datum); }
            }
        })
    }
}

impl<T: Timestamp, C: Container> Inspect<T, C> for Stream<'_, T, C> {
    fn inspect_core<F>(self, mut func: F) -> Self where F: FnMut(Result<(&Stamp<T>, &C), &[T]>)+'static {
        let mut frontier = crate::progress::Antichain::from_elem(T::minimum());
        self.unary_frontier(Pipeline, "Inspect", move |_,_| move |(input, chain), output| {
            if chain.frontier() != frontier.borrow() {
                frontier.clear();
                frontier.extend(chain.frontier().iter().cloned());
                func(Err(frontier.elements()));
            }
            input.for_each_stamp(|cap, data| {
                let mut session = output.session(&cap);
                for data in data {
                    func(Ok((cap.stamp(), &*data)));
                    session.give_container(data);
                }
            });
        })
    }
}
