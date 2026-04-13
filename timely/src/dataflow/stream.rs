//! A handle to a typed stream of timely data.
//!
//! Most high-level timely dataflow programming is done with streams, which are each a handle to an
//! operator output. Extension methods on the `Stream` type provide the appearance of higher-level
//! declarative programming, while constructing a dataflow graph underneath.

use crate::progress::{Source, Target, Timestamp};

use crate::communication::Push;
use crate::dataflow::Scope;
use crate::dataflow::channels::pushers::tee::TeeHelper;
use crate::dataflow::channels::Message;
use std::fmt::{self, Debug};

/// Abstraction of a stream of `C: Container` records timestamped with `T`.
///
/// Internally `Stream` maintains a list of data recipients who should be presented with data
/// produced by the source of the stream.
pub struct Stream<'scope, T: Timestamp, C> {
    /// The progress identifier of the stream's data source.
    name: Source,
    /// The `Scope` containing the stream.
    scope: Scope<'scope, T>,
    /// Maintains a list of Push<Message<T, C>> interested in the stream's output.
    ports: TeeHelper<T, C>,
}

impl<'scope, T: Timestamp, C: Clone+'static> Clone for Stream<'scope, T, C> {
    fn clone(&self) -> Self {
        Self {
            name: self.name,
            scope: self.scope.clone(),
            ports: self.ports.clone(),
        }
    }

    fn clone_from(&mut self, source: &Self) {
        self.name.clone_from(&source.name);
        self.scope.clone_from(&source.scope);
        self.ports.clone_from(&source.ports);
    }
}

/// A stream batching data in owning vectors.
pub type StreamVec<'scope, T, D> = Stream<'scope, T, Vec<D>>;

impl<'scope, T: Timestamp, C> Stream<'scope, T, C> {
    /// Connects the stream to a destination.
    ///
    /// The destination is described both by a `Target`, for progress tracking information, and a `P: Push` where the
    /// records should actually be sent. The identifier is unique to the edge and is used only for logging purposes.
    pub fn connect_to<P: Push<Message<T, C>>+'static>(self, target: Target, pusher: P, identifier: usize) where C: 'static {

        let mut logging: Option<crate::logging::TimelyLogger> = self.scope().worker().logging();
        logging.as_mut().map(|l| l.log(crate::logging::ChannelsEvent {
            id: identifier,
            scope_addr: self.scope.addr().to_vec(),
            source: (self.name.node, self.name.port),
            target: (target.node, target.port),
            typ: std::any::type_name::<C>().to_string(),
        }));

        self.scope.add_edge(self.name, target);
        self.ports.add_pusher(pusher);
    }
    /// Allocates a `Stream` from a supplied `Source` name and rendezvous point.
    pub fn new(source: Source, output: TeeHelper<T, C>, scope: Scope<'scope, T>) -> Self {
        Self { name: source, ports: output, scope }
    }
    /// The name of the stream's source operator.
    pub fn name(&self) -> &Source { &self.name }
    /// The scope immediately containing the stream.
    pub fn scope(&self) -> Scope<'scope, T> { self.scope.clone() }

    /// Allows the assertion of a container type, for the benefit of type inference.
    ///
    /// This method can be needed when the container type of a stream is unconstrained,
    /// most commonly after creating an input, or bracking wholly generic operators.
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
    pub fn container<C2>(self) -> Stream<'scope, T, C2> where Self: AsStream<'scope, T, C2> { self.as_stream() }
}

/// A type that can be translated to a [Stream].
pub trait AsStream<'scope, T: Timestamp, C> {
    /// Translate `self` to a [Stream].
    fn as_stream(self) -> Stream<'scope, T, C>;
}

impl<'scope, T: Timestamp, C> AsStream<'scope, T, C> for Stream<'scope, T, C> {
    fn as_stream(self) -> Self { self }
}

impl<'scope, T: Timestamp, C> Debug for Stream<'scope, T, C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Stream")
            .field("source", &self.name)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use crate::dataflow::channels::pact::Pipeline;
    use crate::dataflow::operators::{Operator, ToStream};

    #[derive(Debug, Eq, PartialEq)]
    struct NotClone;

    #[test]
    fn test_non_clone_stream() {
        crate::example(|scope| {
            let _ = [NotClone]
                .to_stream(scope)
                .container::<Vec<_>>()
                .sink(Pipeline, "check non-clone", |(input, _frontier)| {
                    input.for_each(|_cap, data| {
                        for datum in data.drain(..) {
                            assert_eq!(datum, NotClone);
                        }
                    });
                });
        });
    }
}
