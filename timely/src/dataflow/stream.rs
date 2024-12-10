//! A handle to a typed stream of timely data.
//!
//! Most high-level timely dataflow programming is done with streams, which are each a handle to an
//! operator output. Extension methods on the `Stream` type provide the appearance of higher-level
//! declarative programming, while constructing a dataflow graph underneath.

use crate::progress::{Source, Target};

use crate::communication::Push;
use crate::dataflow::Scope;
use crate::dataflow::channels::pushers::tee::TeeHelper;
use crate::dataflow::channels::Message;
use std::fmt::{self, Debug};
use crate::Container;
use crate::dataflow::channels::pushers::{PushOwned, Tee};

/// Common behavior for all streams. Streams belong to a scope and carry data.
///
/// The main purpose of this is to allow connecting different stream kinds to a pusher.
pub trait StreamLike<S: Scope, D> {
    /// Connects the stream to a destination.
    ///
    /// The destination is described both by a `Target`, for progress tracking information, and a `P: Push` where the
    /// records should actually be sent. The identifier is unique to the edge and is used only for logging purposes.
    fn connect_to<P: Push<Message<S::Timestamp, D>>+'static>(self, target: Target, pusher: P, identifier: usize);
    /// The scope containing the stream.
    fn scope(&self) -> S;
}

/// Abstraction of a stream of `C: Container` records timestamped with `S::Timestamp`.
///
/// Internally `Stream` maintains a list of data recipients who should be presented with data
/// produced by the source of the stream.
pub struct StreamCore<S: Scope, C> {
    /// The progress identifier of the stream's data source.
    name: Source,
    /// The `Scope` containing the stream.
    scope: S,
    /// Maintains a list of Push<Message<T, C>> interested in the stream's output.
    ports: TeeHelper<S::Timestamp, C>,
}

impl<S: Scope, C> Clone for StreamCore<S, C> {
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
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

/// A stream that owns a single pusher.
pub struct OwnedStream<S: Scope, D> {
    /// The progress identifier of the stream's data source.
    name: Source,
    /// The `Scope` containing the stream.
    scope: S,
    /// The single pusher interested in the stream's output, if any.
    port: PushOwned<S::Timestamp, D>,
}

impl<S: Scope, C: Container + 'static> OwnedStream<S, C> {
    /// Allocates an `OwnedStream` from a supplied `Source` name and rendezvous point within a scope.
    pub fn new(name: Source, port: PushOwned<S::Timestamp, C>, scope: S) -> Self {
        Self { name, port, scope }
    }

    /// Convert the stream into a `StreamCore` that can be cloned. Requires elements to be `Clone`.
    /// Consumes this stream.
    pub fn tee(self) -> StreamCore<S, C> where C: Clone {
        let (target, registrar) = Tee::new();
        self.port.set(target);
        StreamCore::new(self.name, registrar, self.scope)
    }

    /// Allows the assertion of a container type, for the benefit of type inference.
    pub fn container<D: Container>(self) -> OwnedStream<S, C> where Self: AsStream<S, D> { self.as_stream() }
}

/// A stream batching data in vectors.
pub type Stream<S, D> = StreamCore<S, Vec<D>>;

impl<S: Scope, C> StreamLike<S, C> for &StreamCore<S, C> {
    fn connect_to<P: Push<Message<S::Timestamp, C>>+'static>(self, target: Target, pusher: P, identifier: usize) {
        let mut logging = self.scope().logging();
        logging.as_mut().map(|l| l.log(crate::logging::ChannelsEvent {
            id: identifier,
            scope_addr: self.scope.addr().to_vec(),
            source: (self.name.node, self.name.port),
            target: (target.node, target.port),
        }));

        self.scope.add_edge(self.name, target);
        self.ports.add_pusher(pusher);
    }

    fn scope(&self) -> S {
        self.scope.clone()
    }
}

impl<S: Scope, D> StreamLike<S, D> for OwnedStream<S, D> {
    fn connect_to<P: Push<Message<S::Timestamp, D>> + 'static>(self, target: Target, pusher: P, identifier: usize) {
        let mut logging = self.scope().logging();
        logging.as_mut().map(|l| l.log(crate::logging::ChannelsEvent {
            id: identifier,
            scope_addr: self.scope.addr().to_vec(),
            source: (self.name.node, self.name.port),
            target: (target.node, target.port),
        }));

        self.scope.add_edge(self.name, target);
        self.port.set(pusher);
    }

    fn scope(&self) -> S {
        self.scope.clone()
    }
}

impl<S: Scope, C: Container> StreamCore<S, C> {
    /// Allocates a `Stream` from a supplied `Source` name and rendezvous point.
    pub fn new(source: Source, output: TeeHelper<S::Timestamp, C>, scope: S) -> Self {
        Self { name: source, ports: output, scope }
    }
    /// The name of the stream's source operator.
    pub fn name(&self) -> &Source { &self.name }
    /// The scope immediately containing the stream.
    pub fn scope(&self) -> S { self.scope.clone() }

    /// Allows the assertion of a container type, for the benefit of type inference.
    pub fn container<D: Container>(self) -> StreamCore<S, C> where Self: AsStream<S, D> { self.as_stream() }
}

/// A type that can be translated to a [StreamCore].
pub trait AsStream<S: Scope, C> {
    /// Translate `self` to a stream.
    fn as_stream(self) -> Self;
}

impl<S: Scope, C> AsStream<S, C> for StreamCore<S, C> {
    fn as_stream(self) -> Self { self }
}

impl<G: Scope, C> AsStream<G, C> for OwnedStream<G, C> {
    fn as_stream(self) -> Self { self }
}

impl<S, C> Debug for StreamCore<S, C>
where
    S: Scope,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Stream")
            .field("source", &self.name)
            // TODO: Use `.finish_non_exhaustive()` after rust/#67364 lands
            .finish()
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
                .sink(Pipeline, "check non-clone", |input| {
                    while let Some((_time, data)) = input.next() {
                        for datum in data.drain(..) {
                            assert_eq!(datum, NotClone);
                        }
                    }
                });
        });
    }

}
