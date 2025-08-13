//! Partition a stream of records into multiple streams.

use timely_container::{Container, ContainerBuilder, PushInto};

use crate::dataflow::channels::pact::Pipeline;
use crate::dataflow::operators::generic::builder_rc::OperatorBuilder;
use crate::dataflow::operators::InputCapability;
use crate::dataflow::{Scope, StreamCore};
use crate::{Data, PartialOrder};

/// Partition a stream of records into multiple streams.
pub trait Partition<G: Scope, C: Container> {
    /// Produces `parts` output streams, containing records produced and assigned by `route`.
    ///
    /// # Examples
    /// ```
    /// use timely::dataflow::operators::ToStream;
    /// use timely::dataflow::operators::core::{Partition, Inspect};
    /// use timely_container::CapacityContainerBuilder;
    ///
    /// timely::example(|scope| {
    ///     let streams = (0..10).to_stream(scope)
    ///                          .partition::<CapacityContainerBuilder<Vec<_>>, _, _>(3, |x| (x % 3, x));
    ///
    ///     for (idx, stream) in streams.into_iter().enumerate() {
    ///         stream
    ///             .inspect(move |x| println!("seen {idx}: {x:?}"));
    ///     }
    /// });
    /// ```
    fn partition<CB, D2, F>(&self, parts: u64, route: F) -> Vec<StreamCore<G, CB::Container>>
    where
        CB: ContainerBuilder + PushInto<D2>,
        CB::Container: Data,
        F: FnMut(C::Item<'_>) -> (u64, D2) + 'static;
}

impl<G: Scope, C: Container + Data> Partition<G, C> for StreamCore<G, C> {
    fn partition<CB, D2, F>(&self, parts: u64, mut route: F) -> Vec<StreamCore<G, CB::Container>>
    where
        CB: ContainerBuilder + PushInto<D2>,
        CB::Container: Data,
        F: FnMut(C::Item<'_>) -> (u64, D2) + 'static,
    {
        let mut builder = OperatorBuilder::new("Partition".to_owned(), self.scope());
        builder.set_notify(false);

        let mut input = builder.new_input(self, Pipeline);
        let mut outputs = Vec::with_capacity(parts as usize);
        let mut streams = Vec::with_capacity(parts as usize);

        for _ in 0..parts {
            let (output, stream) = builder.new_output::<CB>();
            outputs.push(output);
            streams.push(stream);
        }

        builder.build(move |_| {
            move |_frontiers| {
                #[derive(Default)]
                enum SessionState<H, S> {
                    Handle(H),
                    Session(S),
                    #[default]
                    Invalid,
                }

                let mut handles = outputs.iter_mut().map(|o| o.activate()).collect::<Vec<_>>();

                // The capability associated with each session in `sessions`.
                let mut sessions_cap: Option<InputCapability<G::Timestamp>> = None;
                let mut sessions = vec![];

                while let Some((cap, data)) = input.next() {
                    let reset_sessions = match sessions_cap {
                        Some(ref s_cap) => !PartialOrder::less_equal(s_cap.time(), cap.time()),
                        None => true,
                    };
                    if reset_sessions {
                        sessions = handles.iter_mut().map(SessionState::Handle).collect();
                        sessions_cap = Some(cap);
                    }
                    for datum in data.drain() {
                        let (part, datum2) = route(datum);

                        let session = match &mut sessions[part as usize] {
                            SessionState::Session(s) => s,
                            st @ SessionState::Handle(_) => {
                                let SessionState::Handle(handle) = std::mem::take(st) else {
                                    unreachable!();
                                };
                                let session = handle.session_with_builder(sessions_cap.as_ref().unwrap());
                                *st = SessionState::Session(session);
                                let SessionState::Session(session) = st else {
                                    unreachable!();
                                };
                                session
                            }
                            SessionState::Invalid => unreachable!(),
                        };
                        session.give(datum2);
                    }
                }
            }
        });

        streams
    }
}
