
//! Methods to construct generic streaming and blocking unary operators.

use crate::dataflow::channels::pushers::Tee;
use crate::dataflow::channels::pact::ParallelizationContract;

use crate::dataflow::operators::generic::handles::{InputHandleCore, FrontieredInputHandleCore, OutputHandleCore};
use crate::dataflow::operators::capability::Capability;

use crate::dataflow::{Scope, StreamCore};

use super::builder_rc::OperatorBuilder;
use crate::dataflow::operators::generic::OperatorInfo;
use crate::dataflow::operators::generic::notificator::{Notificator, FrontierNotificator};
use crate::{Container, Data};
use crate::container::{ContainerBuilder, CapacityContainerBuilder};

/// Methods to construct generic streaming and blocking operators.
pub trait Operator<G: Scope, C1: Container> {
    /// Creates a new dataflow operator that partitions its input stream by a parallelization
    /// strategy `pact`, and repeatedly invokes `logic`, the function returned by the function passed as `constructor`.
    /// `logic` can read from the input stream, write to the output stream, and inspect the frontier at the input.
    ///
    /// # Examples
    /// ```
    /// use std::collections::HashMap;
    /// use timely::dataflow::operators::{ToStream, FrontierNotificator};
    /// use timely::dataflow::operators::generic::Operator;
    /// use timely::dataflow::channels::pact::Pipeline;
    ///
    /// timely::example(|scope| {
    ///     (0u64..10).to_stream(scope)
    ///         .unary_frontier(Pipeline, "example", |default_cap, _info| {
    ///             let mut cap = Some(default_cap.delayed(&12));
    ///             let mut notificator = FrontierNotificator::default();
    ///             let mut stash = HashMap::new();
    ///             move |input, output| {
    ///                 if let Some(ref c) = cap.take() {
    ///                     output.session(&c).give(12);
    ///                 }
    ///                 while let Some((time, data)) = input.next() {
    ///                     stash.entry(time.time().clone())
    ///                          .or_insert(Vec::new())
    ///                          .extend(data.drain(..));
    ///                 }
    ///                 notificator.for_each(&[input.frontier()], |time, _not| {
    ///                     if let Some(mut vec) = stash.remove(time.time()) {
    ///                         output.session(&time).give_iterator(vec.drain(..));
    ///                     }
    ///                 });
    ///             }
    ///         })
    ///         .container::<Vec<_>>();
    /// });
    /// ```
    fn unary_frontier<CB, B, L, P>(&self, pact: P, name: &str, constructor: B) -> StreamCore<G, CB::Container>
    where
        CB: ContainerBuilder,
        B: FnOnce(Capability<G::Timestamp>, OperatorInfo) -> L,
        L: FnMut(&mut FrontieredInputHandleCore<G::Timestamp, C1, P::Puller>,
                 &mut OutputHandleCore<G::Timestamp, CB, Tee<G::Timestamp, CB::Container>>)+'static,
        P: ParallelizationContract<G::Timestamp, C1>;

    /// Creates a new dataflow operator that partitions its input stream by a parallelization
    /// strategy `pact`, and repeatedly invokes `logic`, the function returned by the function passed as `constructor`.
    /// `logic` can read from the input stream, write to the output stream, and inspect the frontier at the input.
    ///
    /// # Examples
    /// ```
    /// use std::collections::HashMap;
    /// use timely::dataflow::operators::{ToStream, FrontierNotificator};
    /// use timely::dataflow::operators::generic::Operator;
    /// use timely::dataflow::channels::pact::Pipeline;
    ///
    /// timely::example(|scope| {
    ///     (0u64..10)
    ///         .to_stream(scope)
    ///         .unary_notify(Pipeline, "example", None, move |input, output, notificator| {
    ///             input.for_each(|time, data| {
    ///                 output.session(&time).give_container(data);
    ///                 notificator.notify_at(time.retain());
    ///             });
    ///             notificator.for_each(|time, _cnt, _not| {
    ///                 println!("notified at {:?}", time);
    ///             });
    ///         });
    /// });
    /// ```
    fn unary_notify<CB: ContainerBuilder,
            L: FnMut(&mut InputHandleCore<G::Timestamp, C1, P::Puller>,
                     &mut OutputHandleCore<G::Timestamp, CB, Tee<G::Timestamp, CB::Container>>,
                     &mut Notificator<G::Timestamp>)+'static,
             P: ParallelizationContract<G::Timestamp, C1>>
             (&self, pact: P, name: &str, init: impl IntoIterator<Item=G::Timestamp>, logic: L) -> StreamCore<G, CB::Container>;

    /// Creates a new dataflow operator that partitions its input stream by a parallelization
    /// strategy `pact`, and repeatedly invokes `logic`, the function returned by the function passed as `constructor`.
    /// `logic` can read from the input stream, and write to the output stream.
    ///
    /// # Examples
    /// ```
    /// use timely::dataflow::operators::{ToStream, FrontierNotificator};
    /// use timely::dataflow::operators::generic::operator::Operator;
    /// use timely::dataflow::channels::pact::Pipeline;
    /// use timely::dataflow::Scope;
    ///
    /// timely::example(|scope| {
    ///     (0u64..10).to_stream(scope)
    ///         .unary(Pipeline, "example", |default_cap, _info| {
    ///             let mut cap = Some(default_cap.delayed(&12));
    ///             move |input, output| {
    ///                 if let Some(ref c) = cap.take() {
    ///                     output.session(&c).give(100);
    ///                 }
    ///                 while let Some((time, data)) = input.next() {
    ///                     output.session(&time).give_container(data);
    ///                 }
    ///             }
    ///         });
    /// });
    /// ```
    fn unary<CB, B, L, P>(&self, pact: P, name: &str, constructor: B) -> StreamCore<G, CB::Container>
    where
        CB: ContainerBuilder,
        B: FnOnce(Capability<G::Timestamp>, OperatorInfo) -> L,
        L: FnMut(&mut InputHandleCore<G::Timestamp, C1, P::Puller>,
                 &mut OutputHandleCore<G::Timestamp, CB, Tee<G::Timestamp, CB::Container>>)+'static,
        P: ParallelizationContract<G::Timestamp, C1>;

    /// Creates a new dataflow operator that partitions its input streams by a parallelization
    /// strategy `pact`, and repeatedly invokes `logic`, the function returned by the function passed as `constructor`.
    /// `logic` can read from the input streams, write to the output stream, and inspect the frontier at the inputs.
    ///
    /// # Examples
    /// ```
    /// use std::collections::HashMap;
    /// use timely::dataflow::operators::{Input, Inspect, FrontierNotificator};
    /// use timely::dataflow::operators::generic::operator::Operator;
    /// use timely::dataflow::channels::pact::Pipeline;
    ///
    /// timely::execute(timely::Config::thread(), |worker| {
    ///    let (mut in1, mut in2) = worker.dataflow::<usize,_,_>(|scope| {
    ///        let (in1_handle, in1) = scope.new_input();
    ///        let (in2_handle, in2) = scope.new_input();
    ///        in1.binary_frontier(&in2, Pipeline, Pipeline, "example", |mut _default_cap, _info| {
    ///            let mut notificator = FrontierNotificator::default();
    ///            let mut stash = HashMap::new();
    ///            move |input1, input2, output| {
    ///                while let Some((time, data)) = input1.next() {
    ///                    stash.entry(time.time().clone()).or_insert(Vec::new()).extend(data.drain(..));
    ///                    notificator.notify_at(time.retain());
    ///                }
    ///                while let Some((time, data)) = input2.next() {
    ///                    stash.entry(time.time().clone()).or_insert(Vec::new()).extend(data.drain(..));
    ///                    notificator.notify_at(time.retain());
    ///                }
    ///                notificator.for_each(&[input1.frontier(), input2.frontier()], |time, _not| {
    ///                    if let Some(mut vec) = stash.remove(time.time()) {
    ///                        output.session(&time).give_iterator(vec.drain(..));
    ///                    }
    ///                });
    ///            }
    ///        })
    ///        .container::<Vec<_>>()
    ///        .inspect_batch(|t, x| println!("{:?} -> {:?}", t, x));
    ///
    ///        (in1_handle, in2_handle)
    ///    });
    ///
    ///    for i in 1..10 {
    ///        in1.send(i - 1);
    ///        in1.advance_to(i);
    ///        in2.send(i - 1);
    ///        in2.advance_to(i);
    ///    }
    /// }).unwrap();
    /// ```
    fn binary_frontier<C2, CB, B, L, P1, P2>(&self, other: &StreamCore<G, C2>, pact1: P1, pact2: P2, name: &str, constructor: B) -> StreamCore<G, CB::Container>
    where
        C2: Container + Data,
        CB: ContainerBuilder,
        B: FnOnce(Capability<G::Timestamp>, OperatorInfo) -> L,
        L: FnMut(&mut FrontieredInputHandleCore<G::Timestamp, C1, P1::Puller>,
                 &mut FrontieredInputHandleCore<G::Timestamp, C2, P2::Puller>,
                 &mut OutputHandleCore<G::Timestamp, CB, Tee<G::Timestamp, CB::Container>>)+'static,
        P1: ParallelizationContract<G::Timestamp, C1>,
        P2: ParallelizationContract<G::Timestamp, C2>;

    /// Creates a new dataflow operator that partitions its input streams by a parallelization
    /// strategy `pact`, and repeatedly invokes `logic`, the function returned by the function passed as `constructor`.
    /// `logic` can read from the input streams, write to the output stream, and inspect the frontier at the inputs.
    ///
    /// # Examples
    /// ```
    /// use std::collections::HashMap;
    /// use timely::dataflow::operators::{Input, Inspect, FrontierNotificator};
    /// use timely::dataflow::operators::generic::operator::Operator;
    /// use timely::dataflow::channels::pact::Pipeline;
    ///
    /// timely::execute(timely::Config::thread(), |worker| {
    ///    let (mut in1, mut in2) = worker.dataflow::<usize,_,_>(|scope| {
    ///        let (in1_handle, in1) = scope.new_input();
    ///        let (in2_handle, in2) = scope.new_input();
    ///
    ///        in1.binary_notify(&in2, Pipeline, Pipeline, "example", None, move |input1, input2, output, notificator| {
    ///            input1.for_each(|time, data| {
    ///                output.session(&time).give_container(data);
    ///                notificator.notify_at(time.retain());
    ///            });
    ///            input2.for_each(|time, data| {
    ///                output.session(&time).give_container(data);
    ///                notificator.notify_at(time.retain());
    ///            });
    ///            notificator.for_each(|time, _cnt, _not| {
    ///                println!("notified at {:?}", time);
    ///            });
    ///        });
    ///
    ///        (in1_handle, in2_handle)
    ///    });
    ///
    ///    for i in 1..10 {
    ///        in1.send(i - 1);
    ///        in1.advance_to(i);
    ///        in2.send(i - 1);
    ///        in2.advance_to(i);
    ///    }
    /// }).unwrap();
    /// ```
    fn binary_notify<C2: Container + Data,
              CB: ContainerBuilder,
              L: FnMut(&mut InputHandleCore<G::Timestamp, C1, P1::Puller>,
                       &mut InputHandleCore<G::Timestamp, C2, P2::Puller>,
                       &mut OutputHandleCore<G::Timestamp, CB, Tee<G::Timestamp, CB::Container>>,
                       &mut Notificator<G::Timestamp>)+'static,
              P1: ParallelizationContract<G::Timestamp, C1>,
              P2: ParallelizationContract<G::Timestamp, C2>>
            (&self, other: &StreamCore<G, C2>, pact1: P1, pact2: P2, name: &str, init: impl IntoIterator<Item=G::Timestamp>, logic: L) -> StreamCore<G, CB::Container>;

    /// Creates a new dataflow operator that partitions its input streams by a parallelization
    /// strategy `pact`, and repeatedly invokes `logic`, the function returned by the function passed as `constructor`.
    /// `logic` can read from the input streams, write to the output stream, and inspect the frontier at the inputs.
    ///
    /// # Examples
    /// ```
    /// use timely::dataflow::operators::{ToStream, Inspect, FrontierNotificator};
    /// use timely::dataflow::operators::generic::operator::Operator;
    /// use timely::dataflow::channels::pact::Pipeline;
    /// use timely::dataflow::Scope;
    ///
    /// timely::example(|scope| {
    ///     let stream2 = (0u64..10).to_stream(scope);
    ///     (0u64..10).to_stream(scope)
    ///         .binary(&stream2, Pipeline, Pipeline, "example", |default_cap, _info| {
    ///             let mut cap = Some(default_cap.delayed(&12));
    ///             move |input1, input2, output| {
    ///                 if let Some(ref c) = cap.take() {
    ///                     output.session(&c).give(100);
    ///                 }
    ///                 while let Some((time, data)) = input1.next() {
    ///                     output.session(&time).give_container(data);
    ///                 }
    ///                 while let Some((time, data)) = input2.next() {
    ///                     output.session(&time).give_container(data);
    ///                 }
    ///             }
    ///         }).inspect(|x| println!("{:?}", x));
    /// });
    /// ```
    fn binary<C2, CB, B, L, P1, P2>(&self, other: &StreamCore<G, C2>, pact1: P1, pact2: P2, name: &str, constructor: B) -> StreamCore<G, CB::Container>
    where
        C2: Container + Data,
        CB: ContainerBuilder,
        B: FnOnce(Capability<G::Timestamp>, OperatorInfo) -> L,
        L: FnMut(&mut InputHandleCore<G::Timestamp, C1, P1::Puller>,
                 &mut InputHandleCore<G::Timestamp, C2, P2::Puller>,
                 &mut OutputHandleCore<G::Timestamp, CB, Tee<G::Timestamp, CB::Container>>)+'static,
        P1: ParallelizationContract<G::Timestamp, C1>,
        P2: ParallelizationContract<G::Timestamp, C2>;

    /// Creates a new dataflow operator that partitions its input stream by a parallelization
    /// strategy `pact`, and repeatedly invokes the function `logic` which can read from the input stream
    /// and inspect the frontier at the input.
    ///
    /// # Examples
    /// ```
    /// use timely::dataflow::operators::{ToStream, FrontierNotificator};
    /// use timely::dataflow::operators::generic::operator::Operator;
    /// use timely::dataflow::channels::pact::Pipeline;
    /// use timely::dataflow::Scope;
    ///
    /// timely::example(|scope| {
    ///     (0u64..10)
    ///         .to_stream(scope)
    ///         .sink(Pipeline, "example", |input| {
    ///             while let Some((time, data)) = input.next() {
    ///                 for datum in data.iter() {
    ///                     println!("{:?}:\t{:?}", time, datum);
    ///                 }
    ///             }
    ///         });
    /// });
    /// ```
    fn sink<L, P>(&self, pact: P, name: &str, logic: L)
    where
        L: FnMut(&mut FrontieredInputHandleCore<G::Timestamp, C1, P::Puller>)+'static,
        P: ParallelizationContract<G::Timestamp, C1>;
}

impl<G: Scope, C1: Container + Data> Operator<G, C1> for StreamCore<G, C1> {

    fn unary_frontier<CB, B, L, P>(&self, pact: P, name: &str, constructor: B) -> StreamCore<G, CB::Container>
    where
        CB: ContainerBuilder,
        B: FnOnce(Capability<G::Timestamp>, OperatorInfo) -> L,
        L: FnMut(&mut FrontieredInputHandleCore<G::Timestamp, C1, P::Puller>,
                 &mut OutputHandleCore<G::Timestamp, CB, Tee<G::Timestamp, CB::Container>>)+'static,
        P: ParallelizationContract<G::Timestamp, C1> {

        let mut builder = OperatorBuilder::new(name.to_owned(), self.scope());
        let operator_info = builder.operator_info();

        let mut input = builder.new_input(self, pact);
        let (mut output, stream) = builder.new_output();

        builder.build(move |mut capabilities| {
            // `capabilities` should be a single-element vector.
            let capability = capabilities.pop().unwrap();
            let mut logic = constructor(capability, operator_info);
            move |frontiers| {
                let mut input_handle = FrontieredInputHandleCore::new(&mut input, &frontiers[0]);
                let mut output_handle = output.activate();
                logic(&mut input_handle, &mut output_handle);
            }
        });

        stream
    }

    fn unary_notify<CB: ContainerBuilder,
            L: FnMut(&mut InputHandleCore<G::Timestamp, C1, P::Puller>,
                     &mut OutputHandleCore<G::Timestamp, CB, Tee<G::Timestamp, CB::Container>>,
                     &mut Notificator<G::Timestamp>)+'static,
             P: ParallelizationContract<G::Timestamp, C1>>
             (&self, pact: P, name: &str, init: impl IntoIterator<Item=G::Timestamp>, mut logic: L) -> StreamCore<G, CB::Container> {

        self.unary_frontier(pact, name, move |capability, _info| {
            let mut notificator = FrontierNotificator::default();
            for time in init {
                notificator.notify_at(capability.delayed(&time));
            }

            let logging = self.scope().logging();
            move |input, output| {
                let frontier = &[input.frontier()];
                let notificator = &mut Notificator::new(frontier, &mut notificator, &logging);
                logic(input.handle, output, notificator);
            }
        })
    }

    fn unary<CB, B, L, P>(&self, pact: P, name: &str, constructor: B) -> StreamCore<G, CB::Container>
    where
        CB: ContainerBuilder,
        B: FnOnce(Capability<G::Timestamp>, OperatorInfo) -> L,
        L: FnMut(&mut InputHandleCore<G::Timestamp, C1, P::Puller>,
                 &mut OutputHandleCore<G::Timestamp, CB, Tee<G::Timestamp, CB::Container>>)+'static,
        P: ParallelizationContract<G::Timestamp, C1> {

        let mut builder = OperatorBuilder::new(name.to_owned(), self.scope());
        let operator_info = builder.operator_info();

        let mut input = builder.new_input(self, pact);
        let (mut output, stream) = builder.new_output();
        builder.set_notify(false);

        builder.build(move |mut capabilities| {
            // `capabilities` should be a single-element vector.
            let capability = capabilities.pop().unwrap();
            let mut logic = constructor(capability, operator_info);
            move |_frontiers| {
                let mut output_handle = output.activate();
                logic(&mut input, &mut output_handle);
            }
        });

        stream
    }

    fn binary_frontier<C2, CB, B, L, P1, P2>(&self, other: &StreamCore<G, C2>, pact1: P1, pact2: P2, name: &str, constructor: B) -> StreamCore<G, CB::Container>
    where
        C2: Container + Data,
        CB: ContainerBuilder,
        B: FnOnce(Capability<G::Timestamp>, OperatorInfo) -> L,
        L: FnMut(&mut FrontieredInputHandleCore<G::Timestamp, C1, P1::Puller>,
                 &mut FrontieredInputHandleCore<G::Timestamp, C2, P2::Puller>,
                 &mut OutputHandleCore<G::Timestamp, CB, Tee<G::Timestamp, CB::Container>>)+'static,
        P1: ParallelizationContract<G::Timestamp, C1>,
        P2: ParallelizationContract<G::Timestamp, C2> {

        let mut builder = OperatorBuilder::new(name.to_owned(), self.scope());
        let operator_info = builder.operator_info();

        let mut input1 = builder.new_input(self, pact1);
        let mut input2 = builder.new_input(other, pact2);
        let (mut output, stream) = builder.new_output();

        builder.build(move |mut capabilities| {
            // `capabilities` should be a single-element vector.
            let capability = capabilities.pop().unwrap();
            let mut logic = constructor(capability, operator_info);
            move |frontiers| {
                let mut input1_handle = FrontieredInputHandleCore::new(&mut input1, &frontiers[0]);
                let mut input2_handle = FrontieredInputHandleCore::new(&mut input2, &frontiers[1]);
                let mut output_handle = output.activate();
                logic(&mut input1_handle, &mut input2_handle, &mut output_handle);
            }
        });

        stream
    }

    fn binary_notify<C2: Container + Data,
              CB: ContainerBuilder,
              L: FnMut(&mut InputHandleCore<G::Timestamp, C1, P1::Puller>,
                       &mut InputHandleCore<G::Timestamp, C2, P2::Puller>,
                       &mut OutputHandleCore<G::Timestamp, CB, Tee<G::Timestamp, CB::Container>>,
                       &mut Notificator<G::Timestamp>)+'static,
              P1: ParallelizationContract<G::Timestamp, C1>,
              P2: ParallelizationContract<G::Timestamp, C2>>
            (&self, other: &StreamCore<G, C2>, pact1: P1, pact2: P2, name: &str, init: impl IntoIterator<Item=G::Timestamp>, mut logic: L) -> StreamCore<G, CB::Container> {

        self.binary_frontier(other, pact1, pact2, name, |capability, _info| {
            let mut notificator = FrontierNotificator::default();
            for time in init {
                notificator.notify_at(capability.delayed(&time));
            }

            let logging = self.scope().logging();
            move |input1, input2, output| {
                let frontiers = &[input1.frontier(), input2.frontier()];
                let notificator = &mut Notificator::new(frontiers, &mut notificator, &logging);
                logic(input1.handle, input2.handle, output, notificator);
            }
        })

    }


    fn binary<C2, CB, B, L, P1, P2>(&self, other: &StreamCore<G, C2>, pact1: P1, pact2: P2, name: &str, constructor: B) -> StreamCore<G, CB::Container>
    where
        C2: Container + Data,
        CB: ContainerBuilder,
        B: FnOnce(Capability<G::Timestamp>, OperatorInfo) -> L,
        L: FnMut(&mut InputHandleCore<G::Timestamp, C1, P1::Puller>,
                 &mut InputHandleCore<G::Timestamp, C2, P2::Puller>,
                 &mut OutputHandleCore<G::Timestamp, CB, Tee<G::Timestamp, CB::Container>>)+'static,
        P1: ParallelizationContract<G::Timestamp, C1>,
        P2: ParallelizationContract<G::Timestamp, C2> {

        let mut builder = OperatorBuilder::new(name.to_owned(), self.scope());
        let operator_info = builder.operator_info();

        let mut input1 = builder.new_input(self, pact1);
        let mut input2 = builder.new_input(other, pact2);
        let (mut output, stream) = builder.new_output();
        builder.set_notify(false);

        builder.build(move |mut capabilities| {
            // `capabilities` should be a single-element vector.
            let capability = capabilities.pop().unwrap();
            let mut logic = constructor(capability, operator_info);
            move |_frontiers| {
                let mut output_handle = output.activate();
                logic(&mut input1, &mut input2, &mut output_handle);
            }
        });

        stream
    }

    fn sink<L, P>(&self, pact: P, name: &str, mut logic: L)
    where
        L: FnMut(&mut FrontieredInputHandleCore<G::Timestamp, C1, P::Puller>)+'static,
        P: ParallelizationContract<G::Timestamp, C1> {

        let mut builder = OperatorBuilder::new(name.to_owned(), self.scope());
        let mut input = builder.new_input(self, pact);

        builder.build(|_capabilities| {
            move |frontiers| {
                let mut input_handle = FrontieredInputHandleCore::new(&mut input, &frontiers[0]);
                logic(&mut input_handle);
            }
        });
    }
}

/// Creates a new data stream source for a scope.
///
/// The source is defined by a name, and a constructor which takes a default capability to
/// a method that can be repeatedly called on a output handle. The method is then repeatedly
/// invoked, and is expected to eventually send data and downgrade and release capabilities.
///
/// # Examples
/// ```
/// use timely::scheduling::Scheduler;
/// use timely::dataflow::operators::Inspect;
/// use timely::dataflow::operators::generic::operator::source;
/// use timely::dataflow::Scope;
///
/// timely::example(|scope| {
///
///     source(scope, "Source", |capability, info| {
///
///         let activator = scope.activator_for(info.address);
///
///         let mut cap = Some(capability);
///         move |output| {
///
///             let mut done = false;
///             if let Some(cap) = cap.as_mut() {
///                 // get some data and send it.
///                 let time = cap.time().clone();
///                 output.session(&cap)
///                       .give(*cap.time());
///
///                 // downgrade capability.
///                 cap.downgrade(&(time + 1));
///                 done = time > 20;
///             }
///
///             if done { cap = None; }
///             else    { activator.activate(); }
///         }
///     })
///     .container::<Vec<_>>()
///     .inspect(|x| println!("number: {:?}", x));
/// });
/// ```
pub fn source<G: Scope, CB, B, L>(scope: &G, name: &str, constructor: B) -> StreamCore<G, CB::Container>
where
    CB: ContainerBuilder,
    B: FnOnce(Capability<G::Timestamp>, OperatorInfo) -> L,
    L: FnMut(&mut OutputHandleCore<G::Timestamp, CB, Tee<G::Timestamp, CB::Container>>)+'static {

    let mut builder = OperatorBuilder::new(name.to_owned(), scope.clone());
    let operator_info = builder.operator_info();

    let (mut output, stream) = builder.new_output();
    builder.set_notify(false);

    builder.build(move |mut capabilities| {
        // `capabilities` should be a single-element vector.
        let capability = capabilities.pop().unwrap();
        let mut logic = constructor(capability, operator_info);
        move |_frontier| {
            logic(&mut output.activate());
        }
    });

    stream
}

/// Constructs an empty stream.
///
/// This method is useful in patterns where an input is required, but there is no
/// meaningful data to provide. The replaces patterns like `stream.filter(|_| false)`
/// which are just silly.
///
/// # Examples
/// ```
/// use timely::dataflow::operators::Inspect;
/// use timely::dataflow::operators::generic::operator::empty;
/// use timely::dataflow::Scope;
///
/// timely::example(|scope| {
///
///
///     empty::<_, Vec<_>>(scope)     // type required in this example
///         .inspect(|()| panic!("never called"));
///
/// });
/// ```
pub fn empty<G: Scope, C: Container + Data>(scope: &G) -> StreamCore<G, C> {
    source::<_, CapacityContainerBuilder<C>, _, _>(scope, "Empty", |_capability, _info| |_output| {
        // drop capability, do nothing
    })
}
