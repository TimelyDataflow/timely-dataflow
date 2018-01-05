
//! Methods to construct generic streaming and blocking unary operators.

use dataflow::channels::pushers::Tee;
use dataflow::channels::pact::ParallelizationContract;

use dataflow::operators::generic::handles::{InputHandle, FrontieredInputHandle, OutputHandle};
use dataflow::operators::generic::handles::{new_frontier_input_handle};
use dataflow::operators::capability::Capability;

use ::Data;

use dataflow::{Stream, Scope};

use super::builder_rc::OperatorBuilder;

/// Methods to construct generic streaming and blocking operators.
pub trait Operator<G: Scope, D1: Data> {
    /// Creates a new dataflow operator that partitions its input stream by a parallelization
    /// strategy `pact`, and repeteadly invokes `logic`, the function returned by the function passed as `constructor`.
    /// `logic` can read from the input stream, write to the output stream, and inspect the frontier at the input.
    ///
    /// #Examples
    /// ```
    /// use std::collections::HashMap;
    /// use timely::dataflow::operators::{ToStream, FrontierNotificator};
    /// use timely::dataflow::operators::generic::Operator;
    /// use timely::dataflow::channels::pact::Pipeline;
    /// use timely::progress::timestamp::RootTimestamp;
    ///
    /// fn main() {
    ///     timely::example(|scope| {
    ///         (0u64..10).to_stream(scope)
    ///             .unary_frontier(Pipeline, "example", |default_cap| {
    ///                 let mut cap = Some(default_cap.delayed(&RootTimestamp::new(12)));
    ///                 let mut notificator = FrontierNotificator::new();
    ///                 let mut stash = HashMap::new();
    ///                 move |input, output| {
    ///                     if let Some(ref c) = cap.take() {
    ///                         output.session(&c).give(12);
    ///                     }
    ///                     while let Some((time, data)) = input.next() {
    ///                         stash.entry(time.time().clone()).or_insert(Vec::new());
    ///                     }
    ///                     notificator.for_each(&[input.frontier()], |time, _not| {
    ///                         if let Some(mut vec) = stash.remove(time.time()) {
    ///                             output.session(&time).give_iterator(vec.drain(..));
    ///                         }
    ///                     });
    ///                 }
    ///             });
    ///     });
    /// }
    /// ```
    fn unary_frontier<D2, B, L, P>(&self, pact: P, name: &str, constructor: B) -> Stream<G, D2>
    where
        D2: Data,
        B: FnOnce(Capability<G::Timestamp>) -> L,
        L: FnMut(&mut FrontieredInputHandle<G::Timestamp, D1, P::Puller>, 
                 &mut OutputHandle<G::Timestamp, D2, Tee<G::Timestamp, D2>>)+'static,
        P: ParallelizationContract<G::Timestamp, D1>;

    /// Creates a new dataflow operator that partitions its input stream by a parallelization
    /// strategy `pact`, and repeteadly invokes `logic`, the function returned by the function passed as `constructor`.
    /// `logic` can read from the input stream, and write to the output stream.
    ///
    /// #Examples
    /// ```
    /// use timely::dataflow::operators::{ToStream, FrontierNotificator};
    /// use timely::dataflow::operators::generic::operator::Operator;
    /// use timely::dataflow::channels::pact::Pipeline;
    /// use timely::progress::timestamp::RootTimestamp;
    /// use timely::dataflow::Scope;
    ///
    /// timely::example(|scope| {
    ///     (0u64..10).to_stream(scope)
    ///         .unary(Pipeline, "example", |default_cap| {
    ///             let mut cap = Some(default_cap.delayed(&RootTimestamp::new(12)));
    ///             move |input, output| {
    ///                 if let Some(ref c) = cap.take() {
    ///                     output.session(&c).give(100);
    ///                 }
    ///                 while let Some((time, data)) = input.next() {
    ///                     output.session(&time).give_content(data);
    ///                 }
    ///             }
    ///         });
    /// });
    /// ```
    fn unary<D2, B, L, P>(&self, pact: P, name: &str, constructor: B) -> Stream<G, D2>
    where
        D2: Data,
        B: FnOnce(Capability<G::Timestamp>) -> L,
        L: FnMut(&mut InputHandle<G::Timestamp, D1, P::Puller>, 
                 &mut OutputHandle<G::Timestamp, D2, Tee<G::Timestamp, D2>>)+'static,
        P: ParallelizationContract<G::Timestamp, D1>;

    /// Creates a new dataflow operator that partitions its input streams by a parallelization
    /// strategy `pact`, and repeteadly invokes `logic`, the function returned by the function passed as `constructor`.
    /// `logic` can read from the input streams, write to the output stream, and inspect the frontier at the inputs.
    ///
    /// #Examples
    /// ```
    /// use std::collections::HashMap;
    /// use timely::dataflow::operators::{Input, Inspect, FrontierNotificator};
    /// use timely::dataflow::operators::generic::operator::Operator;
    /// use timely::dataflow::channels::pact::Pipeline;
    ///
    /// timely::execute(timely::Configuration::Thread, |worker| {
    ///    let (mut in1, mut in2) = worker.dataflow(|scope| {
    ///        let (in1_handle, in1) = scope.new_input();
    ///        let (in2_handle, in2) = scope.new_input();
    ///        in1.binary_frontier(&in2, Pipeline, Pipeline, "example", |mut _builder| {
    ///            let mut notificator = FrontierNotificator::new();
    ///            let mut stash = HashMap::new();
    ///            move |input1, input2, output| {
    ///                while let Some((time, data)) = input1.next() {
    ///                    stash.entry(time.time().clone()).or_insert(Vec::new()).extend(data.drain(..));
    ///                    notificator.notify_at(time);
    ///                }
    ///                while let Some((time, data)) = input2.next() {
    ///                    stash.entry(time.time().clone()).or_insert(Vec::new()).extend(data.drain(..));
    ///                    notificator.notify_at(time);
    ///                }
    ///                notificator.for_each(&[input1.frontier(), input2.frontier()], |time, _not| {
    ///                    if let Some(mut vec) = stash.remove(time.time()) {
    ///                        output.session(&time).give_iterator(vec.drain(..));
    ///                    }
    ///                });
    ///            }
    ///        }).inspect_batch(|t, x| println!("{:?} -> {:?}", t, x));
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
    fn binary_frontier<D2, D3, B, L, P1, P2>(&self, other: &Stream<G, D2>, pact1: P1, pact2: P2, name: &str, constructor: B) -> Stream<G, D3>
    where
        D2: Data,
        D3: Data,
        B: FnOnce(Capability<G::Timestamp>) -> L,
        L: FnMut(&mut FrontieredInputHandle<G::Timestamp, D1, P1::Puller>,
                 &mut FrontieredInputHandle<G::Timestamp, D2, P2::Puller>,
                 &mut OutputHandle<G::Timestamp, D3, Tee<G::Timestamp, D3>>)+'static,
        P1: ParallelizationContract<G::Timestamp, D1>,
        P2: ParallelizationContract<G::Timestamp, D2>;

    /// Creates a new dataflow operator that partitions its input streams by a parallelization
    /// strategy `pact`, and repeteadly invokes `logic`, the function returned by the function passed as `constructor`.
    /// `logic` can read from the input streams, write to the output stream, and inspect the frontier at the inputs.
    ///
    /// #Examples
    /// ```
    /// use timely::dataflow::operators::{ToStream, Inspect, FrontierNotificator};
    /// use timely::dataflow::operators::generic::operator::Operator;
    /// use timely::dataflow::channels::pact::Pipeline;
    /// use timely::progress::timestamp::RootTimestamp;
    /// use timely::dataflow::Scope;
    ///
    /// timely::example(|scope| {
    ///     let stream2 = (0u64..10).to_stream(scope);
    ///     (0u64..10).to_stream(scope)
    ///         .binary(&stream2, Pipeline, Pipeline, "example", |default_cap| {
    ///             let mut cap = Some(default_cap.delayed(&RootTimestamp::new(12)));
    ///             move |input1, input2, output| {
    ///                 if let Some(ref c) = cap.take() {
    ///                     output.session(&c).give(100);
    ///                 }
    ///                 while let Some((time, data)) = input1.next() {
    ///                     output.session(&time).give_content(data);
    ///                 }
    ///                 while let Some((time, data)) = input2.next() {
    ///                     output.session(&time).give_content(data);
    ///                 }
    ///             }
    ///         }).inspect(|x| println!("{:?}", x));
    /// });
    /// ```
    fn binary<D2, D3, B, L, P1, P2>(&self, other: &Stream<G, D2>, pact1: P1, pact2: P2, name: &str, constructor: B) -> Stream<G, D3>
    where
        D2: Data,
        D3: Data,
        B: FnOnce(Capability<G::Timestamp>) -> L,
        L: FnMut(&mut InputHandle<G::Timestamp, D1, P1::Puller>, 
                 &mut InputHandle<G::Timestamp, D2, P2::Puller>, 
                 &mut OutputHandle<G::Timestamp, D3, Tee<G::Timestamp, D3>>)+'static,
        P1: ParallelizationContract<G::Timestamp, D1>,
        P2: ParallelizationContract<G::Timestamp, D2>;

    /// Creates a new dataflow operator that partitions its input stream by a parallelization
    /// strategy `pact`, and repeteadly invokes the function `logic` which can read from the input stream
    /// and inspect the frontier at the input.
    ///
    /// #Examples
    /// ```
    /// use timely::dataflow::operators::{ToStream, FrontierNotificator};
    /// use timely::dataflow::operators::generic::operator::Operator;
    /// use timely::dataflow::channels::pact::Pipeline;
    /// use timely::progress::timestamp::RootTimestamp;
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
        L: FnMut(&mut FrontieredInputHandle<G::Timestamp, D1, P::Puller>)+'static,
        P: ParallelizationContract<G::Timestamp, D1>;
}

impl<G: Scope, D1: Data> Operator<G, D1> for Stream<G, D1> {

    fn unary_frontier<D2, B, L, P>(&self, pact: P, name: &str, constructor: B) -> Stream<G, D2>
    where
        D2: Data,
        B: FnOnce(Capability<G::Timestamp>) -> L,
        L: FnMut(&mut FrontieredInputHandle<G::Timestamp, D1, P::Puller>, 
                 &mut OutputHandle<G::Timestamp, D2, Tee<G::Timestamp, D2>>)+'static,
        P: ParallelizationContract<G::Timestamp, D1> {

        let mut builder = OperatorBuilder::new(name.to_owned(), self.scope());

        let mut input = builder.new_input(self, pact);
        let (mut output, stream) = builder.new_output();

        builder.build(move |capability| {
            let mut logic = constructor(capability);
            move |frontiers| {
                let mut input_handle = new_frontier_input_handle(&mut input, &frontiers[0]);
                let mut output_handle = output.activate();
                logic(&mut input_handle, &mut output_handle);        
            }
        });

        stream
    }

    fn unary<D2, B, L, P>(&self, pact: P, name: &str, constructor: B) -> Stream<G, D2>
    where
        D2: Data,
        B: FnOnce(Capability<G::Timestamp>) -> L,
        L: FnMut(&mut InputHandle<G::Timestamp, D1, P::Puller>, 
                 &mut OutputHandle<G::Timestamp, D2, Tee<G::Timestamp, D2>>)+'static,
        P: ParallelizationContract<G::Timestamp, D1> {

        let mut builder = OperatorBuilder::new(name.to_owned(), self.scope());

        let mut input = builder.new_input(self, pact);
        let (mut output, stream) = builder.new_output();
        builder.set_notify(false);

        builder.build(move |capability| {
            let mut logic = constructor(capability);
            move |_frontiers| {
                let mut output_handle = output.activate();
                logic(&mut input, &mut output_handle);        
            }
        });

        stream
    }

    fn binary_frontier<D2, D3, B, L, P1, P2>(&self, other: &Stream<G, D2>, pact1: P1, pact2: P2, name: &str, constructor: B) -> Stream<G, D3>
    where
        D2: Data,
        D3: Data,
        B: FnOnce(Capability<G::Timestamp>) -> L,
        L: FnMut(&mut FrontieredInputHandle<G::Timestamp, D1, P1::Puller>,
                 &mut FrontieredInputHandle<G::Timestamp, D2, P2::Puller>,
                 &mut OutputHandle<G::Timestamp, D3, Tee<G::Timestamp, D3>>)+'static,
        P1: ParallelizationContract<G::Timestamp, D1>,
        P2: ParallelizationContract<G::Timestamp, D2> {

        let mut builder = OperatorBuilder::new(name.to_owned(), self.scope());

        let mut input1 = builder.new_input(self, pact1);
        let mut input2 = builder.new_input(other, pact2);
        let (mut output, stream) = builder.new_output();

        builder.build(move |capability| {
            let mut logic = constructor(capability);
            move |frontiers| {
                let mut input1_handle = new_frontier_input_handle(&mut input1, &frontiers[0]);
                let mut input2_handle = new_frontier_input_handle(&mut input2, &frontiers[1]);
                let mut output_handle = output.activate();
                logic(&mut input1_handle, &mut input2_handle, &mut output_handle);        
            }
        });

        stream
    }

    fn binary<D2, D3, B, L, P1, P2>(&self, other: &Stream<G, D2>, pact1: P1, pact2: P2, name: &str, constructor: B) -> Stream<G, D3>
    where
        D2: Data,
        D3: Data,
        B: FnOnce(Capability<G::Timestamp>) -> L,
        L: FnMut(&mut InputHandle<G::Timestamp, D1, P1::Puller>, 
                 &mut InputHandle<G::Timestamp, D2, P2::Puller>, 
                 &mut OutputHandle<G::Timestamp, D3, Tee<G::Timestamp, D3>>)+'static,
        P1: ParallelizationContract<G::Timestamp, D1>,
        P2: ParallelizationContract<G::Timestamp, D2> {

        let mut builder = OperatorBuilder::new(name.to_owned(), self.scope());

        let mut input1 = builder.new_input(self, pact1);
        let mut input2 = builder.new_input(other, pact2);
        let (mut output, stream) = builder.new_output();
        builder.set_notify(false);

        builder.build(move |capability| {
            let mut logic = constructor(capability);
            move |_frontiers| {
                let mut output_handle = output.activate();
                logic(&mut input1, &mut input2, &mut output_handle);        
            }
        });

        stream
    }

    fn sink<L, P>(&self, pact: P, name: &str, mut logic: L) 
    where 
        L: FnMut(&mut FrontieredInputHandle<G::Timestamp, D1, P::Puller>)+'static,
        P: ParallelizationContract<G::Timestamp, D1> {

        let mut builder = OperatorBuilder::new(name.to_owned(), self.scope());
        let mut input = builder.new_input(self, pact);

        builder.build(|_capability| {
            move |frontiers| {
                let mut input_handle = new_frontier_input_handle(&mut input, &frontiers[0]);
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
/// #Examples
/// ```
/// use timely::dataflow::operators::Inspect;
/// use timely::dataflow::operators::generic::operator::source;
/// use timely::dataflow::Scope;
///
/// timely::example(|scope| {
///
///     source(scope, "Source", |capability| {
///         let mut cap = Some(capability);
///         move |output| {
///
///             let mut done = false;
///             if let Some(cap) = cap.as_mut() {
///                 // get some data and send it.
///                 let mut time = cap.time().clone();
///                 output.session(&cap)
///                       .give(cap.time().inner);
///
///                 // downgrade capability.
///                 time.inner += 1;
///                 *cap = cap.delayed(&time);
///                 done = time.inner > 20;
///             }
///
///             if done { cap = None; }
///         }
///     })
///     .inspect(|x| println!("number: {:?}", x));
/// });
/// ```
pub fn source<G: Scope, D, B, L>(scope: &G, name: &str, constructor: B) -> Stream<G, D>
where
    D: Data,
    B: FnOnce(Capability<G::Timestamp>) -> L,
    L: FnMut(&mut OutputHandle<G::Timestamp, D, Tee<G::Timestamp, D>>)+'static {

    let mut builder = OperatorBuilder::new(name.to_owned(), scope.clone());

    let (mut output, stream) = builder.new_output();
    builder.set_notify(false);

    builder.build(|capability| {
        let mut logic = constructor(capability);
        move |_frontier| {
            logic(&mut output.activate());
        }
    });

    stream
}