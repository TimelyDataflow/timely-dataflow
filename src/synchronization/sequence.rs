//! A shared ordered log.

use std::rc::Rc;
use std::cell::RefCell;
use std::time::{Instant, Duration};
use std::collections::VecDeque;

use ::{communication::Allocate, ExchangeData};
use ::scheduling::Scheduler;
use worker::Worker;
use dataflow::channels::pact::Exchange;
use dataflow::operators::generic::operator::source;
use dataflow::operators::generic::operator::Operator;

/// Orders elements inserted across all workers.
///
/// A Sequencer allows each worker to insert into a consistent ordered sequence
/// that is seen by all workers in the same order.
pub struct Sequencer<T> {
    send: Rc<RefCell<VecDeque<T>>>, // proposed items.
    recv: Rc<RefCell<VecDeque<T>>>, // sequenced items.
}

impl<T: Ord+ExchangeData> Sequencer<T> {

    /// Creates a new Sequencer.
    ///
    /// The `timer` instant is used to synchronize the workers, who use this
    /// elapsed time as their timestamp. Elements are ordered by this time,
    /// and cannot be made visible until all workers have reached the time.
    pub fn new<A: Allocate>(worker: &mut Worker<A>, timer: Instant) -> Self {

        let send: Rc<RefCell<VecDeque<T>>> = Rc::new(RefCell::new(VecDeque::new()));
        let recv = Rc::new(RefCell::new(VecDeque::new()));
        let send_weak = Rc::downgrade(&send);
        let recv_weak = Rc::downgrade(&recv);

        // build a dataflow used to serialize and circulate commands
        worker.dataflow::<Duration,_,_>(move |dataflow| {

            let peers = dataflow.peers();
            let mut recvd = Vec::new();
            let mut vector = Vec::new();

            let scope = dataflow.clone();

            // a source that attempts to pull from `recv` and produce commands for everyone
            source(dataflow, "SequenceInput", move |capability, info| {

                let activator = scope.activator_for(&info.address[..]);

                // so we can drop, if input queue vanishes.
                let mut capability = Some(capability);

                // closure broadcasts any commands it grabs.
                move |output| {

                    if let Some(send_queue) = send_weak.upgrade() {

                        // capability *should* still be non-None.
                        let capability = capability.as_mut().expect("Capability unavailable");

                        // downgrade capability to current time.
                        capability.downgrade(&timer.elapsed());

                        // drain and broadcast `send`.
                        let mut session = output.session(&capability);
                        let mut borrow = send_queue.borrow_mut();
                        for element in borrow.drain(..) {
                            for worker_index in 0 .. peers {
                                session.give((worker_index, element.clone()));
                            }
                        }

                        activator.activate();
                    }
                    else {
                        capability = None;
                    }
                }
            })
            .sink(
                Exchange::new(|x: &(usize, T)| x.0 as u64),
                "SequenceOutput",
                move |input| {

                    // grab each command and queue it up
                    input.for_each(|time, data| {
                        data.swap(&mut vector);
                        for (_worker, element) in vector.drain(..) {
                            recvd.push((time.time().clone(), element));
                        }
                    });

                    recvd.sort();

                    // determine how many (which) elements to read from `recvd`.
                    let count = recvd.iter().filter(|&(ref time, _)| !input.frontier().less_equal(time)).count();
                    let iter = recvd.drain(..count);

                    if let Some(recv_queue) = recv_weak.upgrade() {
                        recv_queue.borrow_mut().extend(iter.map(|(_time,elem)| elem));
                    }
                }
            );
        });

        Sequencer { send, recv, }
    }

    /// Adds an element to the shared log.
    pub fn push(&mut self, element: T) {
        self.send.borrow_mut().push_back(element);
    }

    /// Reads the next element from the shared log.
    pub fn next(&mut self) -> Option<T> {
        self.recv.borrow_mut().pop_front()
    }
}