use std::collections::VecDeque;

use progress::frontier::MutableAntichain;
use progress::Timestamp;
use progress::count_map::CountMap;
use dataflow::operators::Capability;

/// Tracks requests for notification and delivers available notifications.
///
/// Notificator is meant to manage the delivery of requested notifications in the presence of
/// inputs that may have outstanding messages to deliver. The notificator tracks the frontiers,
/// as presented from the outside, for each input. Requested notifications can be served only
/// once there are no frontier elements less-or-equal to them, and there are no other pending
/// notification requests less than them. Each with be less-or-equal to itself, so we want to
/// dodge that corner case.
pub struct Notificator<T: Timestamp> {
    pending: Vec<(Capability<T>, u64)>,
    frontier: Vec<MutableAntichain<T>>,
    available: VecDeque<(Capability<T>, u64)>,
}

impl<T: Timestamp> Notificator<T> {
    /// Allocates a new `Notificator`.
    pub fn new() -> Notificator<T> {
        Notificator {
            pending: Vec::new(),
            frontier: Vec::new(),
            available: VecDeque::new(),
        }
    }

    /// Updates the `Notificator`'s frontiers from a `CountMap` per input.
    pub fn update_frontier_from_cm(&mut self, count_map: &mut [CountMap<T>]) {
        while self.frontier.len() < count_map.len() {
            self.frontier.push(MutableAntichain::new());
        }

        for (index, counts) in count_map.iter_mut().enumerate() {
            self.frontier[index].update_iter(counts.drain());
        }
    }

    /// Reveals the elements in the frontier of the indicated input.
    pub fn frontier(&self, input: usize) -> &[T] {
        self.frontier[input].frontier()
    }

    /// Requests a notification at the time associated with capability `cap`. Takes ownership of
    /// the capability.
    ///
    /// In order to request a notification at future timestamp, obtain a capability for the new
    /// timestamp first, as show in the example.
    ///
    /// #Examples
    /// ```
    /// use timely::dataflow::operators::ToStream;
    /// use timely::dataflow::operators::generic::unary::Unary;
    /// use timely::dataflow::channels::pact::Pipeline;
    ///
    /// timely::example(|scope| {
    ///     (0..10).to_stream(scope)
    ///            .unary_notify(Pipeline, "example", Vec::new(), |input, output, notificator| {
    ///                input.for_each(|cap, data| {
    ///                    output.session(&cap).give_content(data);
    ///                    let mut time = cap.time().clone();
    ///                    time.inner += 1;
    ///                    notificator.notify_at(cap.delayed(&time));
    ///                });
    ///                notificator.for_each(|cap,_,_| {
    ///                    println!("done with time: {:?}", cap.time());
    ///                });
    ///            });
    /// });
    /// ```
    #[inline]
    pub fn notify_at(&mut self, cap: Capability<T>) {
        self.pending.push((cap, 1));
        // let push = if let Some(&mut (_, ref mut count)) =
        //     self.pending.iter_mut().find(|&&mut (ref c, _)| c.time().eq(&cap.time())) {
        //         *count += 1;
        //         None
        //     } else {
        //         Some((cap, 1))
        //     };
        // if let Some(p) = push {
        //     self.pending.push(p);
        // }
    }

    /// Repeatedly calls `logic` till exhaustion of the available notifications.
    ///
    /// `logic` receives a capability for `t`, the timestamp being notified and a `count`
    /// representing how many capabilities were requested for that specific timestamp.
    #[inline(never)]
    pub fn for_each<F: FnMut(Capability<T>, u64, &mut Notificator<T>)>(&mut self, mut logic: F) {
        while let Some((cap, count)) = self.next() {
            ::logging::log(&::logging::GUARDED_PROGRESS, true);
            logic(cap, count, self);
            ::logging::log(&::logging::GUARDED_PROGRESS, false);
        }
    }
}

impl<T: Timestamp> Iterator for Notificator<T> {
    type Item = (Capability<T>, u64);

    /// Retrieve the next available notification.
    ///
    /// Returns `None` if no notification is available. Returns `Some(cap, count)` otherwise:
    /// `cap` is a a capability for `t`, the timestamp being notified and, `count` represents
    /// how many notifications (out of those requested) are being delivered for that specific
    /// timestamp.
    #[inline(never)]
    fn next(&mut self) -> Option<(Capability<T>, u64)> {
        if self.available.is_empty() {
            self.make_available();
        }
        self.available.pop_front()
    }
}

impl<T: Timestamp> Notificator<T> {

    // appends elements of `self.pending` not `greater_equal` to `self.frontier` into `self.available`.
    fn make_available(&mut self) {

        // By invariant, nothing in self.available is greater_equal anything in self.pending.
        // It should be safe to append any ordered subset of self.pending to self.available,
        // in that the sequence of capabilities in self.available will remain non-decreasing.

        if self.pending.len() > 0 {
            self.pending.sort_by(|x,y| x.0.time().cmp(y.0.time()));
            for i in 0 .. self.pending.len() - 1 {
                if self.pending[i].0.time() == self.pending[i+1].0.time() {
                    self.pending[i+1].1 += self.pending[i].1;
                    self.pending[i].1 = 0;
                }
            }
            self.pending.retain(|x| x.1 > 0);

            for i in 0 .. self.pending.len() {
                if self.frontier.iter().all(|f| !f.less_equal(&self.pending[i].0)) {
                    // TODO : This clones a capability, whereas we could move it instead.
                    self.available.push_back((self.pending[i].0.clone(), self.pending[i].1));
                    self.pending[i].1 = 0;
                }
            }
            self.pending.retain(|x| x.1 > 0);
        }
    }
}

trait DrainIntoIf<T> {
    /// Invokes "P" on each element of "source" (exactly once) and moves the matching values to
    /// "target". Ordering is not preserved.
    fn drain_into_if<P>(&mut self, target: &mut Vec<T>, p: P) -> () where P: FnMut(&T) -> bool;
}

impl<T> DrainIntoIf<T> for Vec<T> {
    fn drain_into_if<P>(&mut self, target: &mut Vec<T>, mut p: P) -> () where P: FnMut(&T) -> bool {
        let mut i = 0;
        while i < self.len() {
            let matches = {
                let v = &mut **self;
                p(&v[i])
            };
            if matches {
                target.push(self.swap_remove(i));
            } else {
                i += 1;
            }
        }
    }
}

#[test]
fn drain_into_if_behaves_correctly() {
    let mut v = vec![3, 10, 4, 5, 13, 7, 2, 1];
    let mut v1 = Vec::new();
    v.drain_into_if(&mut v1, |x| x >= &5);
    v.sort();
    v1.sort();
    assert!(v == vec![1, 2, 3, 4]);
    assert!(v1 == vec![5, 7, 10, 13]);
}

#[test]
fn notificator_delivers_notifications_in_topo_order() {
    use std::rc::Rc;
    use std::cell::RefCell;
    use order::PartialOrder;
    use progress::nested::product::Product;
    use progress::timestamp::RootTimestamp;
    use dataflow::operators::capability::mint as mint_capability;
    fn ts_from_tuple(t: (u64, u64)) -> Product<Product<RootTimestamp, u64>, u64> {
        let (a, b) = t;
        Product::new(RootTimestamp::new(a), b)
    }
    let mut notificator = Notificator::<Product<Product<RootTimestamp, u64>, u64>>::new();
    notificator.update_frontier_from_cm(&mut vec![CountMap::new_from(&ts_from_tuple((0, 0)), 1)]);
    let internal_changes = Rc::new(RefCell::new(CountMap::new()));
    let times = vec![
        (3, 5),
        (5, 4),
        (1, 2),
        (1, 1),
        (1, 1),
        (5, 4),
        (6, 0),
        (5, 8),
    ].into_iter().map(ts_from_tuple).map(|ts| mint_capability(ts, internal_changes.clone()));
    for t in times {
        notificator.notify_at(t);
    }
    notificator.update_frontier_from_cm(&mut {
        let mut cm = CountMap::new();
        cm.update(&ts_from_tuple((0, 0)), -1);
        cm.update(&ts_from_tuple((5, 7)), 1);
        cm.update(&ts_from_tuple((6, 0)), 1);
        vec![cm]
    });
    // Drains all the available notifications and checks they're being delivered in some
    // topological ordering. Also checks that the counts returned by .next() match the expected
    // counts.
    fn check_notifications(
        notificator: &mut Notificator<Product<Product<RootTimestamp, u64>, u64>>,
        expected_counts: Vec<((u64, u64), u64)>) {

        let notified = notificator.by_ref().collect::<Vec<_>>();
        for (i, &(ref cap, _)) in notified.iter().enumerate() {
            assert!(notified.iter().skip(i + 1).all(|&(ref c, _)| !c.time().less_equal(&cap.time())));
        }
        let mut counts = notified.iter().map(|&(ref cap, count)| {
            let ts = cap.time();
            ((ts.outer.inner, ts.inner), count)
        }).collect::<Vec<_>>();
        counts.sort_by(|&(ts1, _), &(ts2, _)| ts1.cmp(&ts2));
        assert!(counts == expected_counts);
    }
    check_notifications(&mut notificator, vec![
        ((1, 1), 2),
        ((1, 2), 1),
        ((3, 5), 1),
        ((5, 4), 2),
    ]);
    notificator.update_frontier_from_cm(&mut {
        let mut cm = CountMap::new();
        cm.update(&ts_from_tuple((5, 7)), -1);
        cm.update(&ts_from_tuple((6, 0)), -1);
        cm.update(&ts_from_tuple((6, 10)), 1);
        vec![cm]
    });
    check_notifications(&mut notificator, vec![
        ((5, 8), 1),
        ((6, 0), 1),
    ]);

}

/// Tracks requests for notification and delivers available notifications.
///
/// FrontierNotificator is meant to manage the delivery of requested notifications in the
/// presence of inputs that may have outstanding messages to deliver.
/// The notificator inspects the frontiers, as presented from the outside, for each input.
/// Requested notifications can be served only once there are no frontier elements less-or-equal
/// to them, and there are no other pending notification requests less than them. Each will be
/// less-or-equal to itself, so we want to dodge that corner case.
///
/// #Examples
/// ```
/// use std::collections::HashMap;
/// use timely::dataflow::operators::{Input, Inspect, FrontierNotificator};
/// use timely::dataflow::operators::generic::operator::Operator;
/// use timely::dataflow::channels::pact::Pipeline;
///
/// timely::execute(timely::Configuration::Thread, |worker| {
///     let (mut in1, mut in2) = worker.dataflow(|scope| {
///         let (in1_handle, in1) = scope.new_input();
///         let (in2_handle, in2) = scope.new_input();
///         in1.binary_frontier(&in2, Pipeline, Pipeline, "example", |mut _default_cap| {
///             let mut notificator = FrontierNotificator::new();
///             let mut stash = HashMap::new();
///             move |input1, input2, output| {
///                 while let Some((time, data)) = input1.next() {
///                     stash.entry(time.time().clone()).or_insert(Vec::new()).extend(data.drain(..));
///                     notificator.notify_at(time);
///                 }
///                 while let Some((time, data)) = input2.next() {
///                     stash.entry(time.time().clone()).or_insert(Vec::new()).extend(data.drain(..));
///                     notificator.notify_at(time);
///                 }
///                 for time in notificator.iter(&[input1.frontier(), input2.frontier()]) {
///                     if let Some(mut vec) = stash.remove(time.time()) {
///                         output.session(&time).give_iterator(vec.drain(..));
///                     }
///                 }
///             }
///         }).inspect_batch(|t, x| println!("{:?} -> {:?}", t, x));
///
///         (in1_handle, in2_handle)
///     });
///
///     for i in 1..10 {
///         in1.send(i - 1);
///         in1.advance_to(i);
///         in2.send(i - 1);
///         in2.advance_to(i);
///     }
///     in1.close();
///     in2.close();
/// }).unwrap();
/// ```
pub struct FrontierNotificator<T: Timestamp> {
    pending: Vec<(Capability<T>, u64)>,
    available: VecDeque<Capability<T>>,
    // candidates: Vec<Capability<T>>,
}

impl<T: Timestamp> FrontierNotificator<T> {
    /// Allocates a new `Notificator`.
    pub fn new() -> FrontierNotificator<T> {
        FrontierNotificator {
            pending: Vec::new(),
            available: VecDeque::new(),
            // candidates: Vec::new(),
        }
    }

    /// Requests a notification at the time associated with capability `cap`. Takes ownership of
    /// the capability.
    ///
    /// In order to request a notification at future timestamp, obtain a capability for the new
    /// timestamp first, as shown in the example.
    ///
    /// #Examples
    /// ```
    /// use timely::dataflow::operators::{ToStream, FrontierNotificator};
    /// use timely::dataflow::operators::generic::operator::Operator;
    /// use timely::dataflow::channels::pact::Pipeline;
    ///
    /// timely::example(|scope| {
    ///     (0..10).to_stream(scope)
    ///            .unary_frontier(Pipeline, "example", |_| {
    ///                let mut notificator = FrontierNotificator::new();
    ///                move |input, output| {
    ///                    input.for_each(|cap, data| {
    ///                        output.session(&cap).give_content(data);
    ///                        let mut time = cap.time().clone();
    ///                        time.inner += 1;
    ///                        notificator.notify_at(cap.delayed(&time));
    ///                    });
    ///                    notificator.for_each(&[input.frontier()], |cap, _| {
    ///                        println!("done with time: {:?}", cap.time());
    ///                    });
    ///                }
    ///            });
    /// });
    /// ```
    #[inline]
    pub fn notify_at(&mut self, cap: Capability<T>) {
        self.pending.push((cap, 1));
        // if !self.pending.iter().find(|&& ref c| c.time().eq(&cap.time())).is_some() {
        //     self.pending.push(cap);
        // }
    }

    /// Iterate over the notifications made available by inspecting the frontiers.
    pub fn iter<'a>(&'a mut self, frontiers: &'a [&'a MutableAntichain<T>]) -> FrontierNotificatorIterator<'a, T> {
        FrontierNotificatorIterator {
            notificator: self,
            frontiers: frontiers,
        }
    }

    // appends elements of `self.pending` not `greater_equal` to `self.frontier` into `self.available`.
    fn make_available(&mut self, frontiers: &[&MutableAntichain<T>]) {

        // By invariant, nothing in self.available is greater_equal anything in self.pending.
        // It should be safe to append any ordered subset of self.pending to self.available,
        // in that the sequence of capabilities in self.available will remain non-decreasing.

        if self.pending.len() > 0 {
            self.pending.sort_by(|x,y| x.0.time().cmp(y.0.time()));
            for i in 0 .. self.pending.len() - 1 {
                if self.pending[i].0.time() == self.pending[i+1].0.time() {
                    self.pending[i+1].1 += self.pending[i].1;
                    self.pending[i].1 = 0;
                }
            }
            self.pending.retain(|x| x.1 > 0);

            for i in 0 .. self.pending.len() {
                if frontiers.iter().all(|f| !f.less_equal(&self.pending[i].0)) {
                    // TODO : This clones a capability, whereas we could move it instead.
                    self.available.push_back(self.pending[i].0.clone());
                    self.pending[i].1 = 0;
                }
            }
            self.pending.retain(|x| x.1 > 0);
        }
    }

    fn next(&mut self, frontiers: &[& MutableAntichain<T>]) -> Option<Capability<T>> {
        if self.available.is_empty() {
            self.make_available(frontiers);
        }
        self.available.pop_front()

        //     let mut available = &mut self.available; // available is empty
        //     let mut pending = &mut self.pending;
        //     let mut candidates = &mut self.candidates;
        //     assert!(candidates.len() == 0);

        //     pending.drain_into_if(&mut candidates, |& ref cap| {
        //         !frontiers.iter().any(|x| x.less_equal(&cap.time()))
        //     });

        //     while let Some(cap) = candidates.pop() {
        //         let mut cap_in_minimal_antichain = available.is_empty();
        //         available.drain_into_if(&mut pending, |& ref avail_cap| {
        //             let cap_lt_available = cap.time().less_than(&avail_cap.time());
        //             cap_in_minimal_antichain |= cap_lt_available ||
        //                 (!cap.time().less_equal(&avail_cap.time()) && !avail_cap.time().less_equal(&cap.time()));
        //                 // cap.time().partial_cmp(&avail_cap.time()).is_none();
        //             cap_lt_available
        //         });
        //         if cap_in_minimal_antichain {
        //             available.push(cap);
        //         } else {
        //             pending.push(cap);
        //         }
        //     }
        // }

        // self.available.pop()
    }

    /// Repeatedly calls `logic` till exhaustion of the notifications made available by inspecting
    /// the frontiers.
    ///
    /// `logic` receives a capability for `t`, the timestamp being notified.
    #[inline]
    pub fn for_each<F: FnMut(Capability<T>, &mut FrontierNotificator<T>)>(&mut self, frontiers: &[&MutableAntichain<T>], mut logic: F) {
        while let Some(cap) = self.next(frontiers) {
            ::logging::log(&::logging::GUARDED_PROGRESS, true);
            logic(cap, self);
            ::logging::log(&::logging::GUARDED_PROGRESS, false);
        }
    }
}

pub struct FrontierNotificatorIterator<'a, T: Timestamp> {
    notificator: &'a mut FrontierNotificator<T>,
    frontiers: &'a [&'a MutableAntichain<T>],
}

impl<'a, T: Timestamp> Iterator for FrontierNotificatorIterator<'a, T> {
    type Item = Capability<T>;

    /// Retrieve the next available notification.
    ///
    /// Returns `None` if no notification is available. Returns `Some(cap, count)` otherwise:
    /// `cap` is a a capability for `t` - the timestamp being notified - and `count` represents
    /// how many notifications (out of those requested) are being delivered for that specific
    /// timestamp.
    fn next(&mut self) -> Option<Capability<T>> {
        self.notificator.next(self.frontiers)
    }
}
