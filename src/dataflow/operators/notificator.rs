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
    available: Vec<(Capability<T>, u64)>,
    candidates: Vec<(Capability<T>, u64)>,
}

impl<T: Timestamp> Notificator<T> {
    pub fn new() -> Notificator<T> {
        Notificator {
            pending: Vec::new(),
            frontier: Vec::new(),
            available: Vec::new(),
            candidates: Vec::new(),
        }
    }

    /// Updates the `Notificator`'s frontiers from a `CountMap` per input.
    pub fn update_frontier_from_cm(&mut self, count_map: &mut [CountMap<T>]) {
        while self.frontier.len() < count_map.len() {
            self.frontier.push(MutableAntichain::new());
        }

        for (index, counts) in count_map.iter_mut().enumerate() {
            while let Some((time, delta)) = counts.pop() {
                self.frontier[index].update(&time, delta);
            }
        }
    }

    /// Reveals the elements in the frontier of the indicated input.
    pub fn frontier(&self, input: usize) -> &[T] {
        self.frontier[input].elements()
    }

    /// Requests a notification at the time associated with capability `cap`. Takes ownership of
    /// the capability.
    ///
    /// In order to request a notification at future timestamp, obtain a capability for the new
    /// timestamp first, as show in the example.
    ///
    /// #Examples
    /// ```
    /// use timely::dataflow::operators::{ToStream, Unary};
    /// use timely::dataflow::channels::pact::Pipeline;
    ///
    /// timely::example(|scope| {
    ///     (0..10).to_stream(scope)
    ///            .unary_notify(Pipeline, "example", Vec::new(), |input, output, notificator| {
    ///                while let Some((cap, data)) = input.next() {
    ///                    output.session(&cap).give_content(data);
    ///                    let mut time = cap.time();
    ///                    time.inner += 1;
    ///                    notificator.notify_at(cap.delayed(&time));
    ///                }
    ///                while let Some((cap, count)) = notificator.next() {
    ///                    println!("done with time: {:?}", cap.time());
    ///                }
    ///            });
    /// });
    /// ```
    #[inline]
    pub fn notify_at(&mut self, cap: Capability<T>) {
        let push = if let Some(&mut (_, ref mut count)) =
            self.pending.iter_mut().find(|&&mut (ref c, _)| c.time().eq(&cap.time())) {
                *count += 1;
                None
            } else {
                Some((cap, 1))
            };
        if let Some(p) = push {
            self.pending.push(p);
        }
    }

    /// Repeatedly calls `logic` till exhaustion of the available notifications.
    ///
    /// `logic` receives a capability for `t`, the timestamp being notified and a `count`
    /// representing how many capabilities were requested for that specific timestamp.
    #[inline]
    pub fn for_each<F: FnMut(Capability<T>, u64, &mut Notificator<T>)>(&mut self, mut logic: F) {
        while let Some((cap, count)) = self.next() {
        // for (cap, count) in self {
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
    fn next(&mut self) -> Option<(Capability<T>, u64)> {
        if self.available.is_empty() {
            let mut available = &mut self.available; // available is empty
            let mut pending = &mut self.pending;
            let mut candidates = &mut self.candidates;
            assert!(candidates.len() == 0);
            let frontier = &self.frontier;

            pending.drain_into_if(&mut candidates, |&(ref cap, _)| {
                !frontier.iter().any(|x| x.le(&cap.time()))
            });

            while let Some((cap, count)) = candidates.pop() {
                let mut cap_in_minimal_antichain = available.is_empty();
                available.drain_into_if(&mut pending, |&(ref avail_cap, _)| {
                    let cap_lt_available = cap.time().lt(&avail_cap.time());
                    cap_in_minimal_antichain |= cap_lt_available ||
                        cap.time().partial_cmp(&avail_cap.time()).is_none();
                    cap_lt_available
                });
                if cap_in_minimal_antichain {
                    available.push((cap, count));
                } else {
                    pending.push((cap, count));
                }
            }
        }

        self.available.pop()
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
            assert!(notified.iter().skip(i + 1).all(|&(ref c, _)| !c.time().lt(&cap.time())));
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
