use progress::frontier::{AntichainRef, MutableAntichain};
use progress::Timestamp;
use dataflow::operators::Capability;
use logging::Logger;

/// Tracks requests for notification and delivers available notifications.
///
/// A `Notificator` represents a dynamic set of notifications and a fixed notification frontier.
/// One can interact with one by requesting notification with `notify_at`, and retrieving notifications
/// with `for_each` and `next`. The next notification to be delivered will be the available notification
/// with the least timestamp, with the implication that the notifications will be non-decreasing as long
/// as you do not request notifications at times prior to those that have already been delivered.
///
/// It is possible to attach data to notifications by using the `_data` variants of the interface.
/// The data is collected into a vector that is returned once a notification is available.
///
/// Notification requests persist across uses of `Notificator`, and it may help to think of `Notificator`
/// as a notification *session*. However, idiomatically it seems you mostly want to restrict your usage
/// to such sessions, which is why this is the main notificator type.
pub struct Notificator<'a, T: Timestamp, D: 'a = ()> {
    frontiers: &'a [&'a MutableAntichain<T>],
    inner: &'a mut FrontierNotificator<T, D>,
    logging: &'a Logger,
}

impl<'a, T: Timestamp, D> Notificator<'a, T, D> {
    /// Allocates a new `Notificator`.
    ///
    /// This is more commonly accomplished using `input.monotonic(frontiers)`.
    pub fn new(
        frontiers: &'a [&'a MutableAntichain<T>],
        inner: &'a mut FrontierNotificator<T, D>,
        logging: &'a Logger) -> Self {
        inner.make_available(frontiers);

        Notificator {
            frontiers,
            inner,
            logging,
        }
    }

    /// Reveals the elements in the frontier of the indicated input.
    pub fn frontier(&self, input: usize) -> AntichainRef<T> {
        self.frontiers[input].frontier()
    }
}

impl<'a, T: Timestamp> Notificator<'a, T, ()> {

    /// Requests a notification at the time associated with capability `cap`.
    ///
    /// In order to request a notification at future timestamp, obtain a capability for the new
    /// timestamp first, as show in the example.
    ///
    /// # Examples
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
        self.inner.notify_at_frontiered(cap, self.frontiers);
    }

    /// Repeatedly calls `logic` until exhaustion of the available notifications.
    ///
    /// `logic` receives a capability for `t`, the timestamp being notified and a `count`
    /// representing how many capabilities were requested for that specific timestamp.
    #[inline]
    pub fn for_each<F: FnMut(Capability<T>, u64, &mut Notificator<T>)>(&mut self, mut logic: F) {
        while let Some((cap, _data)) = self.next() {
            self.logging.when_enabled(|l| l.log(::logging::TimelyEvent::GuardedProgress(
                ::logging::GuardedProgressEvent { is_start: true })));
            logic(cap, 1, self);
            self.logging.when_enabled(|l| l.log(::logging::TimelyEvent::GuardedProgress(
                ::logging::GuardedProgressEvent { is_start: false })));
        }
    }
}

impl<'a, T: Timestamp, D> Notificator<'a, T, D> {

    /// Requests a notification at the time associated with capability `cap`.
    ///
    /// In order to request a notification at future timestamp, obtain a capability for the new
    /// timestamp first, as show in the example.
    ///
    /// # Examples
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
    pub fn notify_at_data<II: IntoIterator<Item=D>>(&mut self, iter: II, cap: Capability<T>) {
        self.inner.notify_at_frontiered_data(cap, iter, self.frontiers);
    }

    /// Repeatedly calls `logic` until exhaustion of the available notifications.
    ///
    /// `logic` receives a capability for `t`, the timestamp being notified and a `count`
    /// representing how many capabilities were requested for that specific timestamp.
    #[inline]
    pub fn for_each_data<F: FnMut(Capability<T>, Vec<D>, &mut Self)>(&mut self, mut logic: F) {
        while let Some((cap, data)) = self.next() {
            self.logging.when_enabled(|l| l.log(::logging::TimelyEvent::GuardedProgress(
                ::logging::GuardedProgressEvent { is_start: true })));
            logic(cap, data, self);
            self.logging.when_enabled(|l| l.log(::logging::TimelyEvent::GuardedProgress(
                ::logging::GuardedProgressEvent { is_start: false })));
        }
    }
}

impl<'a, T: Timestamp, D> Iterator for Notificator<'a, T, D> {
    type Item = (Capability<T>, Vec<D>);

    /// Retrieve the next available notification.
    ///
    /// Returns `None` if no notification is available. Returns `Some(cap, count)` otherwise:
    /// `cap` is a capability for `t`, the timestamp being notified and, `count` represents
    /// how many notifications (out of those requested) are being delivered for that specific
    /// timestamp.
    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next(self.frontiers)
    }
}

#[test]
fn notificator_delivers_notifications_in_topo_order() {
    use std::rc::Rc;
    use std::cell::RefCell;
    use progress::ChangeBatch;
    use progress::frontier::MutableAntichain;
    use progress::nested::product::Product;
    use dataflow::operators::capability::mint as mint_capability;

    let mut frontier = MutableAntichain::new_bottom(Product::new(0, 0));

    let root_capability = mint_capability(Product::new(0,0), Rc::new(RefCell::new(ChangeBatch::new())));

    let logging = ::logging::new_inactive_logger();

    // notificator.update_frontier_from_cm(&mut vec![ChangeBatch::new_from(ts_from_tuple((0, 0)), 1)]);
    let times = vec![
        Product::new(3, 5),
        Product::new(5, 4),
        Product::new(1, 2),
        Product::new(1, 1),
        Product::new(1, 1),
        Product::new(5, 4),
        Product::new(6, 0),
        Product::new(6, 2),
        Product::new(5, 8),
    ];

    // create a raw notificator with pending notifications at the times above.
    let mut frontier_notificator = FrontierNotificator::from(times.iter().map(|t| root_capability.delayed(t)));

    // the frontier is initially (0,0), and so we should deliver no notifications.
    assert!(frontier_notificator.monotonic(&[&frontier], &logging).next().is_none());

    // advance the frontier to [(5,7), (6,0)], opening up some notifications.
    frontier.update_iter(vec![(Product::new(0,0),-1), (Product::new(5,7), 1), (Product::new(6,1), 1)]);

    {
        let frontiers = [&frontier];
        let mut notificator = frontier_notificator.monotonic(&frontiers, &logging);

        // we should deliver the following available notifications, in this order.
        assert_eq!(notificator.next().unwrap().0.time(), &Product::new(1,1));
        assert_eq!(notificator.next().unwrap().0.time(), &Product::new(1,2));
        assert_eq!(notificator.next().unwrap().0.time(), &Product::new(3,5));
        assert_eq!(notificator.next().unwrap().0.time(), &Product::new(5,4));
        assert_eq!(notificator.next().unwrap().0.time(), &Product::new(6,0));
        assert_eq!(notificator.next(), None);
    }

    // advance the frontier to [(6,10)] opening up all remaining notifications.
    frontier.update_iter(vec![(Product::new(5,7), -1), (Product::new(6,1), -1), (Product::new(6,10), 1)]);

    {
        let frontiers = [&frontier];
        let mut notificator = frontier_notificator.monotonic(&frontiers, &logging);

        // the first available notification should be (5,8). Note: before (6,0) in the total order, but not
        // in the partial order. We don't make the promise that we respect the total order.
        assert_eq!(notificator.next().unwrap().0.time(), &Product::new(5, 8));

        // add a new notification, mid notification session.
        notificator.notify_at(root_capability.delayed(&Product::new(5,9)));

        // we expect to see (5,9) before we see (6,2) before we see None.
        assert_eq!(notificator.next().unwrap().0.time(), &Product::new(5,9));
        assert_eq!(notificator.next().unwrap().0.time(), &Product::new(6,2));
        assert_eq!(notificator.next(), None);
    }
}

/// Tracks requests for notification and delivers available notifications.
///
/// `FrontierNotificator` is meant to manage the delivery of requested notifications in the
/// presence of inputs that may have outstanding messages to deliver.
/// The notificator inspects the frontiers, as presented from the outside, for each input.
/// Requested notifications can be served only once there are no frontier elements less-or-equal
/// to them, and there are no other pending notification requests less than them. Each will be
/// less-or-equal to itself, so we want to dodge that corner case.
///
/// # Examples
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
///         in1.binary_frontier(&in2, Pipeline, Pipeline, "example", |mut _default_cap, _info| {
///             let mut notificator = FrontierNotificator::new();
///             move |input1, input2, output| {
///                 while let Some((time, data)) = input1.next() {
///                     notificator.notify_at_data(time.retain(), data.drain(..));
///                 }
///                 while let Some((time, data)) = input2.next() {
///                     notificator.notify_at_data(time.retain(), data.drain(..));
///                 }
///                 notificator.for_each_data(&[input1.frontier(), input2.frontier()], |time, mut data, _| {
///                     output.session(&time).give_iterator(data.drain(..));
///                 });
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
pub struct FrontierNotificator<T: Timestamp, D> {
    pending: Vec<(Capability<T>, Vec<D>)>,
    available: ::std::collections::BinaryHeap<OrderReversed<T, D>>,
}

impl<T: Timestamp, D> FrontierNotificator<T, D> {
    /// Allocates a new `FrontierNotificator`.
    pub fn new() -> Self {
        FrontierNotificator {
            pending: Vec::new(),
            available: ::std::collections::BinaryHeap::new(),
        }
    }

    /// Allocates a new `FrontierNotificator` with initial capabilities.
    pub fn from_data<I: IntoIterator<Item=(Capability<T>, D)>>(iter: I) -> Self {
        FrontierNotificator {
            pending: iter.into_iter().map(|x| (x.0, vec![x.1])).collect(),
            available: ::std::collections::BinaryHeap::new(),
        }
    }

    /// Requests a notification at the time associated with capability `cap` and attached data `data`.
    /// Takes ownership of the capability.
    ///
    /// In order to request a notification at future timestamp, obtain a capability for the new
    /// timestamp first, as shown in the example.
    ///
    /// # Examples
    /// ```
    /// use timely::dataflow::operators::{ToStream, FrontierNotificator};
    /// use timely::dataflow::operators::generic::operator::Operator;
    /// use timely::dataflow::channels::pact::Pipeline;
    ///
    /// timely::example(|scope| {
    ///     (0..10).to_stream(scope)
    ///            .unary_frontier(Pipeline, "example", |_, _| {
    ///                let mut notificator = FrontierNotificator::new();
    ///                move |input, output| {
    ///                    input.for_each(|cap, data| {
    ///                        output.session(&cap).give_content(data);
    ///                        let mut time = cap.time().clone();
    ///                        time.inner += 1;
    ///                        notificator.notify_at_data(cap.delayed(&time), Some(time.inner));
    ///                    });
    ///                    notificator.for_each_data(&[input.frontier()], |cap, data, _| {
    ///                        println!("done with time: {:?} at: {:?}", cap.time(), data);
    ///                    });
    ///                }
    ///            });
    /// });
    /// ```
    #[inline]
    pub fn notify_at_data<'a, II: IntoIterator<Item=D>>(&mut self, cap: Capability<T>, iter: II) {
        self.pending.push((cap, iter.into_iter().collect()));
    }

    /// Requests a notification at the time associated with capability `cap` and data `data`.
    ///
    /// The method takes list of frontiers from which it determines if the capability is immediately available.
    /// When used with the same frontier as `make_available`, this method can ensure that notifications are
    /// non-decreasing. Simply using `notify_at` will only insert new notifications into the list of pending
    /// notifications, which are only re-examine with calls to `make_available`.
    #[inline]
    pub fn notify_at_frontiered_data<'a, II: IntoIterator<Item=D>>(&mut self, cap: Capability<T>, iter: II, frontiers: &'a [&'a MutableAntichain<T>]) {
        if frontiers.iter().all(|f| !f.less_equal(cap.time())) {
            self.available.push(OrderReversed::new(cap, iter.into_iter().collect()));
        }
        else {
            self.pending.push((cap, iter.into_iter().collect()));
        }
    }

    /// Enables pending notifications not in advance of any element of `frontiers`.
    pub fn make_available<'a>(&mut self, frontiers: &'a [&'a MutableAntichain<T>]) {

        // By invariant, nothing in self.available is greater_equal anything in self.pending.
        // It should be safe to append any ordered subset of self.pending to self.available,
        // in that the sequence of capabilities in self.available will remain non-decreasing.

        if !self.pending.is_empty() {

            self.pending.sort_by(|x,y| x.0.time().cmp(y.0.time()));
            for i in 0 .. self.pending.len() - 1 {
                if self.pending[i].0.time() == self.pending[i+1].0.time() {
                    let data = ::std::mem::replace(&mut self.pending[i].1, vec![]);
                    self.pending[i+1].1.extend(data);
                }
            }
            self.pending.retain(|x| !x.1.is_empty());

            for i in 0 .. self.pending.len() {
                if frontiers.iter().all(|f| !f.less_equal(&self.pending[i].0)) {
                    // TODO : This clones a capability, whereas we could move it instead.
                    let data = ::std::mem::replace(&mut self.pending[i].1, vec![]);
                    self.available.push(OrderReversed::new(self.pending[i].0.clone(), data));
                }
            }
            self.pending.retain(|x| !x.1.is_empty());
        }
    }

    /// Returns the next available capability with respect to the supplied frontiers, if one exists,
    /// and the data previously associated with it.
    ///
    /// In the interest of efficiency, this method may yield capabilities in decreasing order, in certain
    /// circumstances. If you want to iterate through capabilities with an in-order guarantee, either (i)
    /// use `for_each`
    #[inline]
    pub fn next<'a>(&mut self, frontiers: &'a [&'a MutableAntichain<T>]) -> Option<(Capability<T>, Vec<D>)> {
        if self.available.is_empty() {
            self.make_available(frontiers);
        }
        self.available.pop().map(|front| {
            while self.available.peek() == Some(&front) { self.available.pop(); }
            (front.element, front.data)
        })
    }

    /// Repeatedly calls `logic` till exhaustion of the notifications made available by inspecting
    /// the frontiers.
    ///
    /// `logic` receives a capability for `t`, the timestamp being notified, and associated data.
    #[inline]
    pub fn for_each_data<'a, F: FnMut(Capability<T>, Vec<D>, &mut Self)>(&mut self, frontiers: &'a [&'a MutableAntichain<T>], mut logic: F) {
        self.make_available(frontiers);
        while let Some((cap, data)) = self.next(frontiers) {
            logic(cap, data, self);
        }
    }

    /// Creates a notificator session in which delivered notification will be non-decreasing.
    ///
    /// This implementation can be emulated with judicious use of `make_available` and `notify_at_frontiered`,
    /// in the event that `Notificator` provides too restrictive an interface.
    #[inline]
    pub fn monotonic<'a>(&'a mut self, frontiers: &'a [&'a MutableAntichain<T>], logging: &'a Logger) -> Notificator<'a, T, D> {
        Notificator::new(frontiers, self, logging)
    }

    /// Iterates over pending capabilities and their count. The count represents how often a
    /// capability has been requested.
    ///
    /// To make sure all pending capabilities are above the frontier, use `for_each` or exhaust
    /// `next` to consume all available capabilities.
    ///
    /// # Examples
    /// ```
    /// use timely::dataflow::operators::{ToStream, FrontierNotificator};
    /// use timely::dataflow::operators::generic::operator::Operator;
    /// use timely::dataflow::channels::pact::Pipeline;
    ///
    /// timely::example(|scope| {
    ///     (0..10).to_stream(scope)
    ///            .unary_frontier(Pipeline, "example", |_, _| {
    ///                let mut notificator = FrontierNotificator::new();
    ///                move |input, output| {
    ///                    input.for_each(|cap, data| {
    ///                        output.session(&cap).give_content(data);
    ///                        let mut time = cap.time().clone();
    ///                        time.inner += 1;
    ///                        notificator.notify_at(cap.delayed(&time));
    ///                        assert_eq!(notificator.pending().filter(|t| t.0.time() == &time).count(), 1);
    ///                    });
    ///                    notificator.for_each(&[input.frontier()], |cap, _| {
    ///                        println!("done with time: {:?}", cap.time());
    ///                    });
    ///                }
    ///            });
    /// });
    /// ```
    pub fn pending<'a>(&'a self) -> ::std::slice::Iter<'a, (Capability<T>, Vec<D>)> {
        self.pending.iter()
    }
}

impl<T: Timestamp> FrontierNotificator<T, ()> {

    /// Allocates a new `FrontierNotificator` with initial capabilities.
    pub fn from<I: IntoIterator<Item=Capability<T>>>(iter: I) -> Self {
        FrontierNotificator {
            pending: iter.into_iter().map(|x| (x, vec![()])).collect(),
            available: ::std::collections::BinaryHeap::new(),
        }
    }

    /// Requests a notification at the time associated with capability `cap`. Takes ownership of
    /// the capability.
    ///
    /// In order to request a notification at future timestamp, obtain a capability for the new
    /// timestamp first, as shown in the example.
    ///
    /// # Examples
    /// ```
    /// use timely::dataflow::operators::{ToStream, FrontierNotificator};
    /// use timely::dataflow::operators::generic::operator::Operator;
    /// use timely::dataflow::channels::pact::Pipeline;
    ///
    /// timely::example(|scope| {
    ///     (0..10).to_stream(scope)
    ///            .unary_frontier(Pipeline, "example", |_, _| {
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
    pub fn notify_at<'a>(&mut self, cap: Capability<T>) {
        self.pending.push((cap, vec![()]));
    }

    /// Requests a notification at the time associated with capability `cap`.
    ///
    /// The method takes list of frontiers from which it determines if the capability is immediately available.
    /// When used with the same frontier as `make_available`, this method can ensure that notifications are
    /// non-decreasing. Simply using `notify_at` will only insert new notifications into the list of pending
    /// notifications, which are only re-examine with calls to `make_available`.
    #[inline]
    pub fn notify_at_frontiered<'a>(&mut self, cap: Capability<T>, frontiers: &'a [&'a MutableAntichain<T>]) {
        if frontiers.iter().all(|f| !f.less_equal(cap.time())) {
            self.available.push(OrderReversed::new(cap, vec![]));
        }
            else {
                self.pending.push((cap, vec![()]));
            }
    }

    /// Repeatedly calls `logic` till exhaustion of the notifications made available by inspecting
    /// the frontiers.
    ///
    /// `logic` receives a capability for `t`, the timestamp being notified.
    #[inline]
    pub fn for_each<'a, F: FnMut(Capability<T>, &mut Self)>(&mut self, frontiers: &'a [&'a MutableAntichain<T>], mut logic: F) {
        self.make_available(frontiers);
        while let Some((cap, _data)) = self.next(frontiers) {
            logic(cap, self);
        }
    }

}
struct OrderReversed<T: Timestamp, D> {
    element: Capability<T>,
    data: Vec<D>,
}

impl<T: Timestamp, D> OrderReversed<T, D> {
    fn new(element: Capability<T>, data: Vec<D>) -> Self { OrderReversed { element, data } }
}

impl<T: Timestamp, D> PartialOrd for OrderReversed<T, D> {
    fn partial_cmp(&self, other: &Self) -> Option<::std::cmp::Ordering> {
        other.element.time().partial_cmp(self.element.time())
    }
}
impl<T: Timestamp, D> Ord for OrderReversed<T, D> {
    fn cmp(&self, other: &Self) -> ::std::cmp::Ordering {
        other.element.time().cmp(self.element.time())
    }
}
impl<T: Timestamp, D> PartialEq for OrderReversed<T, D> {
    fn eq(&self, other: &Self) -> bool {
        other.element.time().eq(self.element.time())
    }
}
impl<T: Timestamp, D> Eq for OrderReversed<T, D> {}
