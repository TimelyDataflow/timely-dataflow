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
    pending: Vec<Capability<T>>,
    frontier: Vec<MutableAntichain<T>>,
    available: VecDeque<(Capability<T>, i64)>,
}

impl<T: Timestamp> Notificator<T> {
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
        self.pending.push(cap);
    }

    /// Repeatedly calls `logic` till exhaustion of the available notifications.
    ///
    /// `logic` receives a capability for `t`, the timestamp being notified and a `count`
    /// representing how many capabilities were requested for that specific timestamp.
    #[inline]
    pub fn for_each<F: FnMut(Capability<T>, i64)>(&mut self, mut logic: F) {
        while let Some((cap, count)) = self.next() {
            ::logging::log(&::logging::GUARDED_PROGRESS, true);
            logic(cap, count);
            ::logging::log(&::logging::GUARDED_PROGRESS, false);
        }
    }

    #[inline]
    fn scan_for_available_notifications(&mut self) {
        let mut last = 0;
        while let Some(position) = (&self.pending[last..]).iter().position(|cap| {
            let time = cap.time();
            !self.frontier.iter().any(|x| x.le(&time))
        }) {
            let newly_available = self.pending.swap_remove(position + last);
            let push_new = {
                let already_available = self.available.iter_mut().find(|&&mut (ref cap, _)| {
                    cap.time() == newly_available.time()
                });
                match already_available {
                    Some(&mut (_, ref mut count)) => {
                        *count += 1;
                        false
                    },
                    None => true,
                }
            };
            if push_new {
                self.available.push_back((newly_available, 1));
            }
            last = position;
        }
    }
}

impl<T: Timestamp> Iterator for Notificator<T> {
    type Item = (Capability<T>, i64);

    /// Retrieve the next available notification.
    ///
    /// Returns `None` if no notification is available. Returns `Some(cap, count)` otherwise:
    /// `cap` is a a capability for `t`, the timestamp being notified and, `count` represents
    /// how many capabilities were requested for that specific timestamp.
    fn next(&mut self) -> Option<(Capability<T>, i64)> {
        if self.available.len() == 0 {
            self.scan_for_available_notifications();
        }

        self.available.pop_front()
    }
}

