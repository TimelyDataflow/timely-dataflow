use std::collections::VecDeque;

use progress::Timestamp;
use progress::frontier::MutableAntichain;
use progress::count_map::CountMap;

/// A Notificator manages outstanding capabilities on several inputs as well as notification
/// requests that may be blocked by them.
///
/// Notificator is meant to manage the delivery of requested notifications in the presence of
/// inputs that may have outstanding messages to deliver. The notificator tracks the frontiers,
/// as presented from the outside, for each input. Requested notifications can be served only
/// once there are no frontier elements less-or-equal to them, and there are no other pending
/// notification requests less than them. Each with be less-or-equal to itself, so we want to
/// dodge that corner case.

#[derive(Default)]
pub struct Notificator<T: Timestamp> {
    pending:        MutableAntichain<T>,        // notification requests not yet been delivered
    frontier:       Vec<MutableAntichain<T>>,   // outstanding input, preventing notification
    available:      VecDeque<T>,                // notifications available for delivery
    changes:        CountMap<T>,                // change to report through pull_progress
}

impl<T: Timestamp> Notificator<T> {
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

    pub fn frontier(&self, input: usize) -> &[T] {
        self.frontier[input].elements()
    }

    pub fn notify_at(&mut self, time: &T) {
        self.changes.update(time, 1);
        self.pending.update(time, 1);
    }

    pub fn pull_progress(&mut self, internal: &mut CountMap<T>) {
        while let Some((time, delta)) = self.changes.pop() {
            internal.update(&time, delta);
        }
    }
}

impl<T: Timestamp> Iterator for Notificator<T> {
    type Item = (T, i64);

    fn next(&mut self) -> Option<(T, i64)> {

        // if nothing obvious available, scan for options
        if self.available.len() == 0 {
            for pend in self.pending.elements().iter() {
                if !self.frontier.iter().any(|x| x.le(pend) ) {
                    self.available.push_back(pend.clone());
                }
            }
        }

        // return an available notification, after cleaning up
        if let Some(time) = self.available.pop_front() {
            if let Some(delta) = self.pending.count(&time) {
                self.changes.update(&time, -delta);
                self.pending.update(&time, -delta);
                Some((time, delta))
            }
            else {
                panic!("failed to find available time in pending");
            }
        }
        else { None }
    }
}
