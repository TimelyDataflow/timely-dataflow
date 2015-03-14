use progress::Timestamp;
use progress::frontier::MutableAntichain;
use progress::count_map::CountMap;
// use std::vec::Drain;

#[derive(Default)]
pub struct Notificator<T: Timestamp> {
    pending:        MutableAntichain<T>,    // requests that have not yet been notified
    frontier:       MutableAntichain<T>,    // outstanding work, preventing notification
    available:      CountMap<T>,
    temp:           CountMap<T>,
    changes:        CountMap<T>,
}

impl<T: Timestamp> Notificator<T> {
    pub fn update_frontier_from_cm(&mut self, count_map: &mut CountMap<T>) {
        // println!("notificator update: {:?}", count_map);
        while let Some((ref time, delta)) = count_map.pop() {
            self.frontier.update(time, delta);
        }
        for pend in self.pending.elements.iter() {
            if !self.frontier.le(pend) {
                if let Some(val) = self.pending.count(pend) {
                    self.temp.update(pend, -val);
                    self.available.update(pend, val);
                }
            }
        }

        while let Some((pend, val)) = self.temp.pop() {
            self.pending.update(&pend, val);
        }
    }

    pub fn notify_at(&mut self, time: &T) {
        self.changes.update(time, 1);
        if self.frontier.le(time) {
            self.pending.update(time, 1);
        }
        else {
            // println!("notificator error? notify_at called with time not le the frontier. {:?}", time);
            println!("notificator error? {:?} vs {:?}", time, self.frontier);
            panic!("");
            // self.available.update(time, 1);
        }
    }

    pub fn pull_progress(&mut self, internal: &mut CountMap<T>) {
        while let Some((time, delta)) = self.changes.pop() {
            internal.update(&time, delta);
        }
    }
}

// TODO : This prevents notify_at mid-iteration
impl<T: Timestamp> Iterator for Notificator<T> {
    type Item = (T, i64);
    fn next(&mut self) -> Option<(T, i64)> {
        if let Some((time, delta)) =  self.available.pop() {
            self.changes.update(&time, -delta);
            Some((time, delta))
        }
        else { None }
    }
}
