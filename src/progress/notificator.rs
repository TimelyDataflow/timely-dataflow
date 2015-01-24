use progress::Timestamp;
use progress::frontier::MutableAntichain;
use progress::count_map::CountMap;
use std::vec::Drain;

#[derive(Default)]
pub struct Notificator<T: Timestamp> {
    pending:        MutableAntichain<T>,    // requests that have not yet been notified
    frontier:       MutableAntichain<T>,    // outstanding work, preventing notification
    available:      Vec<(T, i64)>,
    temp:           Vec<(T, i64)>,
}

impl<T: Timestamp> Notificator<T> {
    pub fn update_frontier(&mut self, time: &T, delta: i64) {
        self.frontier.update(time, delta);
        for pend in self.pending.elements.iter() {
            if time.le(pend) && !self.frontier.less_than(pend) {
                if let Some(val) = self.pending.count(pend) {
                    self.temp.update(pend, -val);
                    self.available.update(pend, val);
                }
            }
        }
        for (pend, val) in self.temp.drain() {
            self.pending.update(&pend, val);
        }
    }

    pub fn notify_at(&mut self, time: &T) {
        if self.frontier.less_than(time) {
            self.pending.update(time, 1);
        }
        else                             {
            self.available.update(time, 1);
        }
    }
}

// TODO : This prevents notify_at mid-iteration
impl<T: Timestamp> Iterator for Notificator<T> {
    type Item = (T, i64);
    fn next(&mut self) -> Option<(T, i64)> {
        self.available.pop()
    }
}
